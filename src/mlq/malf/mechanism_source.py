"""承接 `malf mechanism` runner 的桥接输入读取与 checkpoint 过滤。"""

from __future__ import annotations

from datetime import date

import duckdb

from mlq.malf.bootstrap import MALF_MECHANISM_CHECKPOINT_TABLE
from mlq.malf.mechanism_shared import (
    _MechanismInputRow,
    _build_source_candidate_nk,
    _build_source_context_nk,
    _coerce_date,
    _normalize_date_value,
    _normalize_optional_float,
    _normalize_optional_int,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
)


def _normalize_instruments(instruments: list[str] | tuple[str, ...] | None) -> set[str]:
    normalized: set[str] = set()
    for instrument in instruments or ():
        candidate = str(instrument).strip().upper()
        if not candidate:
            continue
        normalized.add(candidate)
        if "." in candidate:
            normalized.add(candidate.split(".", 1)[0])
    return normalized


def _load_mechanism_input_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_context_table: str,
    source_structure_input_table: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    batch_size: int,
    checkpoint_map: dict[tuple[str, str], tuple[date | None, date | None]],
    timeframe: str,
) -> list[_MechanismInputRow]:
    available_context_columns = _load_table_columns(connection, source_context_table)
    available_structure_columns = _load_table_columns(connection, source_structure_input_table)
    instrument_column = _resolve_existing_column(
        available_structure_columns,
        ("instrument", "entity_code", "code"),
        field_name="instrument",
        table_name=source_structure_input_table,
    )
    signal_date_column = _resolve_existing_column(
        available_structure_columns,
        ("signal_date",),
        field_name="signal_date",
        table_name=source_structure_input_table,
    )
    asof_date_column = _resolve_optional_column(available_structure_columns, ("asof_date",)) or signal_date_column
    structure_parameters: list[object] = []
    structure_where: list[str] = []
    if signal_start_date is not None:
        structure_where.append(f"s.{signal_date_column} >= ?")
        structure_parameters.append(signal_start_date)
    if signal_end_date is not None:
        structure_where.append(f"s.{signal_date_column} <= ?")
        structure_parameters.append(signal_end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        structure_where.append(f"s.{instrument_column} IN ({placeholders})")
        structure_parameters.extend(instruments)
    where_sql = f"WHERE {' AND '.join(structure_where)}" if structure_where else ""
    limit_sql = max(int(limit), 1) * max(int(batch_size), 1)
    rows = connection.execute(
        f"""
        WITH ranked_context AS (
            SELECT
                {_resolve_existing_column(available_context_columns, ("entity_code", "instrument", "code"), field_name="instrument", table_name=source_context_table)} AS instrument,
                {_resolve_existing_column(available_context_columns, ("signal_date",), field_name="signal_date", table_name=source_context_table)} AS signal_date,
                {(_resolve_optional_column(available_context_columns, ("asof_date", "calc_date")) or _resolve_existing_column(available_context_columns, ("signal_date",), field_name="signal_date", table_name=source_context_table))} AS asof_date,
                COALESCE({_resolve_optional_column(available_context_columns, ("source_context_nk",)) or "NULL"}, '') AS source_context_nk,
                COALESCE({_resolve_optional_column(available_context_columns, ("malf_context_4",)) or "'UNKNOWN'"}, 'UNKNOWN') AS malf_context_4,
                ROW_NUMBER() OVER (
                    PARTITION BY {_resolve_existing_column(available_context_columns, ("entity_code", "instrument", "code"), field_name="instrument", table_name=source_context_table)},
                                 {_resolve_existing_column(available_context_columns, ("signal_date",), field_name="signal_date", table_name=source_context_table)},
                                 {(_resolve_optional_column(available_context_columns, ("asof_date", "calc_date")) or _resolve_existing_column(available_context_columns, ("signal_date",), field_name="signal_date", table_name=source_context_table))}
                    ORDER BY COALESCE({_resolve_optional_column(available_context_columns, ("updated_at", "created_at")) or "CURRENT_TIMESTAMP"}, CURRENT_TIMESTAMP) DESC
                ) AS row_rank
            FROM {source_context_table}
        )
        SELECT
            s.{instrument_column} AS instrument,
            s.{signal_date_column} AS signal_date,
            s.{asof_date_column} AS asof_date,
            COALESCE(c.source_context_nk, '') AS source_context_nk,
            COALESCE({_resolve_optional_column(available_structure_columns, ("candidate_nk",)) or "NULL"}, '') AS source_candidate_nk,
            COALESCE(c.malf_context_4, 'UNKNOWN') AS malf_context_4,
            COALESCE({_resolve_optional_column(available_structure_columns, ("new_high_count",)) or "0"}, 0) AS new_high_count,
            COALESCE({_resolve_optional_column(available_structure_columns, ("new_low_count",)) or "0"}, 0) AS new_low_count,
            COALESCE({_resolve_optional_column(available_structure_columns, ("refresh_density",)) or "0.0"}, 0.0) AS refresh_density,
            COALESCE({_resolve_optional_column(available_structure_columns, ("advancement_density",)) or "0.0"}, 0.0) AS advancement_density,
            COALESCE({_resolve_optional_column(available_structure_columns, ("is_failed_extreme",)) or "FALSE"}, FALSE) AS is_failed_extreme,
            {_resolve_optional_column(available_structure_columns, ("failure_type",)) or "NULL"} AS failure_type
        FROM {source_structure_input_table} AS s
        LEFT JOIN ranked_context AS c
          ON c.instrument = s.{instrument_column}
         AND c.signal_date = s.{signal_date_column}
         AND c.asof_date = s.{asof_date_column}
         AND c.row_rank = 1
        {where_sql}
        ORDER BY s.{signal_date_column}, s.{instrument_column}, s.{asof_date_column}
        LIMIT ?
        """,
        [*structure_parameters, limit_sql],
    ).fetchall()
    input_rows = [
        _MechanismInputRow(
            instrument=str(row[0]),
            signal_date=_normalize_date_value(row[1], field_name="signal_date"),
            asof_date=_normalize_date_value(row[2], field_name="asof_date"),
            source_context_nk=_normalize_optional_str(
                row[3],
                default=_build_source_context_nk(
                    instrument=str(row[0]),
                    signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                    asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                    malf_context_4=_normalize_optional_str(row[5], default="UNKNOWN"),
                ),
            ),
            source_candidate_nk=_normalize_optional_str(
                row[4],
                default=_build_source_candidate_nk(
                    instrument=str(row[0]),
                    signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                    asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                ),
            ),
            malf_context_4=_normalize_optional_str(row[5], default="UNKNOWN"),
            new_high_count=_normalize_optional_int(row[6]),
            new_low_count=_normalize_optional_int(row[7]),
            refresh_density=_normalize_optional_float(row[8]),
            advancement_density=_normalize_optional_float(row[9]),
            is_failed_extreme=bool(row[10]),
            failure_type=_normalize_optional_nullable_str(row[11]),
        )
        for row in rows
    ]
    if signal_start_date is not None or signal_end_date is not None:
        return input_rows
    return [
        row
        for row in input_rows
        if _should_process_after_checkpoint(row, timeframe=timeframe, checkpoint_map=checkpoint_map)
    ]


def _should_process_after_checkpoint(
    row: _MechanismInputRow,
    *,
    timeframe: str,
    checkpoint_map: dict[tuple[str, str], tuple[date | None, date | None]],
) -> bool:
    last_signal_date, last_asof_date = checkpoint_map.get((row.instrument, timeframe), (None, None))
    if last_signal_date is None and last_asof_date is None:
        return True
    if last_signal_date is not None and row.signal_date > last_signal_date:
        return True
    if last_signal_date is not None and row.signal_date < last_signal_date:
        return False
    if last_asof_date is not None and row.asof_date <= last_asof_date:
        return False
    return True


def _load_checkpoint_map(
    connection: duckdb.DuckDBPyConnection,
    *,
    timeframe: str,
) -> dict[tuple[str, str], tuple[date | None, date | None]]:
    rows = connection.execute(
        f"""
        SELECT instrument, timeframe, last_signal_date, last_asof_date
        FROM {MALF_MECHANISM_CHECKPOINT_TABLE}
        WHERE timeframe = ?
        """,
        [timeframe],
    ).fetchall()
    return {(str(row[0]), str(row[1])): (_coerce_date(row[2]), _coerce_date(row[3])) for row in rows}


def _load_table_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchall()
    if not rows:
        raise ValueError(f"Missing table: {table_name}")
    return {str(row[0]) for row in rows}


def _resolve_existing_column(
    available_columns: set[str],
    candidates: tuple[str, ...],
    *,
    field_name: str,
    table_name: str,
) -> str:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    raise ValueError(f"Missing required column `{field_name}` in table `{table_name}`.")


def _resolve_optional_column(available_columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    return None
