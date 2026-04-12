"""`alpha family ledger` runner 的 source 读取 helper。"""

from __future__ import annotations

from datetime import date

import duckdb

from mlq.alpha.family_shared import (
    _TriggerRow,
    _normalize_date_value,
    _normalize_optional_str,
)


def _load_trigger_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    family_scope: tuple[str, ...],
    instruments: tuple[str, ...],
    limit: int,
) -> list[_TriggerRow]:
    parameters: list[object] = []
    where_clauses: list[str] = []
    if signal_start_date is not None:
        where_clauses.append("signal_date >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append("signal_date <= ?")
        parameters.append(signal_end_date)
    if family_scope:
        placeholders = ", ".join("?" for _ in family_scope)
        where_clauses.append(f"LOWER(trigger_type) IN ({placeholders})")
        parameters.extend(family_scope)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"instrument IN ({placeholders})")
        parameters.extend(instruments)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = connection.execute(
        f"""
        SELECT
            trigger_event_nk,
            instrument,
            signal_date,
            asof_date,
            trigger_family,
            trigger_type,
            pattern_code,
            source_filter_snapshot_nk,
            source_structure_snapshot_nk,
            upstream_context_fingerprint
        FROM {table_name}
        {where_sql}
        ORDER BY signal_date, instrument, trigger_type, pattern_code
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        _TriggerRow(
            trigger_event_nk=str(row[0]),
            instrument=str(row[1]),
            signal_date=_normalize_date_value(row[2], field_name="signal_date"),
            asof_date=_normalize_date_value(row[3], field_name="asof_date"),
            trigger_family=_normalize_optional_str(row[4], default="PAS"),
            trigger_type=_normalize_optional_str(row[5]).lower(),
            pattern_code=_normalize_optional_str(row[6]),
            source_filter_snapshot_nk=_normalize_optional_str(row[7]),
            source_structure_snapshot_nk=_normalize_optional_str(row[8]),
            upstream_context_fingerprint=_normalize_optional_str(row[9], default="{}"),
        )
        for row in rows
    ]


def _load_candidate_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    family_scope: tuple[str, ...],
    instruments: tuple[str, ...],
) -> dict[tuple[str, date, date, str, str], dict[str, object]]:
    available_columns = _load_table_columns(connection, table_name)
    instrument_column = _resolve_existing_column(
        available_columns,
        ("instrument", "code"),
        field_name="instrument",
        table_name=table_name,
    )
    signal_date_column = _resolve_existing_column(
        available_columns,
        ("signal_date",),
        field_name="signal_date",
        table_name=table_name,
    )
    asof_date_column = _resolve_optional_column(available_columns, ("asof_date",)) or signal_date_column
    trigger_family_column = _resolve_optional_column(available_columns, ("trigger_family",))
    trigger_type_column = _resolve_existing_column(
        available_columns,
        ("trigger_type",),
        field_name="trigger_type",
        table_name=table_name,
    )
    pattern_code_column = _resolve_existing_column(
        available_columns,
        ("pattern_code", "pattern", "trigger_type"),
        field_name="pattern_code",
        table_name=table_name,
    )
    optional_payload_columns = sorted(
        column_name
        for column_name in available_columns
        if column_name
        not in {
            instrument_column,
            signal_date_column,
            asof_date_column,
            trigger_family_column,
            trigger_type_column,
            pattern_code_column,
        }
    )
    parameters: list[object] = []
    where_clauses: list[str] = []
    if signal_start_date is not None:
        where_clauses.append(f"{signal_date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{signal_date_column} <= ?")
        parameters.append(signal_end_date)
    if family_scope:
        placeholders = ", ".join("?" for _ in family_scope)
        where_clauses.append(f"LOWER({trigger_type_column}) IN ({placeholders})")
        parameters.extend(family_scope)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{instrument_column} IN ({placeholders})")
        parameters.extend(instruments)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    select_columns = [
        f"{instrument_column} AS instrument",
        f"{signal_date_column} AS signal_date",
        f"{asof_date_column} AS asof_date",
        (
            f"{trigger_family_column} AS trigger_family"
            if trigger_family_column is not None
            else "'PAS' AS trigger_family"
        ),
        f"{trigger_type_column} AS trigger_type",
        f"{pattern_code_column} AS pattern_code",
        *(f"{column_name} AS {column_name}" for column_name in optional_payload_columns),
    ]
    rows = connection.execute(
        f"""
        SELECT
            {", ".join(select_columns)}
        FROM {table_name}
        {where_sql}
        """,
        parameters,
    ).fetchall()
    column_names = [str(item[0]) for item in connection.description]
    candidate_map: dict[tuple[str, date, date, str, str], dict[str, object]] = {}
    for row in rows:
        row_dict = {
            column_name: row[index]
            for index, column_name in enumerate(column_names)
        }
        candidate_map[
            (
                _normalize_optional_str(row_dict["instrument"]),
                _normalize_date_value(row_dict["signal_date"], field_name="signal_date"),
                _normalize_date_value(row_dict["asof_date"], field_name="asof_date"),
                _normalize_optional_str(row_dict["trigger_type"]).lower(),
                _normalize_optional_str(row_dict["pattern_code"]),
            )
        ] = row_dict
    return candidate_map


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
