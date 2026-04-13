"""`alpha family ledger` runner 的 source 读取 helper。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.alpha.family_shared import (
    _FamilyContextRow,
    _MalfStateRow,
    _TriggerRow,
    _normalize_date_value,
    _normalize_optional_int,
    _normalize_optional_nullable_str,
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
            daily_source_context_nk,
            weekly_major_state,
            weekly_trend_direction,
            weekly_reversal_stage,
            weekly_source_context_nk,
            monthly_major_state,
            monthly_trend_direction,
            monthly_reversal_stage,
            monthly_source_context_nk,
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
            daily_source_context_nk=_normalize_optional_nullable_str(row[9]),
            weekly_major_state=_normalize_optional_nullable_str(row[10]),
            weekly_trend_direction=_normalize_optional_nullable_str(row[11]),
            weekly_reversal_stage=_normalize_optional_nullable_str(row[12]),
            weekly_source_context_nk=_normalize_optional_nullable_str(row[13]),
            monthly_major_state=_normalize_optional_nullable_str(row[14]),
            monthly_trend_direction=_normalize_optional_nullable_str(row[15]),
            monthly_reversal_stage=_normalize_optional_nullable_str(row[16]),
            monthly_source_context_nk=_normalize_optional_nullable_str(row[17]),
            upstream_context_fingerprint=_normalize_optional_str(row[18], default="{}"),
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


def _load_official_context_rows(
    *,
    structure_path: Path,
    malf_path: Path,
    structure_table_name: str,
    malf_table_name: str,
    trigger_rows: list[_TriggerRow],
) -> dict[str, _FamilyContextRow]:
    if not trigger_rows:
        return {}
    structure_map = _load_structure_rows(
        structure_path=structure_path,
        table_name=structure_table_name,
        structure_snapshot_nks=tuple(
            sorted({row.source_structure_snapshot_nk for row in trigger_rows if row.source_structure_snapshot_nk})
        ),
    )
    malf_snapshot_nks: set[str] = set()
    for trigger_row in trigger_rows:
        structure_row = structure_map.get(trigger_row.source_structure_snapshot_nk)
        for snapshot_nk in (
            None if structure_row is None else structure_row["source_context_nk"],
            None if structure_row is None else structure_row["weekly_source_context_nk"],
            None if structure_row is None else structure_row["monthly_source_context_nk"],
            trigger_row.daily_source_context_nk,
            trigger_row.weekly_source_context_nk,
            trigger_row.monthly_source_context_nk,
        ):
            if snapshot_nk:
                malf_snapshot_nks.add(str(snapshot_nk))
    malf_map = _load_malf_state_rows(
        malf_path=malf_path,
        table_name=malf_table_name,
        snapshot_nks=tuple(sorted(malf_snapshot_nks)),
    )

    context_map: dict[str, _FamilyContextRow] = {}
    for trigger_row in trigger_rows:
        structure_row = structure_map.get(trigger_row.source_structure_snapshot_nk)
        daily_context_nk = (
            _normalize_optional_nullable_str(None if structure_row is None else structure_row["source_context_nk"])
            or trigger_row.daily_source_context_nk
        )
        weekly_context_nk = (
            _normalize_optional_nullable_str(
                None if structure_row is None else structure_row["weekly_source_context_nk"]
            )
            or trigger_row.weekly_source_context_nk
        )
        monthly_context_nk = (
            _normalize_optional_nullable_str(
                None if structure_row is None else structure_row["monthly_source_context_nk"]
            )
            or trigger_row.monthly_source_context_nk
        )
        daily_malf_state = None if daily_context_nk is None else malf_map.get(daily_context_nk)
        weekly_malf_state = None if weekly_context_nk is None else malf_map.get(weekly_context_nk)
        monthly_malf_state = None if monthly_context_nk is None else malf_map.get(monthly_context_nk)
        context_map[trigger_row.trigger_event_nk] = _FamilyContextRow(
            structure_snapshot_nk=trigger_row.source_structure_snapshot_nk,
            instrument=trigger_row.instrument,
            signal_date=trigger_row.signal_date,
            asof_date=trigger_row.asof_date,
            major_state=_normalize_optional_str(
                None if structure_row is None else structure_row["major_state"],
                default=(
                    "unknown"
                    if daily_malf_state is None
                    else daily_malf_state.major_state
                ),
            ),
            trend_direction=_normalize_optional_str(
                None if structure_row is None else structure_row["trend_direction"],
                default=(
                    "unknown"
                    if daily_malf_state is None
                    else daily_malf_state.trend_direction
                ),
            ).lower(),
            reversal_stage=_normalize_optional_str(
                None if structure_row is None else structure_row["reversal_stage"],
                default=(
                    "unknown"
                    if daily_malf_state is None
                    else daily_malf_state.reversal_stage
                ),
            ).lower(),
            current_hh_count=_normalize_optional_int(
                None if structure_row is None else structure_row["current_hh_count"]
            ),
            current_ll_count=_normalize_optional_int(
                None if structure_row is None else structure_row["current_ll_count"]
            ),
            structure_progress_state=_normalize_optional_str(
                None if structure_row is None else structure_row["structure_progress_state"],
                default="unknown",
            ).lower(),
            break_confirmation_status=_normalize_optional_nullable_str(
                None if structure_row is None else structure_row["break_confirmation_status"]
            ),
            break_confirmation_ref=_normalize_optional_nullable_str(
                None if structure_row is None else structure_row["break_confirmation_ref"]
            ),
            exhaustion_risk_bucket=_normalize_optional_nullable_str(
                None if structure_row is None else structure_row["exhaustion_risk_bucket"]
            ),
            reversal_probability_bucket=_normalize_optional_nullable_str(
                None if structure_row is None else structure_row["reversal_probability_bucket"]
            ),
            source_context_nk=daily_context_nk,
            weekly_source_context_nk=weekly_context_nk,
            monthly_source_context_nk=monthly_context_nk,
            daily_malf_state=daily_malf_state,
            weekly_malf_state=weekly_malf_state,
            monthly_malf_state=monthly_malf_state,
        )
    return context_map


def _load_structure_rows(
    *,
    structure_path: Path,
    table_name: str,
    structure_snapshot_nks: tuple[str, ...],
) -> dict[str, dict[str, object]]:
    if not structure_snapshot_nks:
        return {}
    connection = duckdb.connect(str(structure_path), read_only=True)
    try:
        placeholders = ", ".join("?" for _ in structure_snapshot_nks)
        rows = connection.execute(
            f"""
            SELECT
                structure_snapshot_nk,
                instrument,
                signal_date,
                asof_date,
                major_state,
                trend_direction,
                reversal_stage,
                current_hh_count,
                current_ll_count,
                structure_progress_state,
                break_confirmation_status,
                break_confirmation_ref,
                exhaustion_risk_bucket,
                reversal_probability_bucket,
                source_context_nk,
                weekly_source_context_nk,
                monthly_source_context_nk
            FROM {table_name}
            WHERE structure_snapshot_nk IN ({placeholders})
            """,
            [*structure_snapshot_nks],
        ).fetchall()
        column_names = [str(item[0]) for item in connection.description]
        return {
            str(row[0]): {
                column_name: row[index]
                for index, column_name in enumerate(column_names)
            }
            for row in rows
        }
    finally:
        connection.close()


def _load_malf_state_rows(
    *,
    malf_path: Path,
    table_name: str,
    snapshot_nks: tuple[str, ...],
) -> dict[str, _MalfStateRow]:
    if not snapshot_nks:
        return {}
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        placeholders = ", ".join("?" for _ in snapshot_nks)
        rows = connection.execute(
            f"""
            SELECT
                snapshot_nk,
                timeframe,
                major_state,
                trend_direction,
                reversal_stage,
                current_hh_count,
                current_ll_count
            FROM {table_name}
            WHERE snapshot_nk IN ({placeholders})
            """,
            [*snapshot_nks],
        ).fetchall()
        return {
            str(row[0]): _MalfStateRow(
                snapshot_nk=str(row[0]),
                timeframe=_normalize_optional_str(row[1], default="D"),
                major_state=_normalize_optional_str(row[2], default="unknown"),
                trend_direction=_normalize_optional_str(row[3], default="unknown").lower(),
                reversal_stage=_normalize_optional_str(row[4], default="unknown").lower(),
                current_hh_count=_normalize_optional_int(row[5]),
                current_ll_count=_normalize_optional_int(row[6]),
            )
            for row in rows
        }
    finally:
        connection.close()


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
