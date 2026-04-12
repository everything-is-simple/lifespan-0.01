"""封装 `alpha trigger` runner 的上游读取逻辑。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from mlq.alpha.trigger_shared import (
    _OfficialContextRow,
    _TriggerInputRow,
    _normalize_date_value,
    _normalize_optional_int,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
)


def _load_trigger_input_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_TriggerInputRow]:
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
    trigger_type_column = _resolve_existing_column(
        available_columns,
        ("trigger_type",),
        field_name="trigger_type",
        table_name=table_name,
    )
    trigger_family_column = _resolve_optional_column(available_columns, ("trigger_family",))
    pattern_code_column = _resolve_existing_column(
        available_columns,
        ("pattern_code", "pattern", "trigger_type"),
        field_name="pattern_code",
        table_name=table_name,
    )

    parameters: list[object] = []
    where_clauses: list[str] = []
    if signal_start_date is not None:
        where_clauses.append(f"{signal_date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{signal_date_column} <= ?")
        parameters.append(signal_end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{instrument_column} IN ({placeholders})")
        parameters.extend(instruments)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = connection.execute(
        f"""
        SELECT
            {instrument_column} AS instrument,
            {signal_date_column} AS signal_date,
            {asof_date_column} AS asof_date,
            {("'PAS'" if trigger_family_column is None else trigger_family_column)} AS trigger_family,
            {trigger_type_column} AS trigger_type,
            {pattern_code_column} AS pattern_code
        FROM {table_name}
        {where_sql}
        ORDER BY signal_date, instrument, trigger_type, pattern_code
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        _TriggerInputRow(
            instrument=str(row[0]),
            signal_date=_normalize_date_value(row[1], field_name="signal_date"),
            asof_date=_normalize_date_value(row[2], field_name="asof_date"),
            trigger_family=_normalize_optional_str(row[3], default="PAS"),
            trigger_type=_normalize_optional_str(row[4]),
            pattern_code=_normalize_optional_str(row[5]),
        )
        for row in rows
    ]


def _load_official_context_rows(
    *,
    filter_path: Path,
    structure_path: Path,
    filter_table_name: str,
    structure_table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> list[_OfficialContextRow]:
    if not instruments:
        return []
    connection = duckdb.connect(str(filter_path), read_only=True)
    try:
        structure_path_sql = str(structure_path).replace("\\", "/").replace("'", "''")
        connection.execute(f"ATTACH '{structure_path_sql}' AS structure_db")
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"f.instrument IN ({placeholders})"]
        if signal_start_date is not None:
            where_clauses.append("f.signal_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("f.signal_date <= ?")
            parameters.append(signal_end_date)
        rows = connection.execute(
            f"""
            WITH ranked_filter AS (
                SELECT
                    f.filter_snapshot_nk,
                    f.structure_snapshot_nk,
                    f.instrument,
                    f.signal_date,
                    f.asof_date,
                    f.trigger_admissible,
                    f.primary_blocking_condition,
                    f.blocking_conditions_json,
                    f.admission_notes,
                    f.daily_source_context_nk,
                    f.weekly_major_state,
                    f.weekly_trend_direction,
                    f.weekly_reversal_stage,
                    f.weekly_source_context_nk,
                    f.monthly_major_state,
                    f.monthly_trend_direction,
                    f.monthly_reversal_stage,
                    f.monthly_source_context_nk,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.instrument, f.signal_date, f.asof_date
                        ORDER BY f.updated_at DESC, f.last_materialized_run_id DESC
                    ) AS row_rank
                FROM {filter_table_name} AS f
                WHERE {' AND '.join(where_clauses)}
            )
            SELECT
                rf.instrument,
                rf.signal_date,
                rf.asof_date,
                rf.filter_snapshot_nk,
                rf.structure_snapshot_nk,
                rf.trigger_admissible,
                rf.primary_blocking_condition,
                rf.blocking_conditions_json,
                rf.admission_notes,
                rf.daily_source_context_nk,
                rf.weekly_major_state,
                rf.weekly_trend_direction,
                rf.weekly_reversal_stage,
                rf.weekly_source_context_nk,
                rf.monthly_major_state,
                rf.monthly_trend_direction,
                rf.monthly_reversal_stage,
                rf.monthly_source_context_nk,
                s.structure_progress_state,
                s.major_state,
                s.trend_direction,
                s.reversal_stage,
                s.wave_id,
                s.current_hh_count,
                s.current_ll_count
            FROM ranked_filter AS rf
            INNER JOIN structure_db.main.{structure_table_name} AS s
                ON s.structure_snapshot_nk = rf.structure_snapshot_nk
            WHERE rf.row_rank = 1
            """,
            parameters,
        ).fetchall()
        context_rows: list[_OfficialContextRow] = []
        for row in rows:
            fingerprint = json.dumps(
                {
                    "filter_snapshot_nk": str(row[3]),
                    "structure_snapshot_nk": str(row[4]),
                    "trigger_admissible": bool(row[5]),
                    "primary_blocking_condition": _normalize_optional_nullable_str(row[6]),
                    "blocking_conditions_json": _normalize_optional_str(row[7], default="[]"),
                    "admission_notes": _normalize_optional_nullable_str(row[8]),
                    "daily_source_context_nk": _normalize_optional_nullable_str(row[9]),
                    "weekly_major_state": _normalize_optional_nullable_str(row[10]),
                    "weekly_trend_direction": _normalize_optional_nullable_str(row[11]),
                    "weekly_reversal_stage": _normalize_optional_nullable_str(row[12]),
                    "weekly_source_context_nk": _normalize_optional_nullable_str(row[13]),
                    "monthly_major_state": _normalize_optional_nullable_str(row[14]),
                    "monthly_trend_direction": _normalize_optional_nullable_str(row[15]),
                    "monthly_reversal_stage": _normalize_optional_nullable_str(row[16]),
                    "monthly_source_context_nk": _normalize_optional_nullable_str(row[17]),
                    "structure_progress_state": _normalize_optional_str(row[18], default="unknown"),
                    "major_state": _normalize_optional_str(row[19], default="牛逆"),
                    "trend_direction": _normalize_optional_str(row[20], default="down"),
                    "reversal_stage": _normalize_optional_str(row[21], default="none"),
                    "wave_id": _normalize_optional_int(row[22]),
                    "current_hh_count": _normalize_optional_int(row[23]),
                    "current_ll_count": _normalize_optional_int(row[24]),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            context_rows.append(
                _OfficialContextRow(
                    instrument=str(row[0]),
                    signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                    asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                    filter_snapshot_nk=str(row[3]),
                    structure_snapshot_nk=str(row[4]),
                    daily_source_context_nk=_normalize_optional_nullable_str(row[9]),
                    weekly_major_state=_normalize_optional_nullable_str(row[10]),
                    weekly_trend_direction=_normalize_optional_nullable_str(row[11]),
                    weekly_reversal_stage=_normalize_optional_nullable_str(row[12]),
                    weekly_source_context_nk=_normalize_optional_nullable_str(row[13]),
                    monthly_major_state=_normalize_optional_nullable_str(row[14]),
                    monthly_trend_direction=_normalize_optional_nullable_str(row[15]),
                    monthly_reversal_stage=_normalize_optional_nullable_str(row[16]),
                    monthly_source_context_nk=_normalize_optional_nullable_str(row[17]),
                    upstream_context_fingerprint=fingerprint,
                )
            )
        return context_rows
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
