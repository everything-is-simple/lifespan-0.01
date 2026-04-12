"""`structure snapshot` 的列解析与 sidecar 查询辅助。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.structure.structure_shared import (
    _BreakConfirmationRow,
    _StatsSnapshotRow,
    _normalize_date_value,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
)


def _load_break_confirmation_rows(
    *,
    malf_path: Path,
    table_name: str | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> dict[tuple[str, date], _BreakConfirmationRow]:
    if not malf_path.exists() or not instruments or not table_name:
        return {}
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
    except ValueError:
        return {}
    try:
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument", "entity_code", "code"),
            field_name="instrument",
            table_name=table_name,
        )
        trigger_date_column = _resolve_existing_column(
            available_columns,
            ("trigger_bar_dt", "signal_date"),
            field_name="trigger_bar_dt",
            table_name=table_name,
        )
        timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"{instrument_column} IN ({placeholders})"]
        if signal_start_date is not None:
            where_clauses.append(f"{trigger_date_column} >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append(f"{trigger_date_column} <= ?")
            parameters.append(signal_end_date)
        if timeframe_column is not None:
            where_clauses.append(f"{timeframe_column} = ?")
            parameters.append(timeframe)
        rows = connection.execute(
            f"""
            SELECT
                {instrument_column} AS instrument,
                {trigger_date_column} AS trigger_bar_dt,
                COALESCE({_resolve_optional_column(available_columns, ("confirmation_status",)) or "'pending'"}, 'pending') AS confirmation_status,
                COALESCE({_resolve_optional_column(available_columns, ("break_event_nk",)) or "NULL"}, '') AS break_event_nk
            FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY {trigger_date_column}, {instrument_column}
            """,
            parameters,
        ).fetchall()
        return {
            (str(row[0]), _normalize_date_value(row[1], field_name="trigger_bar_dt")): _BreakConfirmationRow(
                instrument=str(row[0]),
                trigger_bar_dt=_normalize_date_value(row[1], field_name="trigger_bar_dt"),
                confirmation_status=_normalize_optional_str(row[2], default="pending"),
                break_event_nk=_normalize_optional_str(row[3]),
            )
            for row in rows
        }
    finally:
        connection.close()


def _load_stats_snapshot_rows(
    *,
    malf_path: Path,
    table_name: str | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> dict[tuple[str, date, date], _StatsSnapshotRow]:
    if not malf_path.exists() or not instruments or not table_name:
        return {}
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
    except ValueError:
        return {}
    try:
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument", "entity_code", "code"),
            field_name="instrument",
            table_name=table_name,
        )
        signal_date_column = _resolve_existing_column(
            available_columns,
            ("signal_date",),
            field_name="signal_date",
            table_name=table_name,
        )
        asof_date_column = _resolve_optional_column(available_columns, ("asof_bar_dt", "asof_date")) or signal_date_column
        timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"{instrument_column} IN ({placeholders})"]
        if signal_start_date is not None:
            where_clauses.append(f"{signal_date_column} >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append(f"{signal_date_column} <= ?")
            parameters.append(signal_end_date)
        if timeframe_column is not None:
            where_clauses.append(f"{timeframe_column} = ?")
            parameters.append(timeframe)
        rows = connection.execute(
            f"""
            SELECT
                {instrument_column} AS instrument,
                {signal_date_column} AS signal_date,
                {asof_date_column} AS asof_bar_dt,
                COALESCE({_resolve_optional_column(available_columns, ("stats_snapshot_nk",)) or "NULL"}, '') AS stats_snapshot_nk,
                {_resolve_optional_column(available_columns, ("exhaustion_risk_bucket",)) or "NULL"} AS exhaustion_risk_bucket,
                {_resolve_optional_column(available_columns, ("reversal_probability_bucket",)) or "NULL"} AS reversal_probability_bucket
            FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY {signal_date_column}, {instrument_column}, {asof_date_column}
            """,
            parameters,
        ).fetchall()
        return {
            (
                str(row[0]),
                _normalize_date_value(row[1], field_name="signal_date"),
                _normalize_date_value(row[2], field_name="asof_bar_dt"),
            ): _StatsSnapshotRow(
                instrument=str(row[0]),
                signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                asof_bar_dt=_normalize_date_value(row[2], field_name="asof_bar_dt"),
                stats_snapshot_nk=_normalize_optional_str(row[3]),
                exhaustion_risk_bucket=_normalize_optional_nullable_str(row[4]),
                reversal_probability_bucket=_normalize_optional_nullable_str(row[5]),
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
