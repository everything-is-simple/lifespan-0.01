"""承接 `filter snapshot` runner 的上游读取与表结构兼容。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.filter.filter_shared import (
    _ObjectiveStatusInputRow,
    _StructureSnapshotInputRow,
    _normalize_date_value,
    _normalize_optional_int,
    _normalize_optional_nullable_int,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
    _normalize_progress_state,
)


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


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


def _load_structure_snapshot_rows(
    *,
    structure_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_StructureSnapshotInputRow]:
    connection = duckdb.connect(str(structure_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument",),
            field_name="instrument",
            table_name=table_name,
        )
        signal_date_column = _resolve_existing_column(
            available_columns,
            ("signal_date",),
            field_name="signal_date",
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
                {_resolve_existing_column(available_columns, ("structure_snapshot_nk",), field_name="structure_snapshot_nk", table_name=table_name)} AS structure_snapshot_nk,
                {instrument_column} AS instrument,
                {signal_date_column} AS signal_date,
                {_resolve_optional_column(available_columns, ("asof_date",)) or signal_date_column} AS asof_date,
                {_resolve_existing_column(available_columns, ("major_state",), field_name="major_state", table_name=table_name)} AS major_state,
                {_resolve_existing_column(available_columns, ("trend_direction",), field_name="trend_direction", table_name=table_name)} AS trend_direction,
                COALESCE({_resolve_optional_column(available_columns, ("reversal_stage",)) or "'none'"}, 'none') AS reversal_stage,
                COALESCE({_resolve_optional_column(available_columns, ("wave_id",)) or "0"}, 0) AS wave_id,
                COALESCE({_resolve_optional_column(available_columns, ("current_hh_count",)) or "0"}, 0) AS current_hh_count,
                COALESCE({_resolve_optional_column(available_columns, ("current_ll_count",)) or "0"}, 0) AS current_ll_count,
                {_resolve_optional_column(available_columns, ("daily_major_state",)) or "NULL"} AS daily_major_state,
                {_resolve_optional_column(available_columns, ("daily_trend_direction",)) or "NULL"} AS daily_trend_direction,
                {_resolve_optional_column(available_columns, ("daily_reversal_stage",)) or "NULL"} AS daily_reversal_stage,
                {_resolve_optional_column(available_columns, ("daily_wave_id",)) or "NULL"} AS daily_wave_id,
                {_resolve_optional_column(available_columns, ("daily_current_hh_count",)) or "NULL"} AS daily_current_hh_count,
                {_resolve_optional_column(available_columns, ("daily_current_ll_count",)) or "NULL"} AS daily_current_ll_count,
                {_resolve_optional_column(available_columns, ("daily_source_context_nk",)) or "NULL"} AS daily_source_context_nk,
                {_resolve_optional_column(available_columns, ("weekly_major_state",)) or "NULL"} AS weekly_major_state,
                {_resolve_optional_column(available_columns, ("weekly_trend_direction",)) or "NULL"} AS weekly_trend_direction,
                {_resolve_optional_column(available_columns, ("weekly_reversal_stage",)) or "NULL"} AS weekly_reversal_stage,
                {_resolve_optional_column(available_columns, ("weekly_wave_id",)) or "NULL"} AS weekly_wave_id,
                {_resolve_optional_column(available_columns, ("weekly_current_hh_count",)) or "NULL"} AS weekly_current_hh_count,
                {_resolve_optional_column(available_columns, ("weekly_current_ll_count",)) or "NULL"} AS weekly_current_ll_count,
                {_resolve_optional_column(available_columns, ("weekly_source_context_nk",)) or "NULL"} AS weekly_source_context_nk,
                {_resolve_optional_column(available_columns, ("monthly_major_state",)) or "NULL"} AS monthly_major_state,
                {_resolve_optional_column(available_columns, ("monthly_trend_direction",)) or "NULL"} AS monthly_trend_direction,
                {_resolve_optional_column(available_columns, ("monthly_reversal_stage",)) or "NULL"} AS monthly_reversal_stage,
                {_resolve_optional_column(available_columns, ("monthly_wave_id",)) or "NULL"} AS monthly_wave_id,
                {_resolve_optional_column(available_columns, ("monthly_current_hh_count",)) or "NULL"} AS monthly_current_hh_count,
                {_resolve_optional_column(available_columns, ("monthly_current_ll_count",)) or "NULL"} AS monthly_current_ll_count,
                {_resolve_optional_column(available_columns, ("monthly_source_context_nk",)) or "NULL"} AS monthly_source_context_nk,
                {_resolve_optional_column(available_columns, ("structure_progress_state",)) or "'unknown'"} AS structure_progress_state,
                {_resolve_optional_column(available_columns, ("break_confirmation_status",)) or "NULL"} AS break_confirmation_status,
                {_resolve_optional_column(available_columns, ("break_confirmation_ref",)) or "NULL"} AS break_confirmation_ref,
                {_resolve_optional_column(available_columns, ("stats_snapshot_nk",)) or "NULL"} AS stats_snapshot_nk,
                {_resolve_optional_column(available_columns, ("exhaustion_risk_bucket",)) or "NULL"} AS exhaustion_risk_bucket,
                {_resolve_optional_column(available_columns, ("reversal_probability_bucket",)) or "NULL"} AS reversal_probability_bucket,
                {_resolve_existing_column(available_columns, ("source_context_nk",), field_name="source_context_nk", table_name=table_name)} AS source_context_nk
            FROM {table_name}
            {where_sql}
            ORDER BY signal_date, instrument, structure_snapshot_nk
            LIMIT ?
            """,
            [*parameters, limit],
        ).fetchall()
        return [
            _StructureSnapshotInputRow(
                structure_snapshot_nk=str(row[0]),
                instrument=str(row[1]),
                signal_date=_normalize_date_value(row[2], field_name="signal_date"),
                asof_date=_normalize_date_value(row[3], field_name="asof_date"),
                major_state=_normalize_optional_str(row[4], default="未知"),
                trend_direction=_normalize_optional_str(row[5], default="down").lower(),
                reversal_stage=_normalize_optional_str(row[6], default="none").lower(),
                wave_id=_normalize_optional_int(row[7]),
                current_hh_count=_normalize_optional_int(row[8]),
                current_ll_count=_normalize_optional_int(row[9]),
                daily_major_state=_normalize_optional_nullable_str(row[10]),
                daily_trend_direction=_normalize_optional_nullable_str(row[11]),
                daily_reversal_stage=_normalize_optional_nullable_str(row[12]),
                daily_wave_id=_normalize_optional_nullable_int(row[13]),
                daily_current_hh_count=_normalize_optional_nullable_int(row[14]),
                daily_current_ll_count=_normalize_optional_nullable_int(row[15]),
                daily_source_context_nk=_normalize_optional_nullable_str(row[16]),
                weekly_major_state=_normalize_optional_nullable_str(row[17]),
                weekly_trend_direction=_normalize_optional_nullable_str(row[18]),
                weekly_reversal_stage=_normalize_optional_nullable_str(row[19]),
                weekly_wave_id=_normalize_optional_nullable_int(row[20]),
                weekly_current_hh_count=_normalize_optional_nullable_int(row[21]),
                weekly_current_ll_count=_normalize_optional_nullable_int(row[22]),
                weekly_source_context_nk=_normalize_optional_nullable_str(row[23]),
                monthly_major_state=_normalize_optional_nullable_str(row[24]),
                monthly_trend_direction=_normalize_optional_nullable_str(row[25]),
                monthly_reversal_stage=_normalize_optional_nullable_str(row[26]),
                monthly_wave_id=_normalize_optional_nullable_int(row[27]),
                monthly_current_hh_count=_normalize_optional_nullable_int(row[28]),
                monthly_current_ll_count=_normalize_optional_nullable_int(row[29]),
                monthly_source_context_nk=_normalize_optional_nullable_str(row[30]),
                structure_progress_state=_normalize_progress_state(row[31]),
                break_confirmation_status=_normalize_optional_nullable_str(row[32]),
                break_confirmation_ref=_normalize_optional_nullable_str(row[33]),
                stats_snapshot_nk=_normalize_optional_nullable_str(row[34]),
                exhaustion_risk_bucket=_normalize_optional_nullable_str(row[35]),
                reversal_probability_bucket=_normalize_optional_nullable_str(row[36]),
                source_context_nk=str(row[37]),
            )
            for row in rows
        ]
    finally:
        connection.close()


def _load_context_presence(
    *,
    malf_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> set[tuple[str, date]]:
    if not malf_path.exists() or not instruments:
        return set()
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument", "entity_code", "code"),
            field_name="instrument",
            table_name=table_name,
        )
        signal_date_column = _resolve_existing_column(
            available_columns,
            ("signal_date", "asof_bar_dt"),
            field_name="signal_date/asof_bar_dt",
            table_name=table_name,
        )
        timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
        asset_type_column = _resolve_optional_column(available_columns, ("asset_type",))
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
        if asset_type_column is not None:
            where_clauses.append(f"{asset_type_column} = ?")
            parameters.append("stock")
        rows = connection.execute(
            f"""
            SELECT DISTINCT
                {instrument_column} AS instrument,
                {signal_date_column} AS signal_date
            FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            """,
            parameters,
        ).fetchall()
        return {
            (str(row[0]), _normalize_date_value(row[1], field_name="signal_date"))
            for row in rows
        }
    except ValueError:
        return set()
    finally:
        connection.close()


def _load_objective_status_rows(
    *,
    raw_market_path: Path,
    table_name: str,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> dict[str, list[_ObjectiveStatusInputRow]]:
    if not raw_market_path.exists() or not instruments:
        return {}
    connection = duckdb.connect(str(raw_market_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
        instrument_column = _resolve_existing_column(
            available_columns,
            ("code", "instrument"),
            field_name="code/instrument",
            table_name=table_name,
        )
        observed_trade_date_column = _resolve_existing_column(
            available_columns,
            ("observed_trade_date", "signal_date", "trade_date"),
            field_name="observed_trade_date",
            table_name=table_name,
        )
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"{instrument_column} IN ({placeholders})"]
        if signal_end_date is not None:
            where_clauses.append(f"{observed_trade_date_column} <= ?")
            parameters.append(signal_end_date)
        rows = connection.execute(
            f"""
            SELECT
                {instrument_column} AS instrument,
                {observed_trade_date_column} AS observed_trade_date,
                {_resolve_optional_column(available_columns, ("market_type",)) or "NULL"} AS market_type,
                {_resolve_optional_column(available_columns, ("security_type",)) or "NULL"} AS security_type,
                {_resolve_optional_column(available_columns, ("suspension_status",)) or "NULL"} AS suspension_status,
                {_resolve_optional_column(available_columns, ("risk_warning_status",)) or "NULL"} AS risk_warning_status,
                {_resolve_optional_column(available_columns, ("delisting_status",)) or "NULL"} AS delisting_status,
                COALESCE({_resolve_optional_column(available_columns, ("is_suspended_or_unresumed",)) or "FALSE"}, FALSE) AS is_suspended_or_unresumed,
                COALESCE({_resolve_optional_column(available_columns, ("is_risk_warning_excluded",)) or "FALSE"}, FALSE) AS is_risk_warning_excluded,
                COALESCE({_resolve_optional_column(available_columns, ("is_delisting_arrangement",)) or "FALSE"}, FALSE) AS is_delisting_arrangement,
                {_resolve_optional_column(available_columns, ("source_request_nk",)) or "NULL"} AS source_request_nk
            FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY instrument, observed_trade_date
            """,
            parameters,
        ).fetchall()
    except ValueError:
        return {}
    finally:
        connection.close()

    objective_rows: dict[str, list[_ObjectiveStatusInputRow]] = {}
    for row in rows:
        objective_rows.setdefault(str(row[0]), []).append(
            _ObjectiveStatusInputRow(
                instrument=str(row[0]),
                observed_trade_date=_normalize_date_value(row[1], field_name="observed_trade_date"),
                market_type=_normalize_optional_nullable_str(row[2]),
                security_type=_normalize_optional_nullable_str(row[3]),
                suspension_status=_normalize_optional_nullable_str(row[4]),
                risk_warning_status=_normalize_optional_nullable_str(row[5]),
                delisting_status=_normalize_optional_nullable_str(row[6]),
                is_suspended_or_unresumed=bool(row[7]),
                is_risk_warning_excluded=bool(row[8]),
                is_delisting_arrangement=bool(row[9]),
                source_request_nk=_normalize_optional_nullable_str(row[10]),
            )
        )
    return objective_rows


def _resolve_objective_status_for_signal(
    objective_rows_by_instrument: dict[str, list[_ObjectiveStatusInputRow]],
    *,
    instrument: str,
    signal_date: date,
) -> _ObjectiveStatusInputRow | None:
    rows = objective_rows_by_instrument.get(instrument)
    if not rows:
        return None
    selected_row: _ObjectiveStatusInputRow | None = None
    for row in rows:
        if row.observed_trade_date <= signal_date:
            selected_row = row
        else:
            break
    return selected_row
