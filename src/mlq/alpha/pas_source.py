"""alpha PAS detector 的官方上游读取。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from mlq.alpha.pas_shared import (
    _DetectorScopeRow,
    _normalize_date_value,
    _normalize_optional_int,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
)


def _load_detector_scope_rows(
    *,
    filter_path: Path,
    structure_path: Path,
    filter_table_name: str,
    structure_table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_DetectorScopeRow]:
    connection = duckdb.connect(str(filter_path), read_only=True)
    try:
        structure_path_sql = str(structure_path).replace("\\", "/").replace("'", "''")
        connection.execute(f"ATTACH '{structure_path_sql}' AS structure_db")
        where_clauses: list[str] = []
        parameters: list[object] = []
        if signal_start_date is not None:
            where_clauses.append("f.signal_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("f.signal_date <= ?")
            parameters.append(signal_end_date)
        if instruments:
            placeholders = ", ".join("?" for _ in instruments)
            where_clauses.append(f"f.instrument IN ({placeholders})")
            parameters.extend(instruments)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
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
                    f.break_confirmation_status,
                    f.break_confirmation_ref,
                    f.exhaustion_risk_bucket,
                    f.reversal_probability_bucket,
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
                {where_sql}
            )
            SELECT
                rf.filter_snapshot_nk,
                rf.structure_snapshot_nk,
                rf.instrument,
                rf.signal_date,
                rf.asof_date,
                rf.trigger_admissible,
                rf.primary_blocking_condition,
                rf.break_confirmation_status,
                rf.break_confirmation_ref,
                rf.exhaustion_risk_bucket,
                rf.reversal_probability_bucket,
                s.major_state,
                s.trend_direction,
                s.reversal_stage,
                s.wave_id,
                s.current_hh_count,
                s.current_ll_count,
                s.structure_progress_state,
                rf.daily_source_context_nk,
                rf.weekly_major_state,
                rf.weekly_trend_direction,
                rf.weekly_reversal_stage,
                rf.weekly_source_context_nk,
                rf.monthly_major_state,
                rf.monthly_trend_direction,
                rf.monthly_reversal_stage,
                rf.monthly_source_context_nk
            FROM ranked_filter AS rf
            INNER JOIN structure_db.main.{structure_table_name} AS s
                ON s.structure_snapshot_nk = rf.structure_snapshot_nk
            WHERE rf.row_rank = 1
            ORDER BY rf.signal_date, rf.instrument
            LIMIT ?
            """,
            [*parameters, limit],
        ).fetchall()
    finally:
        connection.close()
    return [
        _DetectorScopeRow(
            filter_snapshot_nk=str(row[0]),
            structure_snapshot_nk=str(row[1]),
            instrument=str(row[2]),
            signal_date=_normalize_date_value(row[3], field_name="signal_date"),
            asof_date=_normalize_date_value(row[4], field_name="asof_date"),
            trigger_admissible=bool(row[5]),
            primary_blocking_condition=_normalize_optional_nullable_str(row[6]),
            break_confirmation_status=_normalize_optional_nullable_str(row[7]),
            break_confirmation_ref=_normalize_optional_nullable_str(row[8]),
            exhaustion_risk_bucket=_normalize_optional_nullable_str(row[9]),
            reversal_probability_bucket=_normalize_optional_nullable_str(row[10]),
            major_state=_normalize_optional_str(row[11]),
            trend_direction=_normalize_optional_str(row[12]).lower(),
            reversal_stage=_normalize_optional_str(row[13]).lower(),
            wave_id=_normalize_optional_int(row[14]),
            current_hh_count=_normalize_optional_int(row[15]),
            current_ll_count=_normalize_optional_int(row[16]),
            structure_progress_state=_normalize_optional_str(row[17], default="unknown").lower(),
            daily_source_context_nk=_normalize_optional_nullable_str(row[18]),
            weekly_major_state=_normalize_optional_nullable_str(row[19]),
            weekly_trend_direction=_normalize_optional_nullable_str(row[20]),
            weekly_reversal_stage=_normalize_optional_nullable_str(row[21]),
            weekly_source_context_nk=_normalize_optional_nullable_str(row[22]),
            monthly_major_state=_normalize_optional_nullable_str(row[23]),
            monthly_trend_direction=_normalize_optional_nullable_str(row[24]),
            monthly_reversal_stage=_normalize_optional_nullable_str(row[25]),
            monthly_source_context_nk=_normalize_optional_nullable_str(row[26]),
        )
        for row in rows
    ]


def _load_price_history(
    *,
    market_base_path: Path,
    table_name: str,
    adjust_method: str,
    history_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> pd.DataFrame:
    if not instruments:
        return pd.DataFrame(
            columns=["instrument", "date", "adj_open", "adj_high", "adj_low", "adj_close", "volume", "volume_ma20"]
        )
    connection = duckdb.connect(str(market_base_path), read_only=True)
    try:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses = [f"code IN ({placeholders})", "adjust_method = ?"]
        parameters: list[object] = [*instruments, adjust_method]
        if history_start_date is not None:
            where_clauses.append("trade_date >= ?")
            parameters.append(history_start_date)
        if signal_end_date is not None:
            where_clauses.append("trade_date <= ?")
            parameters.append(signal_end_date)
        rows = connection.execute(
            f"""
            SELECT
                code AS instrument,
                trade_date AS date,
                open AS adj_open,
                high AS adj_high,
                low AS adj_low,
                close AS adj_close,
                COALESCE(volume, 0.0) AS volume
            FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY code, trade_date
            """,
            parameters,
        ).fetchdf()
    finally:
        connection.close()
    if rows.empty:
        rows["volume_ma20"] = pd.Series(dtype="float64")
        return rows
    rows["date"] = pd.to_datetime(rows["date"])
    rows["volume_ma20"] = (
        rows.groupby("instrument")["volume"]
        .transform(lambda values: values.rolling(20, min_periods=1).mean().shift(1).fillna(0.0))
        .astype("float64")
    )
    return rows
