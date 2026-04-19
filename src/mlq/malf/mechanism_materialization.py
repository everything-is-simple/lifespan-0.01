"""承接 `malf mechanism` runner 的统计派生、落表与审计写回。"""

from __future__ import annotations

import json
import math
from datetime import date
from statistics import mean, pstdev

import duckdb

from mlq.malf.bootstrap import (
    MALF_MECHANISM_RUN_TABLE,
    PIVOT_CONFIRMED_BREAK_LEDGER_TABLE,
    SAME_TIMEFRAME_STATS_PROFILE_TABLE,
    SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE,
)
from mlq.malf.mechanism_shared import (
    MalfMechanismBuildSummary,
    _METRIC_FIELDS,
    _MechanismInputRow,
    _build_break_event_nk,
    _build_confirmation_pivot_id,
    _build_guard_pivot_id,
    _build_stats_profile_nk,
    _build_stats_snapshot_nk,
    _coerce_date,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
)


def _derive_break_rows(
    *,
    input_rows: list[_MechanismInputRow],
    timeframe: str,
    run_id: str,
) -> list[dict[str, object]]:
    break_rows: list[dict[str, object]] = []
    grouped_rows = _group_rows_by_instrument(input_rows)
    for instrument_rows in grouped_rows.values():
        for index, row in enumerate(instrument_rows):
            if not row.is_failed_extreme and row.failure_type is None:
                continue
            direction = _derive_break_direction(row)
            confirmation_status, confirmation_row = _derive_confirmation(
                trigger_row=row,
                direction=direction,
                future_rows=instrument_rows[index + 1 : index + 4],
            )
            guard_pivot_id = _build_guard_pivot_id(
                instrument=row.instrument,
                timeframe=timeframe,
                signal_date=row.signal_date,
                direction=direction,
            )
            break_rows.append(
                {
                    "break_event_nk": _build_break_event_nk(
                        instrument=row.instrument,
                        timeframe=timeframe,
                        guard_pivot_id=guard_pivot_id,
                        trigger_bar_dt=row.signal_date,
                    ),
                    "instrument": row.instrument,
                    "timeframe": timeframe,
                    "guard_pivot_id": guard_pivot_id,
                    "guard_pivot_role": "LOW_GUARD" if direction == "DOWN" else "HIGH_GUARD",
                    "origin_context": row.malf_context_4,
                    "trigger_bar_dt": row.signal_date,
                    "trigger_price_proxy": float(row.new_high_count - row.new_low_count),
                    "break_direction": direction,
                    "confirmation_status": confirmation_status,
                    "confirmation_bar_dt": None if confirmation_row is None else confirmation_row.signal_date,
                    "confirmation_pivot_id": None
                    if confirmation_row is None
                    else _build_confirmation_pivot_id(
                        instrument=confirmation_row.instrument,
                        timeframe=timeframe,
                        signal_date=confirmation_row.signal_date,
                        direction=direction,
                    ),
                    "confirmation_pivot_role": None if confirmation_row is None else "CONFIRMED_PROGRESS",
                    "source_context_nk": row.source_context_nk,
                    "source_candidate_nk": row.source_candidate_nk,
                    "first_seen_run_id": run_id,
                    "last_materialized_run_id": run_id,
                }
            )
    return break_rows


def _derive_profile_rows(
    *,
    input_rows: list[_MechanismInputRow],
    timeframe: str,
    stats_sample_version: str,
    run_id: str,
) -> tuple[list[dict[str, object]], dict[tuple[str, str], list[float]]]:
    grouped_samples: dict[tuple[str, str], list[float]] = {}
    for row in input_rows:
        for metric_name in _METRIC_FIELDS:
            grouped_samples.setdefault((row.malf_context_4, metric_name), []).append(float(getattr(row, metric_name)))

    profile_rows: list[dict[str, object]] = []
    for (regime_family, metric_name), samples in sorted(grouped_samples.items()):
        ordered_samples = sorted(samples)
        stats_profile_nk = _build_stats_profile_nk(
            universe="ALL",
            timeframe=timeframe,
            regime_family=regime_family,
            metric_name=metric_name,
            sample_version=stats_sample_version,
        )
        profile_rows.append(
            {
                "stats_profile_nk": stats_profile_nk,
                "universe": "ALL",
                "timeframe": timeframe,
                "regime_family": regime_family,
                "metric_name": metric_name,
                "sample_version": stats_sample_version,
                "sample_size": len(ordered_samples),
                "p10": _percentile(ordered_samples, 0.10),
                "p25": _percentile(ordered_samples, 0.25),
                "p50": _percentile(ordered_samples, 0.50),
                "p75": _percentile(ordered_samples, 0.75),
                "p90": _percentile(ordered_samples, 0.90),
                "mean": mean(ordered_samples) if ordered_samples else 0.0,
                "std": pstdev(ordered_samples) if len(ordered_samples) > 1 else 0.0,
                "bucket_definition_json": json.dumps(
                    {"elevated_at_or_above": "p75", "high_at_or_above": "p90"},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "first_seen_run_id": run_id,
                "last_materialized_run_id": run_id,
            }
        )
    return profile_rows, grouped_samples


def _derive_snapshot_rows(
    *,
    input_rows: list[_MechanismInputRow],
    timeframe: str,
    stats_sample_version: str,
    mechanism_contract_version: str,
    run_id: str,
    profile_rows: list[dict[str, object]],
    metric_samples: dict[tuple[str, str], list[float]],
) -> list[dict[str, object]]:
    profile_refs = {
        (str(row["regime_family"]), str(row["metric_name"])): str(row["stats_profile_nk"])
        for row in profile_rows
    }
    snapshot_rows: list[dict[str, object]] = []
    for row in input_rows:
        metric_percentiles = {
            metric_name: _empirical_percentile(
                metric_samples.get((row.malf_context_4, metric_name), []),
                float(getattr(row, metric_name)),
            )
            for metric_name in _METRIC_FIELDS
        }
        exhaustion_signal = max(
            metric_percentiles["new_high_count"],
            metric_percentiles["new_low_count"],
            metric_percentiles["advancement_density"],
        )
        reversal_signal = max(metric_percentiles["refresh_density"], 1.0 if row.is_failed_extreme else 0.0)
        snapshot_rows.append(
            {
                "stats_snapshot_nk": _build_stats_snapshot_nk(
                    instrument=row.instrument,
                    timeframe=timeframe,
                    asof_bar_dt=row.asof_date,
                    sample_version=stats_sample_version,
                    mechanism_contract_version=mechanism_contract_version,
                ),
                "instrument": row.instrument,
                "timeframe": timeframe,
                "signal_date": row.signal_date,
                "asof_bar_dt": row.asof_date,
                "regime_family": row.malf_context_4,
                "sample_version": stats_sample_version,
                "stats_contract_version": mechanism_contract_version,
                "source_context_nk": row.source_context_nk,
                "source_candidate_nk": row.source_candidate_nk,
                "new_high_count_percentile": metric_percentiles["new_high_count"],
                "new_low_count_percentile": metric_percentiles["new_low_count"],
                "refresh_density_percentile": metric_percentiles["refresh_density"],
                "advancement_density_percentile": metric_percentiles["advancement_density"],
                "exhaustion_risk_bucket": _percentile_bucket(exhaustion_signal),
                "reversal_probability_bucket": _percentile_bucket(reversal_signal),
                "source_profile_refs_json": json.dumps(
                    {
                        metric_name: profile_refs.get((row.malf_context_4, metric_name))
                        for metric_name in _METRIC_FIELDS
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "first_seen_run_id": run_id,
                "last_materialized_run_id": run_id,
            }
        )
    return snapshot_rows


def _materialize_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    break_rows: list[dict[str, object]],
    profile_rows: list[dict[str, object]],
    snapshot_rows: list[dict[str, object]],
) -> dict[str, int]:
    counts = {
        "break_inserted_count": 0,
        "break_reused_count": 0,
        "break_rematerialized_count": 0,
        "profile_inserted_count": 0,
        "profile_reused_count": 0,
        "profile_rematerialized_count": 0,
        "snapshot_inserted_count": 0,
        "snapshot_reused_count": 0,
        "snapshot_rematerialized_count": 0,
    }
    for row in break_rows:
        counts[f"break_{_upsert_break_row(connection, row=row)}_count"] += 1
    for row in profile_rows:
        counts[f"profile_{_upsert_profile_row(connection, row=row)}_count"] += 1
    for row in snapshot_rows:
        counts[f"snapshot_{_upsert_snapshot_row(connection, row=row)}_count"] += 1
    return counts


def _upsert_break_row(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT
            guard_pivot_role,
            origin_context,
            break_direction,
            confirmation_status,
            confirmation_bar_dt,
            confirmation_pivot_id,
            confirmation_pivot_role,
            source_context_nk,
            source_candidate_nk,
            first_seen_run_id
        FROM {PIVOT_CONFIRMED_BREAK_LEDGER_TABLE}
        WHERE break_event_nk = ?
        """,
        [row["break_event_nk"]],
    ).fetchone()
    fingerprint = (
        str(row["guard_pivot_role"]),
        str(row["origin_context"]),
        str(row["break_direction"]),
        str(row["confirmation_status"]),
        _coerce_date(row["confirmation_bar_dt"]),
        _normalize_optional_nullable_str(row["confirmation_pivot_id"]),
        _normalize_optional_nullable_str(row["confirmation_pivot_role"]),
        str(row["source_context_nk"]),
        str(row["source_candidate_nk"]),
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {PIVOT_CONFIRMED_BREAK_LEDGER_TABLE} (
                break_event_nk,
                instrument,
                timeframe,
                guard_pivot_id,
                guard_pivot_role,
                origin_context,
                trigger_bar_dt,
                trigger_price_proxy,
                break_direction,
                confirmation_status,
                confirmation_bar_dt,
                confirmation_pivot_id,
                confirmation_pivot_role,
                source_context_nk,
                source_candidate_nk,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [row[column] for column in (
                "break_event_nk",
                "instrument",
                "timeframe",
                "guard_pivot_id",
                "guard_pivot_role",
                "origin_context",
                "trigger_bar_dt",
                "trigger_price_proxy",
                "break_direction",
                "confirmation_status",
                "confirmation_bar_dt",
                "confirmation_pivot_id",
                "confirmation_pivot_role",
                "source_context_nk",
                "source_candidate_nk",
                "first_seen_run_id",
                "last_materialized_run_id",
            )],
        )
        return "inserted"
    existing_fingerprint = (
        _normalize_optional_str(existing[0]),
        _normalize_optional_str(existing[1]),
        _normalize_optional_str(existing[2]),
        _normalize_optional_str(existing[3]),
        _coerce_date(existing[4]),
        _normalize_optional_nullable_str(existing[5]),
        _normalize_optional_nullable_str(existing[6]),
        _normalize_optional_str(existing[7]),
        _normalize_optional_str(existing[8]),
    )
    first_seen_run_id = _normalize_optional_str(existing[9], default=str(row["first_seen_run_id"]))
    connection.execute(
        f"""
        UPDATE {PIVOT_CONFIRMED_BREAK_LEDGER_TABLE}
        SET
            guard_pivot_role = ?,
            origin_context = ?,
            break_direction = ?,
            confirmation_status = ?,
            confirmation_bar_dt = ?,
            confirmation_pivot_id = ?,
            confirmation_pivot_role = ?,
            source_context_nk = ?,
            source_candidate_nk = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE break_event_nk = ?
        """,
        [
            row["guard_pivot_role"],
            row["origin_context"],
            row["break_direction"],
            row["confirmation_status"],
            row["confirmation_bar_dt"],
            row["confirmation_pivot_id"],
            row["confirmation_pivot_role"],
            row["source_context_nk"],
            row["source_candidate_nk"],
            first_seen_run_id,
            row["last_materialized_run_id"],
            row["break_event_nk"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _upsert_profile_row(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT sample_size, p10, p25, p50, p75, p90, mean, std, bucket_definition_json, first_seen_run_id
        FROM {SAME_TIMEFRAME_STATS_PROFILE_TABLE}
        WHERE stats_profile_nk = ?
        """,
        [row["stats_profile_nk"]],
    ).fetchone()
    fingerprint = tuple(
        row[key]
        for key in ("sample_size", "p10", "p25", "p50", "p75", "p90", "mean", "std", "bucket_definition_json")
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {SAME_TIMEFRAME_STATS_PROFILE_TABLE} (
                stats_profile_nk, universe, timeframe, regime_family, metric_name, sample_version,
                sample_size, p10, p25, p50, p75, p90, mean, std, bucket_definition_json,
                first_seen_run_id, last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [row[column] for column in (
                "stats_profile_nk", "universe", "timeframe", "regime_family", "metric_name", "sample_version",
                "sample_size", "p10", "p25", "p50", "p75", "p90", "mean", "std", "bucket_definition_json",
                "first_seen_run_id", "last_materialized_run_id",
            )],
        )
        return "inserted"
    existing_fingerprint = tuple(existing[index] for index in range(9))
    first_seen_run_id = _normalize_optional_str(existing[9], default=str(row["first_seen_run_id"]))
    connection.execute(
        f"""
        UPDATE {SAME_TIMEFRAME_STATS_PROFILE_TABLE}
        SET
            sample_size = ?, p10 = ?, p25 = ?, p50 = ?, p75 = ?, p90 = ?, mean = ?, std = ?,
            bucket_definition_json = ?, first_seen_run_id = ?, last_materialized_run_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE stats_profile_nk = ?
        """,
        [
            row["sample_size"],
            row["p10"],
            row["p25"],
            row["p50"],
            row["p75"],
            row["p90"],
            row["mean"],
            row["std"],
            row["bucket_definition_json"],
            first_seen_run_id,
            row["last_materialized_run_id"],
            row["stats_profile_nk"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _upsert_snapshot_row(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT
            regime_family, new_high_count_percentile, new_low_count_percentile, refresh_density_percentile,
            advancement_density_percentile, exhaustion_risk_bucket, reversal_probability_bucket,
            source_profile_refs_json, first_seen_run_id
        FROM {SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE}
        WHERE stats_snapshot_nk = ?
        """,
        [row["stats_snapshot_nk"]],
    ).fetchone()
    fingerprint = tuple(
        row[key]
        for key in (
            "regime_family",
            "new_high_count_percentile",
            "new_low_count_percentile",
            "refresh_density_percentile",
            "advancement_density_percentile",
            "exhaustion_risk_bucket",
            "reversal_probability_bucket",
            "source_profile_refs_json",
        )
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE} (
                stats_snapshot_nk, instrument, timeframe, signal_date, asof_bar_dt, regime_family,
                sample_version, stats_contract_version, source_context_nk, source_candidate_nk,
                new_high_count_percentile, new_low_count_percentile, refresh_density_percentile,
                advancement_density_percentile, exhaustion_risk_bucket, reversal_probability_bucket,
                source_profile_refs_json, first_seen_run_id, last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [row[column] for column in (
                "stats_snapshot_nk", "instrument", "timeframe", "signal_date", "asof_bar_dt", "regime_family",
                "sample_version", "stats_contract_version", "source_context_nk", "source_candidate_nk",
                "new_high_count_percentile", "new_low_count_percentile", "refresh_density_percentile",
                "advancement_density_percentile", "exhaustion_risk_bucket", "reversal_probability_bucket",
                "source_profile_refs_json", "first_seen_run_id", "last_materialized_run_id",
            )],
        )
        return "inserted"
    existing_fingerprint = tuple(existing[index] for index in range(8))
    first_seen_run_id = _normalize_optional_str(existing[8], default=str(row["first_seen_run_id"]))
    connection.execute(
        f"""
        UPDATE {SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE}
        SET
            regime_family = ?, new_high_count_percentile = ?, new_low_count_percentile = ?,
            refresh_density_percentile = ?, advancement_density_percentile = ?, exhaustion_risk_bucket = ?,
            reversal_probability_bucket = ?, source_profile_refs_json = ?, first_seen_run_id = ?,
            last_materialized_run_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE stats_snapshot_nk = ?
        """,
        [
            row["regime_family"],
            row["new_high_count_percentile"],
            row["new_low_count_percentile"],
            row["refresh_density_percentile"],
            row["advancement_density_percentile"],
            row["exhaustion_risk_bucket"],
            row["reversal_probability_bucket"],
            row["source_profile_refs_json"],
            first_seen_run_id,
            row["last_materialized_run_id"],
            row["stats_snapshot_nk"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _upsert_checkpoints(
    connection: duckdb.DuckDBPyConnection,
    *,
    checkpoint_table: str,
    input_rows: list[_MechanismInputRow],
    timeframe: str,
    run_id: str,
) -> int:
    latest_rows: dict[str, _MechanismInputRow] = {}
    for row in input_rows:
        current = latest_rows.get(row.instrument)
        if current is None or (row.signal_date, row.asof_date) >= (current.signal_date, current.asof_date):
            latest_rows[row.instrument] = row
    for instrument, row in latest_rows.items():
        existing = connection.execute(
            f"""
            SELECT instrument
            FROM {checkpoint_table}
            WHERE instrument = ? AND timeframe = ?
            """,
            [instrument, timeframe],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {checkpoint_table} (
                    instrument, timeframe, last_signal_date, last_asof_date, last_run_id, source_context_nk
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [instrument, timeframe, row.signal_date, row.asof_date, run_id, row.source_context_nk],
            )
            continue
        connection.execute(
            f"""
            UPDATE {checkpoint_table}
            SET
                last_signal_date = ?,
                last_asof_date = ?,
                last_run_id = ?,
                source_context_nk = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE instrument = ? AND timeframe = ?
            """,
            [row.signal_date, row.asof_date, run_id, row.source_context_nk, instrument, timeframe],
        )
    return len(latest_rows)


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    source_context_table: str,
    source_structure_input_table: str,
    timeframe: str,
    stats_sample_version: str,
    mechanism_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {MALF_MECHANISM_RUN_TABLE} (
            run_id, runner_name, runner_version, run_status, signal_start_date, signal_end_date,
            bounded_instrument_count, source_context_table, source_structure_input_table, timeframe,
            stats_sample_version, mechanism_contract_version
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_context_table,
            source_structure_input_table,
            timeframe,
            stats_sample_version,
            mechanism_contract_version,
        ],
    )


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: MalfMechanismBuildSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_MECHANISM_RUN_TABLE}
        SET
            run_status = 'completed',
            bounded_instrument_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [summary.bounded_instrument_count, json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True), run_id],
    )


def _mark_run_failed(connection: duckdb.DuckDBPyConnection, *, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_MECHANISM_RUN_TABLE}
        SET run_status = 'failed', completed_at = CURRENT_TIMESTAMP
        WHERE run_id = ?
        """,
        [run_id],
    )


def _group_rows_by_instrument(input_rows: list[_MechanismInputRow]) -> dict[str, list[_MechanismInputRow]]:
    grouped: dict[str, list[_MechanismInputRow]] = {}
    for row in input_rows:
        grouped.setdefault(row.instrument, []).append(row)
    for rows in grouped.values():
        rows.sort(key=lambda item: (item.signal_date, item.asof_date))
    return grouped


def _derive_break_direction(row: _MechanismInputRow) -> str:
    if row.new_low_count > row.new_high_count:
        return "DOWN"
    if row.new_high_count > row.new_low_count:
        return "UP"
    if "BULL" in row.malf_context_4:
        return "DOWN"
    if "BEAR" in row.malf_context_4:
        return "UP"
    return "DOWN" if row.failure_type and "down" in row.failure_type.lower() else "UP"


def _derive_confirmation(
    *,
    trigger_row: _MechanismInputRow,
    direction: str,
    future_rows: list[_MechanismInputRow],
) -> tuple[str, _MechanismInputRow | None]:
    for row in future_rows:
        if direction == "DOWN" and (row.new_low_count > 0 or row.is_failed_extreme):
            return "confirmed", row
        if direction == "UP" and (
            row.new_high_count > 0 or (row.is_failed_extreme and row.new_high_count >= row.new_low_count)
        ):
            return "confirmed", row
    return "pending", None


def _percentile(values: list[float], level: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    index = (len(values) - 1) * level
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return float(values[lower])
    lower_value = float(values[lower])
    upper_value = float(values[upper])
    return lower_value + (upper_value - lower_value) * (index - lower)


def _empirical_percentile(values: list[float], value: float) -> float:
    if not values:
        return 0.0
    ordered_values = sorted(float(item) for item in values)
    less_or_equal = sum(1 for item in ordered_values if item <= float(value))
    return float(less_or_equal) / float(len(ordered_values))


def _percentile_bucket(value: float) -> str:
    if value >= 0.90:
        return "high"
    if value >= 0.75:
        return "elevated"
    return "normal"
