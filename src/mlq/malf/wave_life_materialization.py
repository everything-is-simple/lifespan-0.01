"""`malf wave life` runner 的 snapshot/profile 写入与审计落表。"""

from __future__ import annotations

import json
from datetime import date

import duckdb
import pandas as pd

from mlq.malf.bootstrap import (
    MALF_WAVE_LIFE_CHECKPOINT_TABLE,
    MALF_WAVE_LIFE_PROFILE_TABLE,
    MALF_WAVE_LIFE_RUN_TABLE,
    MALF_WAVE_LIFE_SNAPSHOT_TABLE,
    MALF_WAVE_LIFE_WORK_QUEUE_TABLE,
)
from mlq.malf.wave_life_shared import (
    MalfWaveLifeBuildSummary,
    _ProfileMaterialization,
    _StateRow,
    _WaveRow,
    _build_snapshot_nk,
    _derive_termination_risk_bucket,
    _estimate_remaining_life,
    _estimate_wave_life_percentile,
    _normalize_row_for_compare,
    _resolve_wave_nk,
    _to_python_date,
)


def _build_scope_snapshot_rows(
    *,
    asset_type: str,
    code: str,
    timeframe: str,
    state_rows: list[_StateRow],
    wave_map: dict[int, _WaveRow],
    profile_map: dict[tuple[str, str, str], _ProfileMaterialization],
    replay_start_bar_dt: date | None,
    replay_confirm_until_dt: date | None,
    sample_version: str,
    run_id: str,
) -> list[dict[str, object]]:
    wave_age_map: dict[int, int] = {}
    snapshot_rows: list[dict[str, object]] = []
    effective_start = replay_start_bar_dt or (state_rows[0].asof_bar_dt if state_rows else None)
    effective_end = replay_confirm_until_dt or (state_rows[-1].asof_bar_dt if state_rows else None)
    for state_row in state_rows:
        wave_age_map[state_row.wave_id] = wave_age_map.get(state_row.wave_id, 0) + 1
        if effective_start is not None and state_row.asof_bar_dt < effective_start:
            continue
        if effective_end is not None and state_row.asof_bar_dt > effective_end:
            continue
        wave_row = wave_map.get(state_row.wave_id)
        profile = profile_map.get((state_row.timeframe, state_row.major_state, state_row.reversal_stage))
        active_wave_bar_age = wave_age_map[state_row.wave_id]
        wave_life_percentile = _estimate_wave_life_percentile(profile, active_wave_bar_age)
        remaining_life_bars_p50 = _estimate_remaining_life(profile.p50 if profile else None, active_wave_bar_age)
        remaining_life_bars_p75 = _estimate_remaining_life(profile.p75 if profile else None, active_wave_bar_age)
        snapshot_rows.append(
            {
                "snapshot_nk": _build_snapshot_nk(asset_type=asset_type, code=code, timeframe=timeframe, asof_bar_dt=state_row.asof_bar_dt),
                "asset_type": asset_type,
                "code": code,
                "timeframe": timeframe,
                "asof_bar_dt": state_row.asof_bar_dt,
                "wave_id": state_row.wave_id,
                "source_wave_nk": _resolve_wave_nk(wave_row=wave_row, asset_type=asset_type, code=code, timeframe=timeframe, wave_id=state_row.wave_id),
                "source_state_snapshot_nk": state_row.snapshot_nk,
                "major_state": state_row.major_state,
                "reversal_stage": state_row.reversal_stage,
                "active_wave_bar_age": active_wave_bar_age,
                "wave_life_percentile": wave_life_percentile,
                "remaining_life_bars_p50": remaining_life_bars_p50,
                "remaining_life_bars_p75": remaining_life_bars_p75,
                "termination_risk_bucket": _derive_termination_risk_bucket(wave_life_percentile),
                "sample_size": 0 if profile is None else profile.sample_size,
                "sample_version": sample_version,
                "source_profile_nk": None if profile is None else profile.profile_nk,
                "profile_origin": None if profile is None else profile.profile_origin,
                "first_seen_run_id": run_id,
                "last_materialized_run_id": run_id,
            }
        )
    return snapshot_rows


def _replace_scope_snapshot_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    code: str,
    timeframe: str,
    replay_start_bar_dt: date | None,
    replay_confirm_until_dt: date | None,
    rows: list[dict[str, object]],
) -> dict[str, int]:
    effective_start = replay_start_bar_dt or min((_to_python_date(row["asof_bar_dt"]) for row in rows), default=None)
    effective_end = replay_confirm_until_dt or max((_to_python_date(row["asof_bar_dt"]) for row in rows), default=None)
    where_clauses = ["asset_type = ?", "code = ?", "timeframe = ?"]
    where_params: list[object] = [asset_type, code, timeframe]
    if effective_start is not None:
        where_clauses.append("asof_bar_dt >= ?")
        where_params.append(effective_start)
    if effective_end is not None:
        where_clauses.append("asof_bar_dt <= ?")
        where_params.append(effective_end)
    existing_rows = connection.execute(
        f"SELECT * FROM {MALF_WAVE_LIFE_SNAPSHOT_TABLE} WHERE {' AND '.join(where_clauses)}",
        where_params,
    ).fetchdf()
    existing_map: dict[str, dict[str, object]] = {}
    if not existing_rows.empty:
        for row in existing_rows.to_dict(orient="records"):
            existing_map[str(row["snapshot_nk"])] = row
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    for row in rows:
        existing_row = existing_map.get(str(row["snapshot_nk"]))
        if existing_row is None:
            inserted_count += 1
            continue
        row["first_seen_run_id"] = existing_row.get("first_seen_run_id") or row.get("first_seen_run_id")
        if _normalize_row_for_compare(existing_row) == _normalize_row_for_compare(row):
            reused_count += 1
        else:
            rematerialized_count += 1
    connection.execute(
        f"DELETE FROM {MALF_WAVE_LIFE_SNAPSHOT_TABLE} WHERE {' AND '.join(where_clauses)}",
        where_params,
    )
    if rows:
        frame = pd.DataFrame(rows)
        temp_name = "tmp_malf_wave_life_snapshot"
        connection.register(temp_name, frame)
        try:
            connection.execute(
                f"""
                INSERT INTO {MALF_WAVE_LIFE_SNAPSHOT_TABLE} ({', '.join(frame.columns)})
                SELECT {', '.join(frame.columns)}
                FROM {temp_name}
                """
            )
        finally:
            connection.unregister(temp_name)
    return {
        "inserted_count": inserted_count,
        "reused_count": reused_count,
        "rematerialized_count": rematerialized_count,
    }


def _replace_profile_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    timeframes: tuple[str, ...],
    sample_version: str,
    rows: list[dict[str, object]],
) -> dict[str, int]:
    placeholders = ", ".join("?" for _ in timeframes)
    existing_rows = connection.execute(
        f"""
        SELECT *
        FROM {MALF_WAVE_LIFE_PROFILE_TABLE}
        WHERE timeframe IN ({placeholders})
          AND sample_version = ?
        """,
        [*timeframes, sample_version],
    ).fetchdf()
    existing_map: dict[str, dict[str, object]] = {}
    if not existing_rows.empty:
        for row in existing_rows.to_dict(orient="records"):
            existing_map[str(row["profile_nk"])] = row
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    for row in rows:
        existing_row = existing_map.get(str(row["profile_nk"]))
        if existing_row is None:
            inserted_count += 1
            continue
        row["first_seen_run_id"] = existing_row.get("first_seen_run_id") or row.get("first_seen_run_id")
        if _normalize_row_for_compare(existing_row) == _normalize_row_for_compare(row):
            reused_count += 1
        else:
            rematerialized_count += 1
    connection.execute(
        f"""
        DELETE FROM {MALF_WAVE_LIFE_PROFILE_TABLE}
        WHERE timeframe IN ({placeholders})
          AND sample_version = ?
        """,
        [*timeframes, sample_version],
    )
    if rows:
        frame = pd.DataFrame(rows)
        temp_name = "tmp_malf_wave_life_profile"
        connection.register(temp_name, frame)
        try:
            connection.execute(
                f"""
                INSERT INTO {MALF_WAVE_LIFE_PROFILE_TABLE} ({', '.join(frame.columns)})
                SELECT {', '.join(frame.columns)}
                FROM {temp_name}
                """
            )
        finally:
            connection.unregister(temp_name)
    return {
        "inserted_count": inserted_count,
        "reused_count": reused_count,
        "rematerialized_count": rematerialized_count,
    }


def _upsert_wave_life_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    code: str,
    timeframe: str,
    last_completed_bar_dt: date | None,
    tail_start_bar_dt: date | None,
    tail_confirm_until_dt: date | None,
    last_sample_version: str,
    source_fingerprint: str,
    last_run_id: str,
) -> None:
    existing = connection.execute(
        f"""
        SELECT asset_type
        FROM {MALF_WAVE_LIFE_CHECKPOINT_TABLE}
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [asset_type, code, timeframe],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {MALF_WAVE_LIFE_CHECKPOINT_TABLE} (
                asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt,
                tail_confirm_until_dt, last_sample_version, source_fingerprint, last_run_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                asset_type,
                code,
                timeframe,
                last_completed_bar_dt,
                tail_start_bar_dt,
                tail_confirm_until_dt,
                last_sample_version,
                source_fingerprint,
                last_run_id,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {MALF_WAVE_LIFE_CHECKPOINT_TABLE}
        SET last_completed_bar_dt = ?,
            tail_start_bar_dt = ?,
            tail_confirm_until_dt = ?,
            last_sample_version = ?,
            source_fingerprint = ?,
            last_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [
            last_completed_bar_dt,
            tail_start_bar_dt,
            tail_confirm_until_dt,
            last_sample_version,
            source_fingerprint,
            last_run_id,
            asset_type,
            code,
            timeframe,
        ],
    )


def _mark_wave_life_queue_completed(connection: duckdb.DuckDBPyConnection, *, queue_nk: str, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_WAVE_LIFE_WORK_QUEUE_TABLE}
        SET queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _mark_wave_life_queue_failed(connection: duckdb.DuckDBPyConnection, *, queue_nk: str, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_WAVE_LIFE_WORK_QUEUE_TABLE}
        SET queue_status = 'failed',
            last_claimed_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    execution_mode: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_scope_count: int,
    claimed_scope_count: int,
    source_wave_table: str,
    source_state_table: str,
    source_stats_table: str,
    sample_version: str,
    life_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {MALF_WAVE_LIFE_RUN_TABLE} (
            run_id, runner_name, runner_version, run_status, execution_mode,
            signal_start_date, signal_end_date, bounded_scope_count, claimed_scope_count,
            source_wave_table, source_state_table, source_stats_table, sample_version, life_contract_version
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            execution_mode,
            signal_start_date,
            signal_end_date,
            bounded_scope_count,
            claimed_scope_count,
            source_wave_table,
            source_state_table,
            source_stats_table,
            sample_version,
            life_contract_version,
        ],
    )


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: MalfWaveLifeBuildSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_WAVE_LIFE_RUN_TABLE}
        SET run_status = 'completed',
            claimed_scope_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            summary.claimed_scope_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def _mark_run_failed(connection: duckdb.DuckDBPyConnection, *, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_WAVE_LIFE_RUN_TABLE}
        SET run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP
        WHERE run_id = ?
        """,
        [run_id],
    )
