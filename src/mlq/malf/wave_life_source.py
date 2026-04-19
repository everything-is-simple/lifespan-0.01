"""`malf wave life` runner 的源数据读取、分组与 queue 计算。"""

from __future__ import annotations

import json
from datetime import date

import duckdb
import pandas as pd

from mlq.malf.bootstrap import (
    MALF_CANONICAL_CHECKPOINT_TABLE,
    MALF_WAVE_LIFE_CHECKPOINT_TABLE,
    MALF_WAVE_LIFE_WORK_QUEUE_TABLE,
)
from mlq.malf.wave_life_shared import (
    DEFAULT_WAVE_LIFE_METRIC_NAME,
    _ProfileMaterialization,
    _StateRow,
    _WaveRow,
    _build_profile_nk,
    _build_queue_nk,
    _build_scope_nk,
    _coerce_optional_float,
    _series_quantile,
    _state_row_from_tuple,
    _to_python_date,
    _wave_row_from_tuple,
)


def _load_bounded_scope_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    asset_type: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframes: tuple[str, ...],
    limit: int,
) -> list[dict[str, object]]:
    where_clauses = ["asset_type = ?"]
    parameters: list[object] = [asset_type]
    if signal_start_date is not None:
        where_clauses.append("asof_bar_dt >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append("asof_bar_dt <= ?")
        parameters.append(signal_end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"code IN ({placeholders})")
        parameters.extend(instruments)
    if timeframes:
        placeholders = ", ".join("?" for _ in timeframes)
        where_clauses.append(f"timeframe IN ({placeholders})")
        parameters.extend(timeframes)
    rows = connection.execute(
        f"""
        SELECT asset_type, code, timeframe, MIN(asof_bar_dt), MAX(asof_bar_dt)
        FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
        GROUP BY asset_type, code, timeframe
        ORDER BY code, timeframe
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        {
            "scope_nk": _build_scope_nk(asset_type=str(row[0]), code=str(row[1]), timeframe=str(row[2])),
            "asset_type": str(row[0]),
            "code": str(row[1]),
            "timeframe": str(row[2]),
            "replay_start_bar_dt": signal_start_date or _to_python_date(row[3]),
            "replay_confirm_until_dt": signal_end_date or _to_python_date(row[4]),
        }
        for row in rows
    ]


def _load_wave_life_dirty_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    timeframes: tuple[str, ...],
    limit: int,
) -> list[dict[str, object]]:
    placeholders = ", ".join("?" for _ in timeframes)
    rows = connection.execute(
        f"""
        SELECT asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, last_wave_id, last_run_id
        FROM {MALF_CANONICAL_CHECKPOINT_TABLE}
        WHERE asset_type = ?
          AND timeframe IN ({placeholders})
        ORDER BY code, timeframe
        LIMIT ?
        """,
        [asset_type, *timeframes, limit],
    ).fetchall()
    scope_rows: list[dict[str, object]] = []
    for row in rows:
        replay_end = _to_python_date(row[5]) or _to_python_date(row[3])
        if replay_end is None:
            continue
        scope_rows.append(
            {
                "scope_nk": _build_scope_nk(asset_type=str(row[0]), code=str(row[1]), timeframe=str(row[2])),
                "asset_type": str(row[0]),
                "code": str(row[1]),
                "timeframe": str(row[2]),
                "replay_start_bar_dt": _to_python_date(row[4]) or replay_end,
                "replay_confirm_until_dt": replay_end,
                "source_fingerprint": _build_wave_life_source_fingerprint(row),
            }
        )
    return scope_rows


def _build_wave_life_source_fingerprint(checkpoint_row: tuple[object, ...]) -> str:
    payload = {
        "asset_type": str(checkpoint_row[0]),
        "code": str(checkpoint_row[1]),
        "timeframe": str(checkpoint_row[2]),
        "last_completed_bar_dt": None if _to_python_date(checkpoint_row[3]) is None else _to_python_date(checkpoint_row[3]).isoformat(),
        "tail_start_bar_dt": None if _to_python_date(checkpoint_row[4]) is None else _to_python_date(checkpoint_row[4]).isoformat(),
        "tail_confirm_until_dt": None if _to_python_date(checkpoint_row[5]) is None else _to_python_date(checkpoint_row[5]).isoformat(),
        "last_wave_id": 0 if checkpoint_row[6] is None else int(checkpoint_row[6]),
        "last_run_id": None if checkpoint_row[7] is None else str(checkpoint_row[7]),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _enqueue_wave_life_dirty_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    scope_rows: list[dict[str, object]],
    run_id: str,
    sample_version: str,
) -> dict[str, int]:
    queue_enqueued_count = 0
    for scope_row in scope_rows:
        checkpoint_row = connection.execute(
            f"""
            SELECT last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, last_sample_version, source_fingerprint
            FROM {MALF_WAVE_LIFE_CHECKPOINT_TABLE}
            WHERE asset_type = ?
              AND code = ?
              AND timeframe = ?
            """,
            [scope_row["asset_type"], scope_row["code"], scope_row["timeframe"]],
        ).fetchone()
        dirty_reason = _derive_wave_life_dirty_reason(
            checkpoint_row=checkpoint_row,
            replay_start_bar_dt=_to_python_date(scope_row["replay_start_bar_dt"]),
            replay_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
            sample_version=sample_version,
            source_fingerprint=str(scope_row["source_fingerprint"]),
        )
        if dirty_reason is None:
            continue
        queue_nk = _build_queue_nk(scope_nk=str(scope_row["scope_nk"]), dirty_reason=dirty_reason)
        existing = connection.execute(
            f"SELECT queue_nk FROM {MALF_WAVE_LIFE_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
            [queue_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {MALF_WAVE_LIFE_WORK_QUEUE_TABLE} (
                    queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
                    replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint,
                    sample_version, queue_status, first_seen_run_id, last_materialized_run_id, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    queue_nk,
                    scope_row["scope_nk"],
                    scope_row["asset_type"],
                    scope_row["code"],
                    scope_row["timeframe"],
                    dirty_reason,
                    _to_python_date(scope_row["replay_start_bar_dt"]),
                    _to_python_date(scope_row["replay_confirm_until_dt"]),
                    scope_row["source_fingerprint"],
                    sample_version,
                    run_id,
                    run_id,
                ],
            )
            queue_enqueued_count += 1
            continue
        connection.execute(
            f"""
            UPDATE {MALF_WAVE_LIFE_WORK_QUEUE_TABLE}
            SET replay_start_bar_dt = ?,
                replay_confirm_until_dt = ?,
                source_fingerprint = ?,
                sample_version = ?,
                queue_status = 'pending',
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [
                _to_python_date(scope_row["replay_start_bar_dt"]),
                _to_python_date(scope_row["replay_confirm_until_dt"]),
                scope_row["source_fingerprint"],
                sample_version,
                queue_nk,
            ],
        )
    return {"queue_enqueued_count": queue_enqueued_count}


def _derive_wave_life_dirty_reason(
    *,
    checkpoint_row: tuple[object, ...] | None,
    replay_start_bar_dt: date | None,
    replay_confirm_until_dt: date | None,
    sample_version: str,
    source_fingerprint: str,
) -> str | None:
    if checkpoint_row is None:
        return "bootstrap_missing_checkpoint"
    last_completed_bar_dt = _to_python_date(checkpoint_row[0])
    tail_start_bar_dt = _to_python_date(checkpoint_row[1])
    tail_confirm_until_dt = _to_python_date(checkpoint_row[2])
    last_sample_version = "" if checkpoint_row[3] is None else str(checkpoint_row[3])
    checkpoint_fingerprint = "" if checkpoint_row[4] is None else str(checkpoint_row[4])
    if last_sample_version != sample_version:
        return "sample_version_changed"
    if checkpoint_fingerprint != source_fingerprint:
        return "source_fingerprint_changed"
    if last_completed_bar_dt is None or (replay_confirm_until_dt is not None and replay_confirm_until_dt > last_completed_bar_dt):
        return "source_advanced"
    if tail_start_bar_dt is None or (replay_start_bar_dt is not None and replay_start_bar_dt < tail_start_bar_dt):
        return "source_replayed"
    if tail_confirm_until_dt is None or (replay_confirm_until_dt is not None and replay_confirm_until_dt > tail_confirm_until_dt):
        return "tail_confirm_advanced"
    return None


def _claim_wave_life_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    timeframes: tuple[str, ...],
    run_id: str,
) -> list[dict[str, object]]:
    placeholders = ", ".join("?" for _ in timeframes)
    rows = connection.execute(
        f"""
        SELECT queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
               replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint
        FROM {MALF_WAVE_LIFE_WORK_QUEUE_TABLE}
        WHERE asset_type = ?
          AND timeframe IN ({placeholders})
          AND queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY code, timeframe, enqueued_at
        """,
        [asset_type, *timeframes],
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {MALF_WAVE_LIFE_WORK_QUEUE_TABLE}
            SET queue_status = 'claimed',
                claimed_at = CURRENT_TIMESTAMP,
                last_claimed_run_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [run_id, str(row[0])],
        )
        claimed_rows.append(
            {
                "queue_nk": str(row[0]),
                "scope_nk": str(row[1]),
                "asset_type": str(row[2]),
                "code": str(row[3]),
                "timeframe": str(row[4]),
                "dirty_reason": str(row[5]),
                "replay_start_bar_dt": _to_python_date(row[6]),
                "replay_confirm_until_dt": _to_python_date(row[7]),
                "source_fingerprint": str(row[8]),
            }
        )
    return claimed_rows


def _load_completed_wave_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    timeframes: tuple[str, ...],
    signal_end_date: date | None,
) -> list[_WaveRow]:
    where_clauses = ["active_flag = FALSE"]
    parameters: list[object] = []
    if timeframes:
        placeholders = ", ".join("?" for _ in timeframes)
        where_clauses.append(f"timeframe IN ({placeholders})")
        parameters.extend(timeframes)
    if signal_end_date is not None:
        where_clauses.append("end_bar_dt <= ?")
        parameters.append(signal_end_date)
    rows = connection.execute(
        f"""
        SELECT wave_nk, asset_type, code, timeframe, wave_id, major_state, reversal_stage,
               start_bar_dt, end_bar_dt, active_flag, bar_count
        FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY timeframe, major_state, reversal_stage, end_bar_dt, wave_id
        """,
        parameters,
    ).fetchall()
    return [_wave_row_from_tuple(row) for row in rows]


def _load_stats_fallback_map(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    timeframes: tuple[str, ...],
) -> dict[tuple[str, str], dict[str, object]]:
    placeholders = ", ".join("?" for _ in timeframes)
    rows = connection.execute(
        f"""
        SELECT stats_nk, timeframe, major_state, sample_size, p10, p25, p50, p75, p90, mean, std
        FROM {table_name}
        WHERE metric_name = ?
          AND timeframe IN ({placeholders})
        ORDER BY timeframe, major_state, updated_at DESC
        """,
        [DEFAULT_WAVE_LIFE_METRIC_NAME, *timeframes],
    ).fetchall()
    fallback_map: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        key = (str(row[1]), str(row[2]))
        if key in fallback_map:
            continue
        fallback_map[key] = {
            "stats_nk": str(row[0]),
            "sample_size": 0 if row[3] is None else int(row[3]),
            "p10": _coerce_optional_float(row[4]),
            "p25": _coerce_optional_float(row[5]),
            "p50": _coerce_optional_float(row[6]),
            "p75": _coerce_optional_float(row[7]),
            "p90": _coerce_optional_float(row[8]),
            "mean": _coerce_optional_float(row[9]),
            "std": _coerce_optional_float(row[10]),
        }
    return fallback_map


def _build_completed_wave_profiles(
    *,
    completed_wave_rows: list[_WaveRow],
    sample_version: str,
    run_id: str,
) -> tuple[list[_ProfileMaterialization], dict[tuple[str, str, str], _ProfileMaterialization]]:
    grouped: dict[tuple[str, str, str], list[float]] = {}
    for row in completed_wave_rows:
        grouped.setdefault((row.timeframe, row.major_state, row.reversal_stage), []).append(float(max(row.bar_count, 0)))
    profile_rows: list[_ProfileMaterialization] = []
    profile_map: dict[tuple[str, str, str], _ProfileMaterialization] = {}
    for key, values in sorted(grouped.items()):
        series = pd.Series(values, dtype="float64")
        profile = _ProfileMaterialization(
            profile_nk=_build_profile_nk(
                timeframe=key[0],
                major_state=key[1],
                reversal_stage=key[2],
                metric_name=DEFAULT_WAVE_LIFE_METRIC_NAME,
                sample_version=sample_version,
            ),
            timeframe=key[0],
            major_state=key[1],
            reversal_stage=key[2],
            metric_name=DEFAULT_WAVE_LIFE_METRIC_NAME,
            sample_version=sample_version,
            sample_size=int(series.shape[0]),
            profile_origin="completed_wave_sample",
            p10=_series_quantile(series, 0.10),
            p25=_series_quantile(series, 0.25),
            p50=_series_quantile(series, 0.50),
            p75=_series_quantile(series, 0.75),
            p90=_series_quantile(series, 0.90),
            mean=float(series.mean()),
            std=float(series.std(ddof=0)),
            source_stats_nk=None,
            first_seen_run_id=run_id,
            last_materialized_run_id=run_id,
            sample_values=tuple(float(value) for value in values),
        )
        profile_rows.append(profile)
        profile_map[key] = profile
    return profile_rows, profile_map


def _ensure_profiles_for_state_rows(
    *,
    profile_rows: list[_ProfileMaterialization],
    profile_map: dict[tuple[str, str, str], _ProfileMaterialization],
    stats_fallback_map: dict[tuple[str, str], dict[str, object]],
    state_rows: list[_StateRow],
    sample_version: str,
    run_id: str,
) -> tuple[list[_ProfileMaterialization], dict[tuple[str, str, str], _ProfileMaterialization]]:
    for row in state_rows:
        key = (row.timeframe, row.major_state, row.reversal_stage)
        if key in profile_map:
            continue
        stats_row = stats_fallback_map.get((row.timeframe, row.major_state))
        profile = _ProfileMaterialization(
            profile_nk=_build_profile_nk(
                timeframe=row.timeframe,
                major_state=row.major_state,
                reversal_stage=row.reversal_stage,
                metric_name=DEFAULT_WAVE_LIFE_METRIC_NAME,
                sample_version=sample_version,
            ),
            timeframe=row.timeframe,
            major_state=row.major_state,
            reversal_stage=row.reversal_stage,
            metric_name=DEFAULT_WAVE_LIFE_METRIC_NAME,
            sample_version=sample_version,
            sample_size=0 if stats_row is None else int(stats_row["sample_size"]),
            profile_origin="missing_profile" if stats_row is None else "same_level_stats_fallback",
            p10=None if stats_row is None else _coerce_optional_float(stats_row["p10"]),
            p25=None if stats_row is None else _coerce_optional_float(stats_row["p25"]),
            p50=None if stats_row is None else _coerce_optional_float(stats_row["p50"]),
            p75=None if stats_row is None else _coerce_optional_float(stats_row["p75"]),
            p90=None if stats_row is None else _coerce_optional_float(stats_row["p90"]),
            mean=None if stats_row is None else _coerce_optional_float(stats_row["mean"]),
            std=None if stats_row is None else _coerce_optional_float(stats_row["std"]),
            source_stats_nk=None if stats_row is None else str(stats_row["stats_nk"]),
            first_seen_run_id=run_id,
            last_materialized_run_id=run_id,
            sample_values=(),
        )
        profile_rows.append(profile)
        profile_map[key] = profile
    return profile_rows, profile_map


def _load_scope_state_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    asset_type: str,
    code: str,
    timeframe: str,
    replay_confirm_until_dt: date | None,
) -> list[_StateRow]:
    parameters: list[object] = [asset_type, code, timeframe]
    where_clause = "asset_type = ? AND code = ? AND timeframe = ?"
    if replay_confirm_until_dt is not None:
        where_clause += " AND asof_bar_dt <= ?"
        parameters.append(replay_confirm_until_dt)
    rows = connection.execute(
        f"""
        SELECT snapshot_nk, asset_type, code, timeframe, asof_bar_dt, major_state, reversal_stage, wave_id
        FROM {table_name}
        WHERE {where_clause}
        ORDER BY asof_bar_dt
        """,
        parameters,
    ).fetchall()
    return [_state_row_from_tuple(row) for row in rows]


def _load_scope_wave_map(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    asset_type: str,
    code: str,
    timeframe: str,
) -> dict[int, _WaveRow]:
    rows = connection.execute(
        f"""
        SELECT wave_nk, asset_type, code, timeframe, wave_id, major_state, reversal_stage,
               start_bar_dt, end_bar_dt, active_flag, bar_count
        FROM {table_name}
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        ORDER BY wave_id
        """,
        [asset_type, code, timeframe],
    ).fetchall()
    return {wave.wave_id: wave for wave in (_wave_row_from_tuple(row) for row in rows)}
