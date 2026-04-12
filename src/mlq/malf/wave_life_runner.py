"""构建 `malf` 波段寿命概率 sidecar 的正式 runner。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb
import pandas as pd

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.malf.bootstrap import (
    MALF_CANONICAL_CHECKPOINT_TABLE,
    MALF_SAME_LEVEL_STATS_TABLE,
    MALF_STATE_SNAPSHOT_TABLE,
    MALF_WAVE_LEDGER_TABLE,
    MALF_WAVE_LIFE_CHECKPOINT_TABLE,
    MALF_WAVE_LIFE_PROFILE_TABLE,
    MALF_WAVE_LIFE_RUN_TABLE,
    MALF_WAVE_LIFE_SNAPSHOT_TABLE,
    MALF_WAVE_LIFE_WORK_QUEUE_TABLE,
    bootstrap_malf_ledger,
    malf_ledger_path,
)


DEFAULT_WAVE_LIFE_CONTRACT_VERSION: Final[str] = "malf-wave-life-v1"
DEFAULT_WAVE_LIFE_SAMPLE_VERSION: Final[str] = "wave-life-v1"
DEFAULT_WAVE_LIFE_METRIC_NAME: Final[str] = "wave_duration_bars"
DEFAULT_WAVE_LIFE_SOURCE_WAVE_TABLE: Final[str] = MALF_WAVE_LEDGER_TABLE
DEFAULT_WAVE_LIFE_SOURCE_STATE_TABLE: Final[str] = MALF_STATE_SNAPSHOT_TABLE
DEFAULT_WAVE_LIFE_SOURCE_STATS_TABLE: Final[str] = MALF_SAME_LEVEL_STATS_TABLE
DEFAULT_TIMEFRAMES: Final[tuple[str, ...]] = ("D", "W", "M")
SUPPORTED_TIMEFRAMES: Final[tuple[str, ...]] = ("D", "W", "M")


@dataclass(frozen=True)
class MalfWaveLifeBuildSummary:
    """总结一次波段寿命 sidecar 物化的正式结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    execution_mode: str
    life_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_scope_count: int
    claimed_scope_count: int
    profile_row_count: int
    snapshot_row_count: int
    profile_inserted_count: int
    profile_reused_count: int
    profile_rematerialized_count: int
    snapshot_inserted_count: int
    snapshot_reused_count: int
    snapshot_rematerialized_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    active_snapshot_count: int
    completed_wave_sample_count: int
    fallback_profile_count: int
    malf_ledger_path: str
    source_wave_table: str
    source_state_table: str
    source_stats_table: str
    sample_version: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _WaveRow:
    wave_nk: str
    asset_type: str
    code: str
    timeframe: str
    wave_id: int
    major_state: str
    reversal_stage: str
    start_bar_dt: date
    end_bar_dt: date | None
    active_flag: bool
    bar_count: int


@dataclass(frozen=True)
class _StateRow:
    snapshot_nk: str
    asset_type: str
    code: str
    timeframe: str
    asof_bar_dt: date
    major_state: str
    reversal_stage: str
    wave_id: int


@dataclass(frozen=True)
class _ProfileMaterialization:
    profile_nk: str
    timeframe: str
    major_state: str
    reversal_stage: str
    metric_name: str
    sample_version: str
    sample_size: int
    profile_origin: str
    p10: float | None
    p25: float | None
    p50: float | None
    p75: float | None
    p90: float | None
    mean: float | None
    std: float | None
    source_stats_nk: str | None
    first_seen_run_id: str
    last_materialized_run_id: str
    sample_values: tuple[float, ...]

    def as_row(self) -> dict[str, object]:
        return {
            "profile_nk": self.profile_nk,
            "timeframe": self.timeframe,
            "major_state": self.major_state,
            "reversal_stage": self.reversal_stage,
            "metric_name": self.metric_name,
            "sample_version": self.sample_version,
            "sample_size": self.sample_size,
            "profile_origin": self.profile_origin,
            "p10": self.p10,
            "p25": self.p25,
            "p50": self.p50,
            "p75": self.p75,
            "p90": self.p90,
            "mean": self.mean,
            "std": self.std,
            "source_stats_nk": self.source_stats_nk,
            "first_seen_run_id": self.first_seen_run_id,
            "last_materialized_run_id": self.last_materialized_run_id,
        }


def run_malf_wave_life_build(
    *,
    settings: WorkspaceRoots | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    asset_type: str = "stock",
    timeframes: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    sample_version: str = DEFAULT_WAVE_LIFE_SAMPLE_VERSION,
    life_contract_version: str = DEFAULT_WAVE_LIFE_CONTRACT_VERSION,
    source_wave_table: str = DEFAULT_WAVE_LIFE_SOURCE_WAVE_TABLE,
    source_state_table: str = DEFAULT_WAVE_LIFE_SOURCE_STATE_TABLE,
    source_stats_table: str = DEFAULT_WAVE_LIFE_SOURCE_STATS_TABLE,
    run_id: str | None = None,
    runner_name: str = "malf_wave_life_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
    use_checkpoint_queue: bool | None = None,
) -> MalfWaveLifeBuildSummary:
    """运行 `malf` 波段寿命概率 sidecar。"""

    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    normalized_timeframes = _normalize_timeframes(timeframes)
    if _should_use_queue_execution(
        use_checkpoint_queue=use_checkpoint_queue,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
    ):
        return _run_queue_build(
            settings=settings,
            malf_path=malf_path,
            asset_type=asset_type,
            timeframes=normalized_timeframes,
            limit=limit,
            sample_version=sample_version,
            life_contract_version=life_contract_version,
            source_wave_table=source_wave_table,
            source_state_table=source_state_table,
            source_stats_table=source_stats_table,
            run_id=run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            summary_path=summary_path,
        )
    return _run_bounded_build(
        settings=settings,
        malf_path=malf_path,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        asset_type=asset_type,
        timeframes=normalized_timeframes,
        limit=limit,
        sample_version=sample_version,
        life_contract_version=life_contract_version,
        source_wave_table=source_wave_table,
        source_state_table=source_state_table,
        source_stats_table=source_stats_table,
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        summary_path=summary_path,
    )


def _run_bounded_build(
    *,
    settings: WorkspaceRoots | None,
    malf_path: Path | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    asset_type: str,
    timeframes: tuple[str, ...],
    limit: int,
    sample_version: str,
    life_contract_version: str,
    source_wave_table: str,
    source_state_table: str,
    source_stats_table: str,
    run_id: str | None,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> MalfWaveLifeBuildSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    resolved_malf_path = Path(malf_path or malf_ledger_path(workspace))
    materialization_run_id = run_id or _build_run_id(prefix="malf-wave-life")
    normalized_limit = max(int(limit), 1)

    if not resolved_malf_path.exists():
        raise FileNotFoundError(f"Missing malf database: {resolved_malf_path}")

    connection = duckdb.connect(str(resolved_malf_path))
    try:
        bootstrap_malf_ledger(workspace, connection=connection)
        scope_rows = _load_bounded_scope_rows(
            connection,
            table_name=source_state_table,
            asset_type=asset_type,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=instruments,
            timeframes=timeframes,
            limit=normalized_limit,
        )
        _insert_run_row(
            connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode="bounded_window",
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            bounded_scope_count=len(scope_rows),
            claimed_scope_count=len(scope_rows),
            source_wave_table=source_wave_table,
            source_state_table=source_state_table,
            source_stats_table=source_stats_table,
            sample_version=sample_version,
            life_contract_version=life_contract_version,
        )
        summary = _materialize_wave_life(
            connection=connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode="bounded_window",
            scope_rows=scope_rows,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            timeframes=timeframes,
            sample_version=sample_version,
            life_contract_version=life_contract_version,
            source_wave_table=source_wave_table,
            source_state_table=source_state_table,
            source_stats_table=source_stats_table,
            resolved_malf_path=resolved_malf_path,
            queue_enqueued_count=0,
            queue_claimed_count=0,
            checkpoint_upserted_count=0,
        )
        _mark_run_completed(connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception:
        _mark_run_failed(connection, run_id=materialization_run_id)
        raise
    finally:
        connection.close()


def _run_queue_build(
    *,
    settings: WorkspaceRoots | None,
    malf_path: Path | None,
    asset_type: str,
    timeframes: tuple[str, ...],
    limit: int,
    sample_version: str,
    life_contract_version: str,
    source_wave_table: str,
    source_state_table: str,
    source_stats_table: str,
    run_id: str | None,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> MalfWaveLifeBuildSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    resolved_malf_path = Path(malf_path or malf_ledger_path(workspace))
    materialization_run_id = run_id or _build_run_id(prefix="malf-wave-life")
    normalized_limit = max(int(limit), 1)

    if not resolved_malf_path.exists():
        raise FileNotFoundError(f"Missing malf database: {resolved_malf_path}")

    connection = duckdb.connect(str(resolved_malf_path))
    claimed_scope_rows: list[dict[str, object]] = []
    try:
        bootstrap_malf_ledger(workspace, connection=connection)
        dirty_scopes = _load_wave_life_dirty_scopes(
            connection,
            asset_type=asset_type,
            timeframes=timeframes,
            limit=normalized_limit,
        )
        enqueue_counts = _enqueue_wave_life_dirty_scopes(
            connection,
            scope_rows=dirty_scopes,
            run_id=materialization_run_id,
            sample_version=sample_version,
        )
        claimed_scope_rows = _claim_wave_life_scopes(
            connection,
            asset_type=asset_type,
            timeframes=timeframes,
            run_id=materialization_run_id,
        )
        _insert_run_row(
            connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode="checkpoint_queue",
            signal_start_date=None,
            signal_end_date=None,
            bounded_scope_count=len(dirty_scopes),
            claimed_scope_count=len(claimed_scope_rows),
            source_wave_table=source_wave_table,
            source_state_table=source_state_table,
            source_stats_table=source_stats_table,
            sample_version=sample_version,
            life_contract_version=life_contract_version,
        )
        summary = _materialize_wave_life(
            connection=connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode="checkpoint_queue",
            scope_rows=claimed_scope_rows,
            signal_start_date=None,
            signal_end_date=None,
            timeframes=timeframes,
            sample_version=sample_version,
            life_contract_version=life_contract_version,
            source_wave_table=source_wave_table,
            source_state_table=source_state_table,
            source_stats_table=source_stats_table,
            resolved_malf_path=resolved_malf_path,
            queue_enqueued_count=enqueue_counts["queue_enqueued_count"],
            queue_claimed_count=len(claimed_scope_rows),
            checkpoint_upserted_count=0,
        )
        checkpoint_upserted_count = 0
        for scope_row in claimed_scope_rows:
            _upsert_wave_life_checkpoint(
                connection,
                asset_type=str(scope_row["asset_type"]),
                code=str(scope_row["code"]),
                timeframe=str(scope_row["timeframe"]),
                last_completed_bar_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
                tail_start_bar_dt=_to_python_date(scope_row["replay_start_bar_dt"]),
                tail_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
                last_sample_version=sample_version,
                source_fingerprint=str(scope_row["source_fingerprint"]),
                last_run_id=materialization_run_id,
            )
            checkpoint_upserted_count += 1
            _mark_wave_life_queue_completed(
                connection,
                queue_nk=str(scope_row["queue_nk"]),
                run_id=materialization_run_id,
            )
        final_summary = MalfWaveLifeBuildSummary(
            **{
                **summary.as_dict(),
                "checkpoint_upserted_count": checkpoint_upserted_count,
            }
        )
        _mark_run_completed(connection, run_id=materialization_run_id, summary=final_summary)
        _write_summary(final_summary.as_dict(), summary_path)
        return final_summary
    except Exception:
        for scope_row in claimed_scope_rows:
            _mark_wave_life_queue_failed(
                connection,
                queue_nk=str(scope_row["queue_nk"]),
                run_id=materialization_run_id,
            )
        _mark_run_failed(connection, run_id=materialization_run_id)
        raise
    finally:
        connection.close()


def _materialize_wave_life(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    runner_name: str,
    runner_version: str,
    execution_mode: str,
    scope_rows: list[dict[str, object]],
    signal_start_date: date | None,
    signal_end_date: date | None,
    timeframes: tuple[str, ...],
    sample_version: str,
    life_contract_version: str,
    source_wave_table: str,
    source_state_table: str,
    source_stats_table: str,
    resolved_malf_path: Path,
    queue_enqueued_count: int,
    queue_claimed_count: int,
    checkpoint_upserted_count: int,
) -> MalfWaveLifeBuildSummary:
    completed_wave_rows = _load_completed_wave_rows(
        connection,
        table_name=source_wave_table,
        timeframes=timeframes,
        signal_end_date=signal_end_date,
    )
    stats_fallback_map = _load_stats_fallback_map(
        connection,
        table_name=source_stats_table,
        timeframes=timeframes,
    )
    profile_rows, profile_map = _build_completed_wave_profiles(
        completed_wave_rows=completed_wave_rows,
        sample_version=sample_version,
        run_id=run_id,
    )

    snapshot_row_count = 0
    snapshot_inserted_count = 0
    snapshot_reused_count = 0
    snapshot_rematerialized_count = 0
    active_snapshot_count = 0
    for scope_row in scope_rows:
        state_rows = _load_scope_state_rows(
            connection,
            table_name=source_state_table,
            asset_type=str(scope_row["asset_type"]),
            code=str(scope_row["code"]),
            timeframe=str(scope_row["timeframe"]),
            replay_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
        )
        if not state_rows:
            continue
        profile_rows, profile_map = _ensure_profiles_for_state_rows(
            profile_rows=profile_rows,
            profile_map=profile_map,
            stats_fallback_map=stats_fallback_map,
            state_rows=state_rows,
            sample_version=sample_version,
            run_id=run_id,
        )
        wave_map = _load_scope_wave_map(
            connection,
            table_name=source_wave_table,
            asset_type=str(scope_row["asset_type"]),
            code=str(scope_row["code"]),
            timeframe=str(scope_row["timeframe"]),
        )
        snapshot_rows = _build_scope_snapshot_rows(
            asset_type=str(scope_row["asset_type"]),
            code=str(scope_row["code"]),
            timeframe=str(scope_row["timeframe"]),
            state_rows=state_rows,
            wave_map=wave_map,
            profile_map=profile_map,
            replay_start_bar_dt=_to_python_date(scope_row["replay_start_bar_dt"]),
            replay_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
            sample_version=sample_version,
            run_id=run_id,
        )
        replace_counts = _replace_scope_snapshot_rows(
            connection,
            asset_type=str(scope_row["asset_type"]),
            code=str(scope_row["code"]),
            timeframe=str(scope_row["timeframe"]),
            replay_start_bar_dt=_to_python_date(scope_row["replay_start_bar_dt"]),
            replay_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
            rows=snapshot_rows,
        )
        snapshot_row_count += len(snapshot_rows)
        snapshot_inserted_count += replace_counts["inserted_count"]
        snapshot_reused_count += replace_counts["reused_count"]
        snapshot_rematerialized_count += replace_counts["rematerialized_count"]
        active_snapshot_count += len(snapshot_rows)

    profile_counts = _replace_profile_rows(
        connection,
        timeframes=timeframes,
        sample_version=sample_version,
        rows=[row.as_row() for row in profile_rows],
    )
    fallback_profile_count = sum(1 for row in profile_rows if row.profile_origin != "completed_wave_sample")
    return MalfWaveLifeBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        execution_mode=execution_mode,
        life_contract_version=life_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_scope_count=len(scope_rows),
        claimed_scope_count=len(scope_rows),
        profile_row_count=len(profile_rows),
        snapshot_row_count=snapshot_row_count,
        profile_inserted_count=profile_counts["inserted_count"],
        profile_reused_count=profile_counts["reused_count"],
        profile_rematerialized_count=profile_counts["rematerialized_count"],
        snapshot_inserted_count=snapshot_inserted_count,
        snapshot_reused_count=snapshot_reused_count,
        snapshot_rematerialized_count=snapshot_rematerialized_count,
        queue_enqueued_count=queue_enqueued_count,
        queue_claimed_count=queue_claimed_count,
        checkpoint_upserted_count=checkpoint_upserted_count,
        active_snapshot_count=active_snapshot_count,
        completed_wave_sample_count=len(completed_wave_rows),
        fallback_profile_count=fallback_profile_count,
        malf_ledger_path=str(resolved_malf_path),
        source_wave_table=source_wave_table,
        source_state_table=source_state_table,
        source_stats_table=source_stats_table,
        sample_version=sample_version,
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


def _estimate_wave_life_percentile(profile: _ProfileMaterialization | None, active_wave_bar_age: int) -> float | None:
    if profile is None:
        return None
    if profile.sample_values:
        values = sorted(profile.sample_values)
        less_or_equal = sum(1 for value in values if value <= float(active_wave_bar_age))
        return float(less_or_equal) / float(len(values))
    thresholds = (
        (profile.p10, 0.10),
        (profile.p25, 0.25),
        (profile.p50, 0.50),
        (profile.p75, 0.75),
        (profile.p90, 0.90),
    )
    for threshold, percentile in thresholds:
        if threshold is not None and float(active_wave_bar_age) <= float(threshold):
            return float(percentile)
    return None if profile.p90 is None else 1.0


def _estimate_remaining_life(target_life: float | None, active_wave_bar_age: int) -> float | None:
    if target_life is None:
        return None
    return float(max(float(target_life) - float(active_wave_bar_age), 0.0))


def _derive_termination_risk_bucket(percentile: float | None) -> str | None:
    if percentile is None:
        return None
    if percentile >= 0.90:
        return "high"
    if percentile >= 0.75:
        return "elevated"
    return "normal"


def _resolve_wave_nk(
    *,
    wave_row: _WaveRow | None,
    asset_type: str,
    code: str,
    timeframe: str,
    wave_id: int,
) -> str:
    if wave_row is not None:
        return wave_row.wave_nk
    return f"{asset_type}|{code}|{timeframe}|wave|{wave_id}"


def _wave_row_from_tuple(row: tuple[object, ...]) -> _WaveRow:
    return _WaveRow(
        wave_nk=str(row[0]),
        asset_type=str(row[1]),
        code=str(row[2]),
        timeframe=str(row[3]),
        wave_id=int(row[4]),
        major_state=str(row[5]),
        reversal_stage=str(row[6]),
        start_bar_dt=_normalize_date_value(row[7], field_name="start_bar_dt"),
        end_bar_dt=_to_python_date(row[8]),
        active_flag=bool(row[9]),
        bar_count=0 if row[10] is None else int(row[10]),
    )


def _state_row_from_tuple(row: tuple[object, ...]) -> _StateRow:
    return _StateRow(
        snapshot_nk=str(row[0]),
        asset_type=str(row[1]),
        code=str(row[2]),
        timeframe=str(row[3]),
        asof_bar_dt=_normalize_date_value(row[4], field_name="asof_bar_dt"),
        major_state=str(row[5]),
        reversal_stage=str(row[6]),
        wave_id=int(row[7]),
    )


def _should_use_queue_execution(
    *,
    use_checkpoint_queue: bool | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> bool:
    if use_checkpoint_queue is not None:
        return use_checkpoint_queue
    return signal_start_date is None and signal_end_date is None and not instruments


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


def _normalize_timeframes(timeframes: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if not timeframes:
        return DEFAULT_TIMEFRAMES
    normalized = tuple(dict.fromkeys(str(value).strip().upper() for value in timeframes if str(value).strip()))
    invalid = [value for value in normalized if value not in SUPPORTED_TIMEFRAMES]
    if invalid:
        raise ValueError(f"Unsupported timeframes: {invalid}")
    return normalized


def _normalize_date_value(value: object, *, field_name: str) -> date:
    normalized = _to_python_date(value)
    if normalized is None:
        raise ValueError(f"Missing required date field: {field_name}")
    return normalized


def _to_python_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _coerce_optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _series_quantile(series: pd.Series, quantile: float) -> float | None:
    if series.empty:
        return None
    return float(series.quantile(quantile))


def _normalize_row_for_compare(row: dict[str, object]) -> dict[str, object]:
    ignored = {"first_seen_run_id", "last_materialized_run_id", "created_at", "updated_at"}
    normalized: dict[str, object] = {}
    for key, value in row.items():
        if key in ignored:
            continue
        if pd.isna(value):
            normalized[key] = None
        elif isinstance(value, (datetime, pd.Timestamp)):
            normalized[key] = value.date().isoformat()
        elif isinstance(value, date):
            normalized[key] = value.isoformat()
        elif isinstance(value, float):
            normalized[key] = round(float(value), 12)
        else:
            normalized[key] = value
    return normalized


def _build_run_id(*, prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}"


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return f"{asset_type}|{code}|{timeframe}"


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return f"{scope_nk}|{dirty_reason}"


def _build_snapshot_nk(*, asset_type: str, code: str, timeframe: str, asof_bar_dt: date) -> str:
    return f"{asset_type}|{code}|{timeframe}|wave-life|{asof_bar_dt.isoformat()}"


def _build_profile_nk(
    *,
    timeframe: str,
    major_state: str,
    reversal_stage: str,
    metric_name: str,
    sample_version: str,
) -> str:
    return "|".join([timeframe, major_state, reversal_stage, metric_name, sample_version])


def _write_summary(payload: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
