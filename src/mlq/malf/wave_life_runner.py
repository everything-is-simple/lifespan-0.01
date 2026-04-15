"""`malf wave life` runner 的正式入口。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.malf.bootstrap import bootstrap_malf_ledger, malf_ledger_path
from mlq.malf.wave_life_materialization import (
    _build_scope_snapshot_rows,
    _insert_run_row,
    _mark_run_completed,
    _mark_run_failed,
    _mark_wave_life_queue_completed,
    _mark_wave_life_queue_failed,
    _replace_profile_rows,
    _replace_scope_snapshot_rows,
    _upsert_wave_life_checkpoint,
)
from mlq.malf.wave_life_shared import (
    DEFAULT_WAVE_LIFE_CONTRACT_VERSION,
    DEFAULT_WAVE_LIFE_SAMPLE_VERSION,
    DEFAULT_WAVE_LIFE_SOURCE_STATE_TABLE,
    DEFAULT_WAVE_LIFE_SOURCE_STATS_TABLE,
    DEFAULT_WAVE_LIFE_SOURCE_WAVE_TABLE,
    MalfWaveLifeBuildSummary,
    _build_run_id,
    _coerce_date,
    _normalize_instruments,
    _normalize_timeframes,
    _should_use_queue_execution,
    _to_python_date,
    _write_summary,
)
from mlq.malf.wave_life_source import (
    _build_completed_wave_profiles,
    _claim_wave_life_scopes,
    _enqueue_wave_life_dirty_scopes,
    _ensure_profiles_for_state_rows,
    _load_bounded_scope_rows,
    _load_completed_wave_rows,
    _load_scope_state_rows,
    _load_scope_wave_map,
    _load_stats_fallback_map,
    _load_wave_life_dirty_scopes,
)


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
    require_explicit_queue_mode: bool = False,
) -> MalfWaveLifeBuildSummary:
    """物化 canonical `malf` 的 wave life probability sidecar。"""

    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    normalized_timeframes = _normalize_timeframes(timeframes)
    queue_execution = _should_use_queue_execution(
        use_checkpoint_queue=use_checkpoint_queue,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
    )
    _validate_wave_life_execution_mode(
        use_checkpoint_queue=use_checkpoint_queue,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        queue_execution=queue_execution,
        require_explicit_queue_mode=require_explicit_queue_mode,
    )
    if queue_execution:
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


def _validate_wave_life_execution_mode(
    *,
    use_checkpoint_queue: bool | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    queue_execution: bool,
    require_explicit_queue_mode: bool,
) -> None:
    if not require_explicit_queue_mode or not queue_execution or use_checkpoint_queue is not None:
        return
    if signal_start_date is None and signal_end_date is None and not instruments:
        raise ValueError(
            "malf wave life official script requires an explicit bounded window for initial bootstrap; "
            "pass `signal_start_date/signal_end_date` for bounded materialization or "
            "set `use_checkpoint_queue=True` for incremental checkpoint queue execution."
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
