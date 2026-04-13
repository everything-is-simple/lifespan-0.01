"""执行 `structure snapshot` 官方 producer 的最小 bounded 运行时。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.structure.bootstrap import bootstrap_structure_snapshot_ledger, structure_ledger_path
from mlq.structure.structure_materialization import (
    _insert_run_row,
    _mark_run_completed,
    _materialize_structure_rows,
    _materialize_structure_scope,
    _update_run_summary,
)
from mlq.structure.structure_shared import (
    DEFAULT_STRUCTURE_BREAK_CONFIRMATION_TABLE,
    DEFAULT_STRUCTURE_CONTEXT_TABLE,
    DEFAULT_STRUCTURE_CONTRACT_VERSION,
    DEFAULT_STRUCTURE_INPUT_TABLE,
    DEFAULT_STRUCTURE_SOURCE_TIMEFRAME,
    DEFAULT_STRUCTURE_STATS_TABLE,
    StructureSnapshotBuildSummary,
    _build_context_series_index,
    _build_structure_run_id,
    _coerce_date,
    _normalize_timeframe,
    _should_use_queue_execution,
    _to_python_date,
    _write_summary,
)
from mlq.structure.structure_source import (
    _claim_structure_scopes,
    _enqueue_structure_dirty_scopes,
    _ensure_database_exists,
    _load_break_confirmation_rows,
    _load_context_rows,
    _load_read_only_context_rows,
    _load_stats_snapshot_rows,
    _load_structure_dirty_scopes,
    _load_structure_input_rows,
    _mark_structure_queue_completed,
    _mark_structure_queue_failed,
    _resolve_optional_sidecar_table,
    _upsert_structure_checkpoint,
)


def run_structure_snapshot_build(
    *,
    settings: WorkspaceRoots | None = None,
    structure_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    run_id: str | None = None,
    source_context_table: str = DEFAULT_STRUCTURE_CONTEXT_TABLE,
    source_structure_input_table: str = DEFAULT_STRUCTURE_INPUT_TABLE,
    source_break_confirmation_table: str | None = DEFAULT_STRUCTURE_BREAK_CONFIRMATION_TABLE,
    source_stats_table: str | None = DEFAULT_STRUCTURE_STATS_TABLE,
    source_timeframe: str = DEFAULT_STRUCTURE_SOURCE_TIMEFRAME,
    structure_contract_version: str = DEFAULT_STRUCTURE_CONTRACT_VERSION,
    runner_name: str = "structure_snapshot_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
    use_checkpoint_queue: bool | None = None,
) -> StructureSnapshotBuildSummary:
    """从官方 `malf` 上游物化 `structure snapshot`。"""

    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_timeframe = _normalize_timeframe(source_timeframe)
    _validate_structure_mainline_contract(
        source_context_table=source_context_table,
        source_structure_input_table=source_structure_input_table,
        source_timeframe=normalized_timeframe,
    )
    if _should_use_queue_execution(
        use_checkpoint_queue=use_checkpoint_queue,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
    ):
        return _run_structure_queue_build(
            settings=settings,
            structure_path=structure_path,
            malf_path=malf_path,
            limit=limit,
            batch_size=batch_size,
            run_id=run_id,
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
            source_break_confirmation_table=source_break_confirmation_table,
            source_stats_table=source_stats_table,
            source_timeframe=normalized_timeframe,
            structure_contract_version=structure_contract_version,
            runner_name=runner_name,
            runner_version=runner_version,
            summary_path=summary_path,
        )
    return _run_structure_bounded_build(
        settings=settings,
        structure_path=structure_path,
        malf_path=malf_path,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        limit=limit,
        batch_size=batch_size,
        run_id=run_id,
        source_context_table=source_context_table,
        source_structure_input_table=source_structure_input_table,
        source_break_confirmation_table=source_break_confirmation_table,
        source_stats_table=source_stats_table,
        source_timeframe=normalized_timeframe,
        structure_contract_version=structure_contract_version,
        runner_name=runner_name,
        runner_version=runner_version,
        summary_path=summary_path,
    )


def _validate_structure_mainline_contract(
    *,
    source_context_table: str,
    source_structure_input_table: str,
    source_timeframe: str,
) -> None:
    if source_context_table != DEFAULT_STRUCTURE_CONTEXT_TABLE:
        raise ValueError(
            "structure mainline only accepts canonical `malf_state_snapshot` as source_context_table."
        )
    if source_structure_input_table != DEFAULT_STRUCTURE_INPUT_TABLE:
        raise ValueError(
            "structure mainline only accepts canonical `malf_state_snapshot` as source_structure_input_table."
        )
    if source_timeframe != DEFAULT_STRUCTURE_SOURCE_TIMEFRAME:
        raise ValueError("structure mainline only accepts canonical timeframe `D`.")


def _run_structure_bounded_build(
    *,
    settings: WorkspaceRoots | None,
    structure_path: Path | None,
    malf_path: Path | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    batch_size: int,
    run_id: str | None,
    source_context_table: str,
    source_structure_input_table: str,
    source_break_confirmation_table: str | None,
    source_stats_table: str | None,
    source_timeframe: str,
    structure_contract_version: str,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> StructureSnapshotBuildSummary:
    """执行显式 bounded window 物化。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_structure_path = Path(structure_path or structure_ledger_path(workspace))
    resolved_malf_path = Path(malf_path or workspace.databases.malf)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_timeframe = _normalize_timeframe(source_timeframe)
    materialization_run_id = run_id or _build_structure_run_id()

    _ensure_database_exists(resolved_malf_path, label="malf")
    actual_break_confirmation_table = _resolve_optional_sidecar_table(
        malf_path=resolved_malf_path,
        requested_table=source_break_confirmation_table,
        fallback_table=None,
    )
    actual_stats_table = _resolve_optional_sidecar_table(
        malf_path=resolved_malf_path,
        requested_table=source_stats_table,
        fallback_table=None,
    )
    input_rows = _load_structure_input_rows(
        malf_path=resolved_malf_path,
        table_name=source_structure_input_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=instruments,
        limit=normalized_limit,
        timeframe=normalized_timeframe,
    )
    materialized_instruments = tuple(sorted({row.instrument for row in input_rows}))
    context_rows = _load_context_rows(
        malf_path=resolved_malf_path,
        table_name=source_context_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=materialized_instruments,
        timeframe=normalized_timeframe,
    )
    daily_context_map = {
        (row.instrument, row.signal_date, row.asof_date): row
        for row in context_rows
    }
    weekly_context_index = _build_context_series_index(
        _load_read_only_context_rows(
            malf_path=resolved_malf_path,
            table_name=source_context_table,
            signal_end_date=signal_end_date,
            instruments=materialized_instruments,
            timeframe="W",
        )
    )
    monthly_context_index = _build_context_series_index(
        _load_read_only_context_rows(
            malf_path=resolved_malf_path,
            table_name=source_context_table,
            signal_end_date=signal_end_date,
            instruments=materialized_instruments,
            timeframe="M",
        )
    )
    break_confirmation_map = _load_break_confirmation_rows(
        malf_path=resolved_malf_path,
        table_name=actual_break_confirmation_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=materialized_instruments,
        timeframe=normalized_timeframe,
    )
    stats_snapshot_map = _load_stats_snapshot_rows(
        malf_path=resolved_malf_path,
        table_name=actual_stats_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=materialized_instruments,
        timeframe=normalized_timeframe,
    )

    structure_connection = duckdb.connect(str(resolved_structure_path))
    try:
        bootstrap_structure_snapshot_ledger(workspace, connection=structure_connection)
        _insert_run_row(
            structure_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            bounded_instrument_count=len({row.instrument for row in input_rows}),
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
            structure_contract_version=structure_contract_version,
        )
        summary = _materialize_structure_rows(
            connection=structure_connection,
            run_id=materialization_run_id,
            input_rows=input_rows,
            daily_context_map=daily_context_map,
            weekly_context_index=weekly_context_index,
            monthly_context_index=monthly_context_index,
            break_confirmation_map=break_confirmation_map,
            stats_snapshot_map=stats_snapshot_map,
            structure_contract_version=structure_contract_version,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            structure_path=resolved_structure_path,
            malf_path=resolved_malf_path,
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
            source_break_confirmation_table=actual_break_confirmation_table,
            source_stats_table=actual_stats_table,
            source_timeframe=normalized_timeframe,
            batch_size=normalized_batch_size,
        )
        _mark_run_completed(structure_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            structure_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        structure_connection.close()


def _run_structure_queue_build(
    *,
    settings: WorkspaceRoots | None,
    structure_path: Path | None,
    malf_path: Path | None,
    limit: int,
    batch_size: int,
    run_id: str | None,
    source_context_table: str,
    source_structure_input_table: str,
    source_break_confirmation_table: str | None,
    source_stats_table: str | None,
    source_timeframe: str,
    structure_contract_version: str,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> StructureSnapshotBuildSummary:
    """执行默认 data-grade queue/checkpoint 续跑。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_structure_path = Path(structure_path or structure_ledger_path(workspace))
    resolved_malf_path = Path(malf_path or workspace.databases.malf)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_timeframe = _normalize_timeframe(source_timeframe)
    materialization_run_id = run_id or _build_structure_run_id()

    _ensure_database_exists(resolved_malf_path, label="malf")
    structure_connection = duckdb.connect(str(resolved_structure_path))
    try:
        bootstrap_structure_snapshot_ledger(workspace, connection=structure_connection)
        dirty_scopes = _load_structure_dirty_scopes(
            malf_path=resolved_malf_path,
            limit=normalized_limit,
            timeframe=normalized_timeframe,
        )
        enqueue_counts = _enqueue_structure_dirty_scopes(
            connection=structure_connection,
            scope_rows=dirty_scopes,
            run_id=materialization_run_id,
        )
        claimed_scope_rows = _claim_structure_scopes(
            connection=structure_connection,
            run_id=materialization_run_id,
            timeframe=normalized_timeframe,
        )
        actual_break_confirmation_table = _resolve_optional_sidecar_table(
            malf_path=resolved_malf_path,
            requested_table=source_break_confirmation_table,
            fallback_table=None,
        )
        actual_stats_table = _resolve_optional_sidecar_table(
            malf_path=resolved_malf_path,
            requested_table=source_stats_table,
            fallback_table=None,
        )
        _insert_run_row(
            structure_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
            structure_contract_version=structure_contract_version,
        )

        summary_counts = {
            "candidate_input_count": 0,
            "materialized_snapshot_count": 0,
            "inserted_count": 0,
            "reused_count": 0,
            "rematerialized_count": 0,
            "missing_context_count": 0,
            "advancing_count": 0,
            "stalled_count": 0,
            "failed_count": 0,
            "unknown_count": 0,
        }
        checkpoint_upserted_count = 0
        for scope_row in claimed_scope_rows:
            try:
                scope_summary = _materialize_structure_scope(
                    connection=structure_connection,
                    run_id=materialization_run_id,
                    malf_path=resolved_malf_path,
                    instrument=str(scope_row["code"]),
                    signal_start_date=_to_python_date(scope_row["replay_start_bar_dt"]),
                    signal_end_date=_to_python_date(scope_row["replay_confirm_until_dt"]),
                    limit=normalized_limit,
                    batch_size=normalized_batch_size,
                    source_context_table=source_context_table,
                    source_structure_input_table=source_structure_input_table,
                    source_break_confirmation_table=actual_break_confirmation_table,
                    source_stats_table=actual_stats_table,
                    source_timeframe=normalized_timeframe,
                    structure_contract_version=structure_contract_version,
                    runner_name=runner_name,
                    runner_version=runner_version,
                    structure_path=resolved_structure_path,
                )
                for key in summary_counts:
                    summary_counts[key] += int(getattr(scope_summary, key))
                _upsert_structure_checkpoint(
                    structure_connection,
                    asset_type=str(scope_row["asset_type"]),
                    code=str(scope_row["code"]),
                    timeframe=str(scope_row["timeframe"]),
                    last_completed_bar_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
                    tail_start_bar_dt=_to_python_date(scope_row["replay_start_bar_dt"]),
                    tail_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
                    source_fingerprint=str(scope_row["source_fingerprint"]),
                    last_run_id=materialization_run_id,
                )
                checkpoint_upserted_count += 1
                _mark_structure_queue_completed(
                    structure_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
            except Exception:
                _mark_structure_queue_failed(
                    structure_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
                raise

        summary = StructureSnapshotBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode="checkpoint_queue",
            structure_contract_version=structure_contract_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            claimed_scope_count=len(claimed_scope_rows),
            candidate_input_count=summary_counts["candidate_input_count"],
            materialized_snapshot_count=summary_counts["materialized_snapshot_count"],
            inserted_count=summary_counts["inserted_count"],
            reused_count=summary_counts["reused_count"],
            rematerialized_count=summary_counts["rematerialized_count"],
            missing_context_count=summary_counts["missing_context_count"],
            advancing_count=summary_counts["advancing_count"],
            stalled_count=summary_counts["stalled_count"],
            failed_count=summary_counts["failed_count"],
            unknown_count=summary_counts["unknown_count"],
            queue_enqueued_count=enqueue_counts["queue_enqueued_count"],
            queue_claimed_count=len(claimed_scope_rows),
            checkpoint_upserted_count=checkpoint_upserted_count,
            structure_ledger_path=str(resolved_structure_path),
            malf_ledger_path=str(resolved_malf_path),
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
            source_break_confirmation_table=actual_break_confirmation_table,
            source_stats_table=actual_stats_table,
            source_timeframe=normalized_timeframe,
        )
        _mark_run_completed(structure_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            structure_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        structure_connection.close()
