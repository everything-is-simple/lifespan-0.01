"""执行 `alpha formal signal` 官方 producer 的最小 bounded 运行时。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.alpha.bootstrap import (
    alpha_ledger_path,
    bootstrap_alpha_formal_signal_ledger,
)
from mlq.alpha.formal_signal_materialization import (
    _insert_run_row,
    _mark_run_completed,
    _materialize_alpha_formal_signal_scope,
    _materialize_formal_signal_rows,
    _update_run_summary,
)
from mlq.alpha.formal_signal_shared import (
    DEFAULT_ALPHA_FORMAL_SIGNAL_CONTRACT_VERSION,
    DEFAULT_ALPHA_FORMAL_SIGNAL_FAMILY_TABLE,
    DEFAULT_ALPHA_FORMAL_SIGNAL_FILTER_TABLE,
    DEFAULT_ALPHA_FORMAL_SIGNAL_STRUCTURE_TABLE,
    DEFAULT_ALPHA_FORMAL_SIGNAL_TRIGGER_TABLE,
    AlphaFormalSignalBuildSummary,
    _build_alpha_formal_signal_run_id,
    _coerce_date,
    _should_use_queue_execution,
    _to_python_date,
    _write_summary,
)
from mlq.alpha.formal_signal_source import (
    _claim_alpha_formal_signal_scopes,
    _enqueue_alpha_formal_signal_dirty_scopes,
    _ensure_database_exists,
    _load_family_rows,
    _load_alpha_formal_signal_dirty_scopes,
    _load_official_context_rows,
    _load_trigger_rows,
    _mark_alpha_formal_signal_queue_completed,
    _mark_alpha_formal_signal_queue_failed,
    _upsert_alpha_formal_signal_checkpoint,
)
from mlq.core.paths import WorkspaceRoots, default_settings


def run_alpha_formal_signal_build(
    *,
    settings: WorkspaceRoots | None = None,
    alpha_path: Path | None = None,
    filter_path: Path | None = None,
    structure_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    run_id: str | None = None,
    source_trigger_table: str = DEFAULT_ALPHA_FORMAL_SIGNAL_TRIGGER_TABLE,
    source_family_table: str = DEFAULT_ALPHA_FORMAL_SIGNAL_FAMILY_TABLE,
    source_filter_table: str = DEFAULT_ALPHA_FORMAL_SIGNAL_FILTER_TABLE,
    source_structure_table: str = DEFAULT_ALPHA_FORMAL_SIGNAL_STRUCTURE_TABLE,
    signal_contract_version: str = DEFAULT_ALPHA_FORMAL_SIGNAL_CONTRACT_VERSION,
    producer_name: str = "alpha_formal_signal_producer",
    producer_version: str = "v1",
    summary_path: Path | None = None,
    use_checkpoint_queue: bool | None = None,
) -> AlphaFormalSignalBuildSummary:
    """从官方触发事实和 `filter/structure snapshot` 物化 `alpha formal signal`。"""

    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    if _should_use_queue_execution(
        use_checkpoint_queue=use_checkpoint_queue,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
    ):
        return _run_alpha_formal_signal_queue_build(
            settings=settings,
            alpha_path=alpha_path,
            filter_path=filter_path,
            structure_path=structure_path,
            limit=limit,
            batch_size=batch_size,
            run_id=run_id,
            source_trigger_table=source_trigger_table,
            source_family_table=source_family_table,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            signal_contract_version=signal_contract_version,
            producer_name=producer_name,
            producer_version=producer_version,
            summary_path=summary_path,
        )
    return _run_alpha_formal_signal_bounded_build(
        settings=settings,
        alpha_path=alpha_path,
        filter_path=filter_path,
        structure_path=structure_path,
        malf_path=malf_path,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        limit=limit,
        batch_size=batch_size,
        run_id=run_id,
        source_trigger_table=source_trigger_table,
        source_family_table=source_family_table,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        signal_contract_version=signal_contract_version,
        producer_name=producer_name,
        producer_version=producer_version,
        summary_path=summary_path,
    )


def _run_alpha_formal_signal_bounded_build(
    *,
    settings: WorkspaceRoots | None,
    alpha_path: Path | None,
    filter_path: Path | None,
    structure_path: Path | None,
    malf_path: Path | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    batch_size: int,
    run_id: str | None,
    source_trigger_table: str,
    source_family_table: str,
    source_filter_table: str,
    source_structure_table: str,
    signal_contract_version: str,
    producer_name: str,
    producer_version: str,
    summary_path: Path | None,
) -> AlphaFormalSignalBuildSummary:
    """执行显式 bounded formal signal 物化。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    resolved_filter_path = Path(filter_path or workspace.databases.filter)
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    materialization_run_id = run_id or _build_alpha_formal_signal_run_id()

    _ensure_database_exists(resolved_filter_path, label="filter")
    _ensure_database_exists(resolved_structure_path, label="structure")
    alpha_connection = duckdb.connect(str(resolved_alpha_path))
    try:
        bootstrap_alpha_formal_signal_ledger(workspace, connection=alpha_connection)
        trigger_rows = _load_trigger_rows(
            connection=alpha_connection,
            table_name=source_trigger_table,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=instruments,
            limit=normalized_limit,
        )
        family_rows = _load_family_rows(
            connection=alpha_connection,
            table_name=source_family_table,
            trigger_rows=trigger_rows,
        )
        context_rows = _load_official_context_rows(
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            filter_table_name=source_filter_table,
            structure_table_name=source_structure_table,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=tuple(sorted({row.instrument for row in trigger_rows})),
        )
        context_map = {
            (row.instrument, row.signal_date, row.asof_date): row
            for row in context_rows
        }

        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            producer_name=producer_name,
            producer_version=producer_version,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            bounded_instrument_count=len({row.instrument for row in trigger_rows}),
            source_trigger_table=source_trigger_table,
            source_family_table=source_family_table,
            source_context_table=source_filter_table,
            signal_contract_version=signal_contract_version,
        )

        summary = _materialize_formal_signal_rows(
            connection=alpha_connection,
            run_id=materialization_run_id,
            trigger_rows=trigger_rows,
            family_map=family_rows,
            context_map=context_map,
            signal_contract_version=signal_contract_version,
            producer_name=producer_name,
            producer_version=producer_version,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            alpha_path=resolved_alpha_path,
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            source_trigger_table=source_trigger_table,
            source_family_table=source_family_table,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            batch_size=normalized_batch_size,
        )
        _mark_run_completed(alpha_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            alpha_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        alpha_connection.close()


def _run_alpha_formal_signal_queue_build(
    *,
    settings: WorkspaceRoots | None,
    alpha_path: Path | None,
    filter_path: Path | None,
    structure_path: Path | None,
    limit: int,
    batch_size: int,
    run_id: str | None,
    source_trigger_table: str,
    source_family_table: str,
    source_filter_table: str,
    source_structure_table: str,
    signal_contract_version: str,
    producer_name: str,
    producer_version: str,
    summary_path: Path | None,
) -> AlphaFormalSignalBuildSummary:
    """执行默认 data-grade queue/checkpoint 续跑。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    resolved_filter_path = Path(filter_path or workspace.databases.filter)
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    materialization_run_id = run_id or _build_alpha_formal_signal_run_id()

    _ensure_database_exists(resolved_filter_path, label="filter")
    _ensure_database_exists(resolved_structure_path, label="structure")
    alpha_connection = duckdb.connect(str(resolved_alpha_path))
    try:
        bootstrap_alpha_formal_signal_ledger(workspace, connection=alpha_connection)
        dirty_scopes = _load_alpha_formal_signal_dirty_scopes(
            connection=alpha_connection,
            limit=normalized_limit,
        )
        enqueue_counts = _enqueue_alpha_formal_signal_dirty_scopes(
            connection=alpha_connection,
            scope_rows=dirty_scopes,
            run_id=materialization_run_id,
        )
        claimed_scope_rows = _claim_alpha_formal_signal_scopes(
            connection=alpha_connection,
            run_id=materialization_run_id,
        )
        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            producer_name=producer_name,
            producer_version=producer_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            source_trigger_table=source_trigger_table,
            source_family_table=source_family_table,
            source_context_table=source_filter_table,
            signal_contract_version=signal_contract_version,
        )

        summary_counts = {
            "candidate_trigger_count": 0,
            "materialized_signal_count": 0,
            "inserted_count": 0,
            "reused_count": 0,
            "rematerialized_count": 0,
            "missing_context_count": 0,
            "admitted_count": 0,
            "blocked_count": 0,
            "deferred_count": 0,
        }
        checkpoint_upserted_count = 0
        for scope_row in claimed_scope_rows:
            try:
                scope_summary = _materialize_alpha_formal_signal_scope(
                    connection=alpha_connection,
                    run_id=materialization_run_id,
                    alpha_path=resolved_alpha_path,
                    filter_path=resolved_filter_path,
                    structure_path=resolved_structure_path,
                    instrument=str(scope_row["code"]),
                    signal_start_date=_to_python_date(scope_row["replay_start_bar_dt"]),
                    signal_end_date=_to_python_date(scope_row["replay_confirm_until_dt"]),
                    limit=normalized_limit,
                    batch_size=normalized_batch_size,
                    source_trigger_table=source_trigger_table,
                    source_family_table=source_family_table,
                    source_filter_table=source_filter_table,
                    source_structure_table=source_structure_table,
                    signal_contract_version=signal_contract_version,
                    producer_name=producer_name,
                    producer_version=producer_version,
                )
                for key in summary_counts:
                    summary_counts[key] += int(getattr(scope_summary, key))
                _upsert_alpha_formal_signal_checkpoint(
                    alpha_connection,
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
                _mark_alpha_formal_signal_queue_completed(
                    alpha_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
            except Exception:
                _mark_alpha_formal_signal_queue_failed(
                    alpha_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
                raise

        summary = AlphaFormalSignalBuildSummary(
            run_id=materialization_run_id,
            producer_name=producer_name,
            producer_version=producer_version,
            execution_mode="checkpoint_queue",
            signal_contract_version=signal_contract_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            claimed_scope_count=len(claimed_scope_rows),
            candidate_trigger_count=summary_counts["candidate_trigger_count"],
            materialized_signal_count=summary_counts["materialized_signal_count"],
            inserted_count=summary_counts["inserted_count"],
            reused_count=summary_counts["reused_count"],
            rematerialized_count=summary_counts["rematerialized_count"],
            missing_context_count=summary_counts["missing_context_count"],
            admitted_count=summary_counts["admitted_count"],
            blocked_count=summary_counts["blocked_count"],
            deferred_count=summary_counts["deferred_count"],
            queue_enqueued_count=enqueue_counts["queue_enqueued_count"],
            queue_claimed_count=len(claimed_scope_rows),
            checkpoint_upserted_count=checkpoint_upserted_count,
            alpha_ledger_path=str(resolved_alpha_path),
            filter_ledger_path=str(resolved_filter_path),
            structure_ledger_path=str(resolved_structure_path),
            source_trigger_table=source_trigger_table,
            source_family_table=source_family_table,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
        )
        _mark_run_completed(alpha_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            alpha_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        alpha_connection.close()
