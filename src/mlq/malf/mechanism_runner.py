"""执行 `malf` 机制层 sidecar 账本的最小 bounded runner。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.malf.bootstrap import (
    MALF_MECHANISM_CHECKPOINT_TABLE,
    PAS_CONTEXT_SNAPSHOT_TABLE,
    STRUCTURE_CANDIDATE_SNAPSHOT_TABLE,
    bootstrap_malf_ledger,
    malf_ledger_path,
)
from mlq.malf.mechanism_materialization import (
    _derive_break_rows,
    _derive_profile_rows,
    _derive_snapshot_rows,
    _insert_run_row,
    _mark_run_completed,
    _mark_run_failed,
    _materialize_rows,
    _upsert_checkpoints,
)
from mlq.malf.mechanism_shared import (
    MalfMechanismBuildSummary,
    _build_run_id,
    _coerce_date,
    _write_summary,
)
from mlq.malf.mechanism_source import (
    _load_checkpoint_map,
    _load_mechanism_input_rows,
    _normalize_instruments,
)


DEFAULT_MECHANISM_TIMEFRAME: Final[str] = "D"
DEFAULT_MECHANISM_SAMPLE_VERSION: Final[str] = "bridge-v1"
DEFAULT_MECHANISM_CONTRACT_VERSION: Final[str] = "malf-mechanism-v1"


def run_malf_mechanism_build(
    *,
    settings: WorkspaceRoots | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 500,
    batch_size: int = 100,
    timeframe: str = DEFAULT_MECHANISM_TIMEFRAME,
    stats_sample_version: str = DEFAULT_MECHANISM_SAMPLE_VERSION,
    mechanism_contract_version: str = DEFAULT_MECHANISM_CONTRACT_VERSION,
    run_id: str | None = None,
    source_context_table: str = PAS_CONTEXT_SNAPSHOT_TABLE,
    source_structure_input_table: str = STRUCTURE_CANDIDATE_SNAPSHOT_TABLE,
    runner_name: str = "malf_mechanism_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
) -> MalfMechanismBuildSummary:
    """从 bridge v1 `malf` 输入物化 break/stats sidecar 历史账本。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    resolved_malf_path = Path(malf_path or malf_ledger_path(workspace, use_legacy=True))
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    materialization_run_id = run_id or _build_run_id(prefix="malf-mechanism")

    if not resolved_malf_path.exists():
        raise FileNotFoundError(f"Missing malf database: {resolved_malf_path}")

    connection = duckdb.connect(str(resolved_malf_path))
    try:
        bootstrap_malf_ledger(workspace, connection=connection, use_legacy=True)
        _insert_run_row(
            connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=0,
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
            timeframe=timeframe,
            stats_sample_version=stats_sample_version,
            mechanism_contract_version=mechanism_contract_version,
        )
        checkpoint_map = _load_checkpoint_map(connection, timeframe=timeframe)
        input_rows = _load_mechanism_input_rows(
            connection=connection,
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=normalized_instruments,
            limit=normalized_limit,
            batch_size=normalized_batch_size,
            checkpoint_map=checkpoint_map,
            timeframe=timeframe,
        )
        break_rows = _derive_break_rows(
            input_rows=input_rows,
            timeframe=timeframe,
            run_id=materialization_run_id,
        )
        profile_rows, metric_samples = _derive_profile_rows(
            input_rows=input_rows,
            timeframe=timeframe,
            stats_sample_version=stats_sample_version,
            run_id=materialization_run_id,
        )
        snapshot_rows = _derive_snapshot_rows(
            input_rows=input_rows,
            timeframe=timeframe,
            stats_sample_version=stats_sample_version,
            mechanism_contract_version=mechanism_contract_version,
            run_id=materialization_run_id,
            profile_rows=profile_rows,
            metric_samples=metric_samples,
        )
        counts = _materialize_rows(
            connection=connection,
            break_rows=break_rows,
            profile_rows=profile_rows,
            snapshot_rows=snapshot_rows,
        )
        checkpoint_upserted_count = _upsert_checkpoints(
            connection,
            checkpoint_table=MALF_MECHANISM_CHECKPOINT_TABLE,
            input_rows=input_rows,
            timeframe=timeframe,
            run_id=materialization_run_id,
        )
        bounded_instrument_count = len({row.instrument for row in input_rows})
        summary = MalfMechanismBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            timeframe=timeframe,
            stats_sample_version=stats_sample_version,
            mechanism_contract_version=mechanism_contract_version,
            signal_start_date=None if normalized_start_date is None else normalized_start_date.isoformat(),
            signal_end_date=None if normalized_end_date is None else normalized_end_date.isoformat(),
            bounded_instrument_count=bounded_instrument_count,
            source_candidate_count=len(input_rows),
            break_ledger_count=len(break_rows),
            stats_profile_count=len(profile_rows),
            stats_snapshot_count=len(snapshot_rows),
            break_inserted_count=counts["break_inserted_count"],
            break_reused_count=counts["break_reused_count"],
            break_rematerialized_count=counts["break_rematerialized_count"],
            profile_inserted_count=counts["profile_inserted_count"],
            profile_reused_count=counts["profile_reused_count"],
            profile_rematerialized_count=counts["profile_rematerialized_count"],
            snapshot_inserted_count=counts["snapshot_inserted_count"],
            snapshot_reused_count=counts["snapshot_reused_count"],
            snapshot_rematerialized_count=counts["snapshot_rematerialized_count"],
            checkpoint_upserted_count=checkpoint_upserted_count,
            confirmed_break_count=sum(1 for row in break_rows if row["confirmation_status"] == "confirmed"),
            pending_break_count=sum(1 for row in break_rows if row["confirmation_status"] != "confirmed"),
            malf_ledger_path=str(resolved_malf_path),
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
        )
        _mark_run_completed(connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception:
        _mark_run_failed(connection, run_id=materialization_run_id)
        raise
    finally:
        connection.close()
