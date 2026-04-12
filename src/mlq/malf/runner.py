"""正式 bridge v1 `market_base -> malf` 快照 runner。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import market_base_ledger_path
from mlq.malf.bootstrap import bootstrap_malf_ledger, malf_ledger_path
from mlq.malf.snapshot_materialization import (
    _insert_run_row,
    _mark_run_completed,
    _mark_run_failed,
    _materialize_snapshot_rows,
)
from mlq.malf.snapshot_shared import (
    DEFAULT_MARKET_PRICE_TABLE as SHARED_DEFAULT_MARKET_PRICE_TABLE,
    DEFAULT_MALF_ADJUST_METHOD as SHARED_DEFAULT_MALF_ADJUST_METHOD,
    DEFAULT_MALF_CONTRACT_VERSION as SHARED_DEFAULT_MALF_CONTRACT_VERSION,
    MalfSnapshotBuildSummary,
    _build_run_id,
    _chunked,
    _coerce_date,
    _normalize_instruments,
    _write_summary,
)
from mlq.malf.snapshot_source import (
    _derive_malf_snapshots,
    _load_price_frame,
    _load_target_instruments,
)


DEFAULT_MARKET_PRICE_TABLE: Final[str] = SHARED_DEFAULT_MARKET_PRICE_TABLE
DEFAULT_MALF_CONTRACT_VERSION: Final[str] = SHARED_DEFAULT_MALF_CONTRACT_VERSION
DEFAULT_MALF_ADJUST_METHOD: Final[str] = SHARED_DEFAULT_MALF_ADJUST_METHOD


def run_malf_snapshot_build(
    *,
    settings: WorkspaceRoots | None = None,
    market_base_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    adjust_method: str = DEFAULT_MALF_ADJUST_METHOD,
    source_price_table: str = DEFAULT_MARKET_PRICE_TABLE,
    malf_contract_version: str = DEFAULT_MALF_CONTRACT_VERSION,
    run_id: str | None = None,
    runner_name: str = "malf_snapshot_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
) -> MalfSnapshotBuildSummary:
    """从 `market_base` 物化 bridge v1 `malf` 快照。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    materialization_run_id = run_id or _build_run_id(prefix="malf")
    resolved_market_base_path = Path(market_base_path or market_base_ledger_path(workspace))
    resolved_malf_path = Path(malf_path or malf_ledger_path(workspace))
    if not resolved_market_base_path.exists():
        raise FileNotFoundError(f"Missing market_base database: {resolved_market_base_path}")

    market_connection = duckdb.connect(str(resolved_market_base_path), read_only=True)
    malf_connection = duckdb.connect(str(resolved_malf_path))
    try:
        bootstrap_malf_ledger(workspace, connection=malf_connection)
        instrument_list = _load_target_instruments(
            market_connection,
            table_name=source_price_table,
            adjust_method=adjust_method,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=normalized_instruments,
            limit=normalized_limit,
        )
        _insert_run_row(
            malf_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=len(instrument_list),
            source_price_table=source_price_table,
            adjust_method=adjust_method,
            malf_contract_version=malf_contract_version,
        )

        all_context_rows: list[dict[str, object]] = []
        all_structure_rows: list[dict[str, object]] = []
        source_price_row_count = 0
        for batch in _chunked(instrument_list, size=normalized_batch_size):
            price_frame = _load_price_frame(
                market_connection,
                table_name=source_price_table,
                adjust_method=adjust_method,
                instruments=batch,
                signal_start_date=normalized_start_date,
                signal_end_date=normalized_end_date,
            )
            if price_frame.empty:
                continue
            source_price_row_count += int(len(price_frame))
            context_rows, structure_rows = _derive_malf_snapshots(
                price_frame,
                signal_start_date=normalized_start_date,
                signal_end_date=normalized_end_date,
                adjust_method=adjust_method,
                malf_contract_version=malf_contract_version,
                run_id=materialization_run_id,
            )
            all_context_rows.extend(context_rows)
            all_structure_rows.extend(structure_rows)

        counts = _materialize_snapshot_rows(
            malf_connection,
            run_id=materialization_run_id,
            context_rows=all_context_rows,
            structure_rows=all_structure_rows,
        )
        summary = MalfSnapshotBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            malf_contract_version=malf_contract_version,
            signal_start_date=None if normalized_start_date is None else normalized_start_date.isoformat(),
            signal_end_date=None if normalized_end_date is None else normalized_end_date.isoformat(),
            bounded_instrument_count=len(instrument_list),
            source_price_row_count=source_price_row_count,
            context_snapshot_count=len(all_context_rows),
            structure_candidate_count=len(all_structure_rows),
            context_inserted_count=counts["context_inserted_count"],
            context_reused_count=counts["context_reused_count"],
            context_rematerialized_count=counts["context_rematerialized_count"],
            structure_inserted_count=counts["structure_inserted_count"],
            structure_reused_count=counts["structure_reused_count"],
            structure_rematerialized_count=counts["structure_rematerialized_count"],
            market_base_path=str(resolved_market_base_path),
            malf_ledger_path=str(resolved_malf_path),
            source_price_table=source_price_table,
            adjust_method=adjust_method,
        )
        _mark_run_completed(malf_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception:
        _mark_run_failed(malf_connection, run_id=materialization_run_id)
        raise
    finally:
        market_connection.close()
        malf_connection.close()
