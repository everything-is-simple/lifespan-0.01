"""实现 canonical MALF v2 的纯语义 runner。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import market_base_ledger_path
from mlq.malf.bootstrap import bootstrap_malf_ledger, malf_ledger_path
from mlq.malf.canonical_materialization import (
    _build_scope_materialization,
    _insert_canonical_run_row,
    _mark_canonical_run_completed,
    _mark_canonical_run_failed,
    _materialize_scope_rows,
    _upsert_checkpoint,
)
from mlq.malf.canonical_shared import (
    DEFAULT_TIMEFRAMES,
    MalfCanonicalBuildSummary,
    _build_run_id,
    _coerce_date,
    _normalize_instruments,
    _normalize_timeframes,
    _to_python_date,
    _write_summary,
)
from mlq.malf.canonical_source import (
    _claim_ready_scopes,
    _enqueue_dirty_scopes,
    _load_source_bars,
    _load_source_scope_rows,
    _mark_queue_completed,
    _mark_queue_failed,
    _resample_bars_by_timeframe,
)


DEFAULT_CANONICAL_MARKET_PRICE_TABLE: Final[str] = "stock_daily_adjusted"
DEFAULT_CANONICAL_ADJUST_METHOD: Final[str] = "backward"
DEFAULT_CANONICAL_CONTRACT_VERSION: Final[str] = "malf-canonical-v2"
DEFAULT_CANONICAL_RUNNER_VERSION: Final[str] = "v2"
DEFAULT_CANONICAL_SAMPLE_VERSION: Final[str] = "malf-wave-stats-v1"
DEFAULT_PIVOT_CONFIRMATION_WINDOW: Final[int] = 2


def run_malf_canonical_build(
    *,
    settings: WorkspaceRoots | None = None,
    market_base_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    asset_type: str = "stock",
    timeframes: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    adjust_method: str = DEFAULT_CANONICAL_ADJUST_METHOD,
    source_price_table: str = DEFAULT_CANONICAL_MARKET_PRICE_TABLE,
    canonical_contract_version: str = DEFAULT_CANONICAL_CONTRACT_VERSION,
    sample_version: str = DEFAULT_CANONICAL_SAMPLE_VERSION,
    pivot_confirmation_window: int = DEFAULT_PIVOT_CONFIRMATION_WINDOW,
    run_id: str | None = None,
    runner_name: str = "malf_canonical_builder",
    runner_version: str = DEFAULT_CANONICAL_RUNNER_VERSION,
    summary_path: Path | None = None,
) -> MalfCanonicalBuildSummary:
    """从官方 market_base 构建 canonical MALF v2 历史账本。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    normalized_limit = max(int(limit), 1)
    normalized_timeframes = _normalize_timeframes(timeframes)
    normalized_window = max(int(pivot_confirmation_window), 1)
    materialization_run_id = run_id or _build_run_id(prefix="malf-canonical")
    resolved_market_base_path = Path(market_base_path or market_base_ledger_path(workspace))
    resolved_malf_path = Path(malf_path or malf_ledger_path(workspace))
    if not resolved_market_base_path.exists():
        raise FileNotFoundError(f"Missing market_base database: {resolved_market_base_path}")

    market_connection = duckdb.connect(str(resolved_market_base_path), read_only=True)
    malf_connection = duckdb.connect(str(resolved_malf_path))
    claimed_scope_rows: list[dict[str, object]] = []
    try:
        bootstrap_malf_ledger(workspace, connection=malf_connection)
        scope_rows = _load_source_scope_rows(
            market_connection,
            table_name=source_price_table,
            adjust_method=adjust_method,
            asset_type=asset_type,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=normalized_instruments,
            limit=normalized_limit,
            to_python_date=_to_python_date,
        )
        enqueue_counts = _enqueue_dirty_scopes(
            malf_connection,
            scope_rows=scope_rows,
            timeframes=normalized_timeframes,
            run_id=materialization_run_id,
            to_python_date=_to_python_date,
        )
        claimed_scope_rows = _claim_ready_scopes(
            malf_connection,
            asset_type=asset_type,
            run_id=materialization_run_id,
            to_python_date=_to_python_date,
        )
        _insert_canonical_run_row(
            malf_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_scope_count=len(scope_rows) * len(normalized_timeframes),
            pivot_confirmation_window=normalized_window,
            source_price_table=source_price_table,
            source_adjust_method=adjust_method,
            canonical_contract_version=canonical_contract_version,
            timeframe_list=normalized_timeframes,
        )

        count_totals = {
            "pivot_row_count": 0,
            "wave_row_count": 0,
            "extreme_row_count": 0,
            "state_row_count": 0,
            "stats_row_count": 0,
            "pivot_inserted_count": 0,
            "pivot_reused_count": 0,
            "pivot_rematerialized_count": 0,
            "wave_inserted_count": 0,
            "wave_reused_count": 0,
            "wave_rematerialized_count": 0,
            "extreme_inserted_count": 0,
            "extreme_reused_count": 0,
            "extreme_rematerialized_count": 0,
            "state_inserted_count": 0,
            "state_reused_count": 0,
            "state_rematerialized_count": 0,
            "stats_inserted_count": 0,
            "stats_reused_count": 0,
            "stats_rematerialized_count": 0,
        }
        completed_scope_count = 0
        failed_scope_count = 0
        for scope_row in claimed_scope_rows:
            try:
                base_frame = _load_source_bars(
                    market_connection,
                    table_name=source_price_table,
                    code=str(scope_row["code"]),
                    adjust_method=adjust_method,
                    signal_end_date=normalized_end_date,
                )
                if base_frame.empty:
                    _mark_queue_completed(
                        malf_connection,
                        queue_nk=str(scope_row["queue_nk"]),
                        run_id=materialization_run_id,
                    )
                    completed_scope_count += 1
                    continue
                timeframe_frame = _resample_bars_by_timeframe(base_frame, str(scope_row["timeframe"]))
                if timeframe_frame.empty:
                    _mark_queue_completed(
                        malf_connection,
                        queue_nk=str(scope_row["queue_nk"]),
                        run_id=materialization_run_id,
                    )
                    completed_scope_count += 1
                    continue
                materialized = _build_scope_materialization(
                    timeframe_frame=timeframe_frame,
                    asset_type=str(scope_row["asset_type"]),
                    code=str(scope_row["code"]),
                    timeframe=str(scope_row["timeframe"]),
                    run_id=materialization_run_id,
                    sample_version=sample_version,
                    pivot_confirmation_window=normalized_window,
                )
                scope_counts = _materialize_scope_rows(
                    malf_connection,
                    asset_type=str(scope_row["asset_type"]),
                    code=str(scope_row["code"]),
                    timeframe=str(scope_row["timeframe"]),
                    run_id=materialization_run_id,
                    materialized=materialized,
                )
                for key, value in scope_counts.items():
                    count_totals[key] += int(value)
                last_completed_bar_dt = _to_python_date(timeframe_frame["trade_date"].max())
                last_wave_id = max((int(row["wave_id"]) for row in materialized["waves"]), default=0)
                _upsert_checkpoint(
                    malf_connection,
                    asset_type=str(scope_row["asset_type"]),
                    code=str(scope_row["code"]),
                    timeframe=str(scope_row["timeframe"]),
                    last_completed_bar_dt=last_completed_bar_dt,
                    tail_start_bar_dt=last_completed_bar_dt,
                    tail_confirm_until_dt=last_completed_bar_dt,
                    last_wave_id=last_wave_id,
                    last_run_id=materialization_run_id,
                )
                _mark_queue_completed(
                    malf_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
                completed_scope_count += 1
            except Exception:
                _mark_queue_failed(
                    malf_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
                failed_scope_count += 1
                raise

        summary = MalfCanonicalBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            canonical_contract_version=canonical_contract_version,
            signal_start_date=None if normalized_start_date is None else normalized_start_date.isoformat(),
            signal_end_date=None if normalized_end_date is None else normalized_end_date.isoformat(),
            timeframe_list=list(normalized_timeframes),
            bounded_scope_count=len(scope_rows) * len(normalized_timeframes),
            claimed_scope_count=len(claimed_scope_rows),
            completed_scope_count=completed_scope_count,
            failed_scope_count=failed_scope_count,
            queue_enqueued_count=enqueue_counts["queue_enqueued_count"],
            queue_claimed_count=len(claimed_scope_rows),
            pivot_row_count=count_totals["pivot_row_count"],
            wave_row_count=count_totals["wave_row_count"],
            extreme_row_count=count_totals["extreme_row_count"],
            state_row_count=count_totals["state_row_count"],
            stats_row_count=count_totals["stats_row_count"],
            pivot_inserted_count=count_totals["pivot_inserted_count"],
            pivot_reused_count=count_totals["pivot_reused_count"],
            pivot_rematerialized_count=count_totals["pivot_rematerialized_count"],
            wave_inserted_count=count_totals["wave_inserted_count"],
            wave_reused_count=count_totals["wave_reused_count"],
            wave_rematerialized_count=count_totals["wave_rematerialized_count"],
            extreme_inserted_count=count_totals["extreme_inserted_count"],
            extreme_reused_count=count_totals["extreme_reused_count"],
            extreme_rematerialized_count=count_totals["extreme_rematerialized_count"],
            state_inserted_count=count_totals["state_inserted_count"],
            state_reused_count=count_totals["state_reused_count"],
            state_rematerialized_count=count_totals["state_rematerialized_count"],
            stats_inserted_count=count_totals["stats_inserted_count"],
            stats_reused_count=count_totals["stats_reused_count"],
            stats_rematerialized_count=count_totals["stats_rematerialized_count"],
            market_base_path=str(resolved_market_base_path),
            malf_ledger_path=str(resolved_malf_path),
            source_price_table=source_price_table,
            source_adjust_method=adjust_method,
            pivot_confirmation_window=normalized_window,
        )
        _mark_canonical_run_completed(malf_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception:
        _mark_canonical_run_failed(malf_connection, run_id=materialization_run_id)
        raise
    finally:
        market_connection.close()
        malf_connection.close()
