"""实现 canonical MALF v2 的纯语义 runner。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import MARKET_BASE_TABLE_BY_ASSET_AND_TIMEFRAME, market_base_timeframe_ledger_path
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
)


DEFAULT_CANONICAL_MARKET_PRICE_TABLE: Final[str] = "stock_daily_adjusted"
DEFAULT_CANONICAL_ADJUST_METHOD: Final[str] = "backward"
DEFAULT_CANONICAL_CONTRACT_VERSION: Final[str] = "malf-canonical-v2"
DEFAULT_CANONICAL_RUNNER_VERSION: Final[str] = "v2"
DEFAULT_CANONICAL_SAMPLE_VERSION: Final[str] = "malf-wave-stats-v1"
DEFAULT_PIVOT_CONFIRMATION_WINDOW: Final[int] = 2
_CANONICAL_NATIVE_TIMEFRAME_MAP: Final[dict[str, str]] = {"D": "day", "W": "week", "M": "month"}


def _build_canonical_summary(
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    canonical_contract_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    timeframe_list: tuple[str, ...],
    bounded_scope_count: int,
    claimed_scope_count: int,
    completed_scope_count: int,
    failed_scope_count: int,
    queue_enqueued_count: int,
    queue_claimed_count: int,
    count_totals: dict[str, int],
    primary_summary_timeframe: str,
    market_base_path_map: dict[str, str],
    malf_ledger_path_map: dict[str, str],
    source_price_table_map: dict[str, str],
    source_adjust_method: str,
    pivot_confirmation_window: int,
    source_scope_count_by_timeframe: dict[str, int],
    claimed_scope_count_by_timeframe: dict[str, int],
    completed_scope_count_by_timeframe: dict[str, int],
    source_row_count_by_timeframe: dict[str, int],
    source_date_range_by_timeframe: dict[str, dict[str, str | None]],
) -> MalfCanonicalBuildSummary:
    return MalfCanonicalBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        canonical_contract_version=canonical_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        timeframe_list=list(timeframe_list),
        bounded_scope_count=bounded_scope_count,
        claimed_scope_count=claimed_scope_count,
        completed_scope_count=completed_scope_count,
        failed_scope_count=failed_scope_count,
        queue_enqueued_count=queue_enqueued_count,
        queue_claimed_count=queue_claimed_count,
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
        primary_summary_timeframe=primary_summary_timeframe,
        market_base_path=market_base_path_map[primary_summary_timeframe],
        malf_ledger_path=malf_ledger_path_map[primary_summary_timeframe],
        market_base_path_map=market_base_path_map,
        malf_ledger_path_map=malf_ledger_path_map,
        source_price_table=source_price_table_map[primary_summary_timeframe],
        source_price_table_map=source_price_table_map,
        source_adjust_method=source_adjust_method,
        pivot_confirmation_window=pivot_confirmation_window,
        source_scope_count_by_timeframe=source_scope_count_by_timeframe,
        claimed_scope_count_by_timeframe=claimed_scope_count_by_timeframe,
        completed_scope_count_by_timeframe=completed_scope_count_by_timeframe,
        source_row_count_by_timeframe=source_row_count_by_timeframe,
        source_date_range_by_timeframe=source_date_range_by_timeframe,
    )


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
    normalized_limit = None if int(limit) <= 0 else int(limit)
    normalized_timeframes = _normalize_timeframes(timeframes)
    normalized_window = max(int(pivot_confirmation_window), 1)
    materialization_run_id = run_id or _build_run_id(prefix="malf-canonical")
    if (market_base_path is not None or malf_path is not None or source_price_table != DEFAULT_CANONICAL_MARKET_PRICE_TABLE) and len(normalized_timeframes) != 1:
        raise ValueError(
            "Custom market_base_path, malf_path, or source_price_table only supports a single timeframe native build."
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
    bounded_scope_count = 0
    claimed_scope_count = 0
    completed_scope_count = 0
    failed_scope_count = 0
    queue_enqueued_count = 0
    queue_claimed_count = 0
    market_base_path_map: dict[str, str] = {}
    malf_ledger_path_map: dict[str, str] = {}
    source_price_table_map: dict[str, str] = {}
    source_scope_count_by_timeframe: dict[str, int] = {}
    claimed_scope_count_by_timeframe: dict[str, int] = {}
    completed_scope_count_by_timeframe: dict[str, int] = {}
    source_row_count_by_timeframe: dict[str, int] = {}
    source_date_range_by_timeframe: dict[str, dict[str, str | None]] = {}

    for timeframe in normalized_timeframes:
        native_market_base_timeframe = _CANONICAL_NATIVE_TIMEFRAME_MAP[timeframe]
        resolved_market_base_path = Path(
            market_base_path or market_base_timeframe_ledger_path(workspace, timeframe=native_market_base_timeframe)
        )
        resolved_malf_path = Path(malf_path or malf_ledger_path(workspace, timeframe=timeframe))
        resolved_source_price_table = (
            source_price_table
            if source_price_table != DEFAULT_CANONICAL_MARKET_PRICE_TABLE
            else MARKET_BASE_TABLE_BY_ASSET_AND_TIMEFRAME[asset_type][native_market_base_timeframe]
        )
        if not resolved_market_base_path.exists():
            raise FileNotFoundError(f"Missing market_base database for timeframe={timeframe}: {resolved_market_base_path}")

        market_base_path_map[timeframe] = str(resolved_market_base_path)
        malf_ledger_path_map[timeframe] = str(resolved_malf_path)
        source_price_table_map[timeframe] = resolved_source_price_table

        market_connection = duckdb.connect(str(resolved_market_base_path), read_only=True)
        malf_connection = duckdb.connect(str(resolved_malf_path))
        timeframe_scope_rows: list[dict[str, object]] = []
        timeframe_claimed_rows: list[dict[str, object]] = []
        timeframe_completed_scope_count = 0
        timeframe_failed_scope_count = 0
        timeframe_source_row_count = 0
        timeframe_source_start_date: date | None = None
        timeframe_source_end_date: date | None = None
        timeframe_count_totals = {key: 0 for key in count_totals}
        try:
            bootstrap_malf_ledger(workspace, connection=malf_connection, timeframe=timeframe)
            timeframe_scope_rows = _load_source_scope_rows(
                market_connection,
                table_name=resolved_source_price_table,
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
                scope_rows=timeframe_scope_rows,
                timeframes=(timeframe,),
                run_id=materialization_run_id,
                to_python_date=_to_python_date,
            )
            timeframe_claimed_rows = _claim_ready_scopes(
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
                bounded_scope_count=len(timeframe_scope_rows),
                pivot_confirmation_window=normalized_window,
                source_price_table=resolved_source_price_table,
                source_adjust_method=adjust_method,
                canonical_contract_version=canonical_contract_version,
                timeframe_list=(timeframe,),
            )
            for scope_row in timeframe_claimed_rows:
                try:
                    timeframe_frame = _load_source_bars(
                        market_connection,
                        table_name=resolved_source_price_table,
                        code=str(scope_row["code"]),
                        adjust_method=adjust_method,
                        signal_end_date=normalized_end_date,
                    )
                    timeframe_source_row_count += len(timeframe_frame)
                    if timeframe_frame.empty:
                        _mark_queue_completed(
                            malf_connection,
                            queue_nk=str(scope_row["queue_nk"]),
                            run_id=materialization_run_id,
                        )
                        timeframe_completed_scope_count += 1
                        continue
                    scope_start_date = _to_python_date(timeframe_frame["trade_date"].min())
                    scope_end_date = _to_python_date(timeframe_frame["trade_date"].max())
                    if scope_start_date is not None:
                        timeframe_source_start_date = (
                            scope_start_date
                            if timeframe_source_start_date is None
                            else min(timeframe_source_start_date, scope_start_date)
                        )
                    if scope_end_date is not None:
                        timeframe_source_end_date = (
                            scope_end_date
                            if timeframe_source_end_date is None
                            else max(timeframe_source_end_date, scope_end_date)
                        )
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
                        timeframe_count_totals[key] += int(value)
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
                    timeframe_completed_scope_count += 1
                except Exception:
                    _mark_queue_failed(
                        malf_connection,
                        queue_nk=str(scope_row["queue_nk"]),
                        run_id=materialization_run_id,
                    )
                    timeframe_failed_scope_count += 1
                    raise

            source_dates = [
                row["last_trade_date"]
                for row in timeframe_scope_rows
                if row.get("last_trade_date") is not None
            ]
            source_date_range_by_timeframe[timeframe] = {
                "start": (
                    timeframe_source_start_date.isoformat()
                    if timeframe_source_start_date is not None
                    else (None if not source_dates else min(source_dates).isoformat())
                ),
                "end": (
                    timeframe_source_end_date.isoformat()
                    if timeframe_source_end_date is not None
                    else (None if not source_dates else max(source_dates).isoformat())
                ),
            }
            source_scope_count_by_timeframe[timeframe] = len(timeframe_scope_rows)
            claimed_scope_count_by_timeframe[timeframe] = len(timeframe_claimed_rows)
            completed_scope_count_by_timeframe[timeframe] = timeframe_completed_scope_count
            source_row_count_by_timeframe[timeframe] = timeframe_source_row_count
            bounded_scope_count += len(timeframe_scope_rows)
            claimed_scope_count += len(timeframe_claimed_rows)
            completed_scope_count += timeframe_completed_scope_count
            failed_scope_count += timeframe_failed_scope_count
            queue_enqueued_count += enqueue_counts["queue_enqueued_count"]
            queue_claimed_count += len(timeframe_claimed_rows)
            timeframe_summary = _build_canonical_summary(
                run_id=materialization_run_id,
                runner_name=runner_name,
                runner_version=runner_version,
                canonical_contract_version=canonical_contract_version,
                signal_start_date=normalized_start_date,
                signal_end_date=normalized_end_date,
                timeframe_list=(timeframe,),
                bounded_scope_count=len(timeframe_scope_rows),
                claimed_scope_count=len(timeframe_claimed_rows),
                completed_scope_count=timeframe_completed_scope_count,
                failed_scope_count=timeframe_failed_scope_count,
                queue_enqueued_count=enqueue_counts["queue_enqueued_count"],
                queue_claimed_count=len(timeframe_claimed_rows),
                count_totals=timeframe_count_totals,
                primary_summary_timeframe=timeframe,
                market_base_path_map={timeframe: str(resolved_market_base_path)},
                malf_ledger_path_map={timeframe: str(resolved_malf_path)},
                source_price_table_map={timeframe: resolved_source_price_table},
                source_adjust_method=adjust_method,
                pivot_confirmation_window=normalized_window,
                source_scope_count_by_timeframe={timeframe: len(timeframe_scope_rows)},
                claimed_scope_count_by_timeframe={timeframe: len(timeframe_claimed_rows)},
                completed_scope_count_by_timeframe={timeframe: timeframe_completed_scope_count},
                source_row_count_by_timeframe={timeframe: timeframe_source_row_count},
                source_date_range_by_timeframe={timeframe: source_date_range_by_timeframe[timeframe]},
            )
            _mark_canonical_run_completed(
                malf_connection,
                run_id=materialization_run_id,
                summary=timeframe_summary,
            )
        except Exception:
            _mark_canonical_run_failed(malf_connection, run_id=materialization_run_id)
            raise
        finally:
            market_connection.close()
            malf_connection.close()

    primary_summary_timeframe = "D" if "D" in normalized_timeframes else normalized_timeframes[0]
    summary = _build_canonical_summary(
        run_id=materialization_run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        canonical_contract_version=canonical_contract_version,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        timeframe_list=normalized_timeframes,
        bounded_scope_count=bounded_scope_count,
        claimed_scope_count=claimed_scope_count,
        completed_scope_count=completed_scope_count,
        failed_scope_count=failed_scope_count,
        queue_enqueued_count=queue_enqueued_count,
        queue_claimed_count=queue_claimed_count,
        count_totals=count_totals,
        primary_summary_timeframe=primary_summary_timeframe,
        market_base_path_map=market_base_path_map,
        malf_ledger_path_map=malf_ledger_path_map,
        source_price_table_map=source_price_table_map,
        source_adjust_method=adjust_method,
        pivot_confirmation_window=normalized_window,
        source_scope_count_by_timeframe=source_scope_count_by_timeframe,
        claimed_scope_count_by_timeframe=claimed_scope_count_by_timeframe,
        completed_scope_count_by_timeframe=completed_scope_count_by_timeframe,
        source_row_count_by_timeframe=source_row_count_by_timeframe,
        source_date_range_by_timeframe=source_date_range_by_timeframe,
    )
    _write_summary(summary.as_dict(), summary_path)
    return summary
