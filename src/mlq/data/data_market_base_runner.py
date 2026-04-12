"""market_base 正式构建入口。"""

from __future__ import annotations

from mlq.data.data_common import *
from mlq.data.data_market_base_materialization import *
from mlq.data.data_market_base_scope import *
from mlq.data.data_shared import *

def run_market_base_build(
    *,
    settings: WorkspaceRoots | None = None,
    adjust_method: str = "backward",
    instruments: list[str] | tuple[str, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    limit: int = 1000,
    build_mode: str = "full",
    consume_dirty_only: bool | None = None,
    mark_clean_on_success: bool = True,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> MarketBaseBuildSummary:
    """??? `raw_market` ?? `market_base.stock_daily_adjusted`?"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    normalized_start_date = _coerce_date(start_date)
    normalized_end_date = _coerce_date(end_date)
    normalized_limit = _normalize_limit(limit)
    normalized_build_mode = _normalize_build_mode(build_mode)
    should_consume_dirty_only = _resolve_consume_dirty_only(
        build_mode=normalized_build_mode,
        consume_dirty_only=consume_dirty_only,
    )
    materialization_run_id = run_id or _build_run_id(prefix="market-base")

    market_connection = duckdb.connect(str(market_base_ledger_path(workspace)))
    _insert_base_build_run_start(
        market_connection,
        run_id=materialization_run_id,
        adjust_method=adjust_method,
        build_mode=normalized_build_mode,
        source_scope_kind=_resolve_initial_scope_kind(
            build_mode=normalized_build_mode,
            instruments=normalized_instruments,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
            consume_dirty_only=should_consume_dirty_only,
        ),
    )
    try:
        _attach_raw_market_ledger(
            market_connection,
            raw_market_path=raw_market_ledger_path(workspace),
        )
        scope_plan = _resolve_base_build_scope_plan(
            connection=market_connection,
            adjust_method=adjust_method,
            build_mode=normalized_build_mode,
            consume_dirty_only=should_consume_dirty_only,
            instruments=normalized_instruments,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
            limit=normalized_limit,
        )
        effective_stage_limit = _resolve_market_base_stage_limit(
            source_scope_kind=scope_plan.source_scope_kind,
            limit=normalized_limit,
        )
        market_connection.execute("BEGIN TRANSACTION")
        _record_base_build_scopes(
            market_connection,
            run_id=materialization_run_id,
            scope_records=scope_plan.scope_records,
        )
        _stage_market_base_rows(
            connection=market_connection,
            adjust_method=adjust_method,
            instruments=scope_plan.instruments,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
            limit=effective_stage_limit,
            force_empty_result=scope_plan.scope_is_empty,
        )
        source_row_count = int(
            market_connection.execute(f"SELECT COUNT(*) FROM {MARKET_BASE_STAGE_TABLE}").fetchone()[0]
        )
        inserted_count, reused_count, rematerialized_count = _count_market_base_actions(market_connection)
        _stage_market_base_action_rows(market_connection)
        _materialize_market_base_stage(
            market_connection,
            adjust_method=adjust_method,
            run_id=materialization_run_id,
            full_scope=normalized_build_mode == "full",
        )
        _record_base_build_actions(market_connection, run_id=materialization_run_id)
        consumed_dirty_count = 0
        if mark_clean_on_success and scope_plan.dirty_entries:
            _mark_dirty_entries_consumed(
                market_connection,
                run_id=materialization_run_id,
                dirty_entries=scope_plan.dirty_entries,
            )
            consumed_dirty_count = len(scope_plan.dirty_entries)
        elif mark_clean_on_success and normalized_build_mode == "full":
            consumed_dirty_count = _mark_scope_dirty_entries_consumed(
                market_connection,
                run_id=materialization_run_id,
                adjust_method=adjust_method,
                instruments=normalized_instruments,
            )
        summary = MarketBaseBuildSummary(
            run_id=materialization_run_id,
            asset_type=DEFAULT_ASSET_TYPE,
            adjust_method=adjust_method,
            build_mode=normalized_build_mode,
            source_scope_kind=scope_plan.source_scope_kind,
            source_row_count=source_row_count,
            inserted_count=inserted_count,
            reused_count=reused_count,
            rematerialized_count=rematerialized_count,
            consumed_dirty_count=consumed_dirty_count,
            raw_market_path=str(raw_market_ledger_path(workspace)),
            market_base_path=str(market_base_ledger_path(workspace)),
            raw_table=RAW_STOCK_DAILY_BAR_TABLE,
            market_table=MARKET_BASE_STOCK_DAILY_TABLE,
        )
        _update_base_build_run_success(
            market_connection,
            summary=summary,
        )
        market_connection.execute("COMMIT")
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception as exc:
        try:
            market_connection.execute("ROLLBACK")
        except Exception:
            pass
        _update_base_build_run_failure(
            market_connection,
            run_id=materialization_run_id,
            error_message=str(exc),
        )
        raise
    finally:
        market_connection.close()


def run_asset_market_base_build(
    *,
    asset_type: str,
    settings: WorkspaceRoots | None = None,
    adjust_method: str = "backward",
    instruments: list[str] | tuple[str, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    limit: int = 1000,
    build_mode: str = "full",
    consume_dirty_only: bool | None = None,
    mark_clean_on_success: bool = True,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> MarketBaseBuildSummary:
    """? stock/index/block ????? `raw -> market_base` ?????"""

    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        return run_market_base_build(
            settings=settings,
            adjust_method=adjust_method,
            instruments=instruments,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            build_mode=build_mode,
            consume_dirty_only=consume_dirty_only,
            mark_clean_on_success=mark_clean_on_success,
            run_id=run_id,
            summary_path=summary_path,
        )

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    normalized_start_date = _coerce_date(start_date)
    normalized_end_date = _coerce_date(end_date)
    normalized_limit = _normalize_limit(limit)
    normalized_build_mode = _normalize_build_mode(build_mode)
    should_consume_dirty_only = _resolve_consume_dirty_only(
        build_mode=normalized_build_mode,
        consume_dirty_only=consume_dirty_only,
    )
    materialization_run_id = run_id or _build_run_id(prefix=f"market-base-{normalized_asset_type}")
    raw_table = RAW_DAILY_BAR_TABLE_BY_ASSET_TYPE[normalized_asset_type]
    market_table = MARKET_BASE_DAILY_TABLE_BY_ASSET_TYPE[normalized_asset_type]

    market_connection = duckdb.connect(str(market_base_ledger_path(workspace)))
    _insert_base_build_run_start_by_asset(
        market_connection,
        run_id=materialization_run_id,
        asset_type=normalized_asset_type,
        adjust_method=adjust_method,
        build_mode=normalized_build_mode,
        source_scope_kind=_resolve_initial_scope_kind(
            build_mode=normalized_build_mode,
            instruments=normalized_instruments,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
            consume_dirty_only=should_consume_dirty_only,
        ),
    )
    try:
        _attach_raw_market_ledger(
            market_connection,
            raw_market_path=raw_market_ledger_path(workspace),
        )
        scope_plan = _resolve_base_build_scope_plan_by_asset(
            connection=market_connection,
            asset_type=normalized_asset_type,
            adjust_method=adjust_method,
            build_mode=normalized_build_mode,
            consume_dirty_only=should_consume_dirty_only,
            instruments=normalized_instruments,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
            limit=normalized_limit,
        )
        effective_stage_limit = _resolve_market_base_stage_limit(
            source_scope_kind=scope_plan.source_scope_kind,
            limit=normalized_limit,
        )
        market_connection.execute("BEGIN TRANSACTION")
        _record_base_build_scopes_by_asset(
            market_connection,
            run_id=materialization_run_id,
            asset_type=normalized_asset_type,
            scope_records=scope_plan.scope_records,
        )
        _stage_market_base_rows_by_asset(
            connection=market_connection,
            asset_type=normalized_asset_type,
            raw_table=raw_table,
            market_table=market_table,
            adjust_method=adjust_method,
            instruments=scope_plan.instruments,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
            limit=effective_stage_limit,
            force_empty_result=scope_plan.scope_is_empty,
        )
        source_row_count = int(
            market_connection.execute(f"SELECT COUNT(*) FROM {MARKET_BASE_STAGE_TABLE}").fetchone()[0]
        )
        inserted_count, reused_count, rematerialized_count = _count_market_base_actions(market_connection)
        _stage_market_base_action_rows(market_connection)
        _materialize_market_base_stage_by_asset(
            market_connection,
            market_table=market_table,
            adjust_method=adjust_method,
            run_id=materialization_run_id,
            full_scope=normalized_build_mode == "full",
        )
        _record_base_build_actions_by_asset(
            market_connection,
            run_id=materialization_run_id,
            asset_type=normalized_asset_type,
        )
        consumed_dirty_count = 0
        if mark_clean_on_success and scope_plan.dirty_entries:
            _mark_dirty_entries_consumed(
                market_connection,
                run_id=materialization_run_id,
                dirty_entries=scope_plan.dirty_entries,
            )
            consumed_dirty_count = len(scope_plan.dirty_entries)
        elif mark_clean_on_success and normalized_build_mode == "full":
            consumed_dirty_count = _mark_scope_dirty_entries_consumed_by_asset(
                market_connection,
                run_id=materialization_run_id,
                asset_type=normalized_asset_type,
                adjust_method=adjust_method,
                instruments=normalized_instruments,
            )
        summary = MarketBaseBuildSummary(
            run_id=materialization_run_id,
            asset_type=normalized_asset_type,
            adjust_method=adjust_method,
            build_mode=normalized_build_mode,
            source_scope_kind=scope_plan.source_scope_kind,
            source_row_count=source_row_count,
            inserted_count=inserted_count,
            reused_count=reused_count,
            rematerialized_count=rematerialized_count,
            consumed_dirty_count=consumed_dirty_count,
            raw_market_path=str(raw_market_ledger_path(workspace)),
            market_base_path=str(market_base_ledger_path(workspace)),
            raw_table=raw_table,
            market_table=market_table,
        )
        _update_base_build_run_success(market_connection, summary=summary)
        market_connection.execute("COMMIT")
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception as exc:
        try:
            market_connection.execute("ROLLBACK")
        except Exception:
            pass
        _update_base_build_run_failure_by_asset(
            market_connection,
            run_id=materialization_run_id,
            asset_type=normalized_asset_type,
            error_message=str(exc),
        )
        raise
    finally:
        market_connection.close()


