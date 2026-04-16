"""market_base 正式构建入口。"""

from __future__ import annotations

from mlq.data.data_common import *
from mlq.data.data_market_base_materialization import *
from mlq.data.data_market_base_scope import *
from mlq.data.data_shared import *


def _run_market_base_build_for_asset(
    *,
    asset_type: str,
    timeframe: str,
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
    """按资产与 timeframe 执行 `raw_market -> market_base` 物化。"""

    normalized_asset_type = _normalize_asset_type(asset_type)
    normalized_timeframe = _normalize_timeframe(timeframe)
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
    materialization_run_id = run_id or _build_run_id(
        prefix=f"market-base-{normalized_asset_type}-{normalized_timeframe}"
    )
    raw_table = _resolve_raw_bar_table(asset_type=normalized_asset_type, timeframe=normalized_timeframe)
    market_table = _resolve_market_base_table(asset_type=normalized_asset_type, timeframe=normalized_timeframe)

    market_connection = duckdb.connect(str(market_base_ledger_path(workspace)))
    _insert_base_build_run_start_by_asset(
        market_connection,
        run_id=materialization_run_id,
        asset_type=normalized_asset_type,
        timeframe=normalized_timeframe,
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
            timeframe=normalized_timeframe,
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
            timeframe=normalized_timeframe,
            scope_records=scope_plan.scope_records,
        )
        _stage_market_base_rows_by_asset(
            connection=market_connection,
            asset_type=normalized_asset_type,
            raw_table=raw_table,
            market_table=market_table,
            timeframe=normalized_timeframe,
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
            timeframe=normalized_timeframe,
            adjust_method=adjust_method,
            run_id=materialization_run_id,
            full_scope=_should_delete_missing_market_base_rows_in_scope(
                build_mode=normalized_build_mode,
                effective_stage_limit=effective_stage_limit,
                scope_is_empty=scope_plan.scope_is_empty,
            ),
            instruments=scope_plan.instruments,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
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
                timeframe=normalized_timeframe,
                adjust_method=adjust_method,
                instruments=normalized_instruments,
            )
        summary = MarketBaseBuildSummary(
            run_id=materialization_run_id,
            asset_type=normalized_asset_type,
            timeframe=normalized_timeframe,
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
            timeframe=normalized_timeframe,
            error_message=str(exc),
        )
        raise
    finally:
        market_connection.close()


def run_market_base_build(
    *,
    settings: WorkspaceRoots | None = None,
    timeframe: str = "day",
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
    """执行股票 `raw_market -> market_base` 构建。"""

    return _run_market_base_build_for_asset(
        asset_type=DEFAULT_ASSET_TYPE,
        timeframe=timeframe,
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


def _should_delete_missing_market_base_rows_in_scope(
    *,
    build_mode: str,
    effective_stage_limit: int | None,
    scope_is_empty: bool,
) -> bool:
    """只有未被 row limit 截断的 full 作用域才允许清理缺失行。"""

    return build_mode == "full" and effective_stage_limit is None and not scope_is_empty


def run_asset_market_base_build_batched(
    *,
    asset_type: str,
    timeframe: str = "day",
    settings: WorkspaceRoots | None = None,
    adjust_method: str = "backward",
    instruments: list[str] | tuple[str, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    batch_size: int = 100,
    build_mode: str = "full",
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> dict[str, object]:
    """按标的批次执行 `raw_market -> market_base`，避免一次 staging 全历史。"""

    normalized_asset_type = _normalize_asset_type(asset_type)
    normalized_timeframe = _normalize_timeframe(timeframe)
    normalized_batch_size = int(batch_size)
    if normalized_batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")
    normalized_build_mode = _normalize_build_mode(build_mode)
    if normalized_build_mode != "full":
        raise ValueError("batched market_base build currently supports full mode only")

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    normalized_start_date = _coerce_date(start_date)
    normalized_end_date = _coerce_date(end_date)
    raw_table = _resolve_raw_bar_table(asset_type=normalized_asset_type, timeframe=normalized_timeframe)
    batch_parent_run_id = run_id or _build_run_id(
        prefix=f"market-base-{normalized_asset_type}-{normalized_timeframe}-batch"
    )
    candidate_codes = _fetch_market_base_batch_candidate_codes(
        workspace=workspace,
        raw_table=raw_table,
        adjust_method=adjust_method,
        instruments=normalized_instruments,
        start_date=normalized_start_date,
        end_date=normalized_end_date,
    )
    child_summaries: list[dict[str, object]] = []
    for batch_number, offset in enumerate(range(0, len(candidate_codes), normalized_batch_size), start=1):
        batch_codes = candidate_codes[offset : offset + normalized_batch_size]
        child_run_id = f"{batch_parent_run_id}-b{batch_number:04d}"
        child_summary = run_asset_market_base_build(
            asset_type=normalized_asset_type,
            timeframe=normalized_timeframe,
            settings=workspace,
            adjust_method=adjust_method,
            instruments=batch_codes,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
            limit=0,
            build_mode="full",
            consume_dirty_only=False,
            run_id=child_run_id,
        )
        child_summaries.append(child_summary.as_dict())

    summary: dict[str, object] = {
        "run_id": batch_parent_run_id,
        "asset_type": normalized_asset_type,
        "timeframe": normalized_timeframe,
        "adjust_method": adjust_method,
        "build_mode": "full",
        "batch_size": normalized_batch_size,
        "batch_count": len(child_summaries),
        "instrument_count": len(candidate_codes),
        "source_row_count": sum(int(item["source_row_count"]) for item in child_summaries),
        "inserted_count": sum(int(item["inserted_count"]) for item in child_summaries),
        "reused_count": sum(int(item["reused_count"]) for item in child_summaries),
        "rematerialized_count": sum(int(item["rematerialized_count"]) for item in child_summaries),
        "child_runs": child_summaries,
        "raw_market_path": str(raw_market_ledger_path(workspace)),
        "market_base_path": str(market_base_ledger_path(workspace)),
        "raw_table": raw_table,
        "market_table": _resolve_market_base_table(asset_type=normalized_asset_type, timeframe=normalized_timeframe),
    }
    _write_summary(summary, summary_path)
    return summary


def _fetch_market_base_batch_candidate_codes(
    *,
    workspace: WorkspaceRoots,
    raw_table: str,
    adjust_method: str,
    instruments: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
) -> tuple[str, ...]:
    parameters: list[object] = [adjust_method]
    where_clauses = ["adjust_method = ?"]
    if start_date is not None:
        where_clauses.append("trade_date >= ?")
        parameters.append(start_date)
    if end_date is not None:
        where_clauses.append("trade_date <= ?")
        parameters.append(end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"code IN ({placeholders})")
        parameters.extend(instruments)
    connection = duckdb.connect(str(raw_market_ledger_path(workspace)), read_only=True)
    try:
        rows = connection.execute(
            f"""
            SELECT DISTINCT code
            FROM {raw_table}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY code
            """,
            parameters,
        ).fetchall()
    finally:
        connection.close()
    return tuple(str(row[0]) for row in rows)


def run_asset_market_base_build(
    *,
    asset_type: str,
    timeframe: str = "day",
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
    """执行 stock/index/block 的 `raw_market -> market_base` 构建。"""

    return _run_market_base_build_for_asset(
        asset_type=asset_type,
        timeframe=timeframe,
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
