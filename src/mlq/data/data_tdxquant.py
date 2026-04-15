"""`data` 模块的 TdxQuant none 日线桥接 runner 与辅助函数。"""

from __future__ import annotations

from mlq.data.data_common import *
from mlq.data.data_shared import *
from mlq.data.data_tdxquant_support import (
    build_tdxquant_checkpoint_nk as _build_tdxquant_checkpoint_nk,
    build_tdxquant_request_nk as _build_tdxquant_request_nk,
    build_tdxquant_response_digest as _build_tdxquant_response_digest,
    fetch_tdxquant_checkpoint as _fetch_tdxquant_checkpoint,
    insert_raw_tdxquant_run_start as _insert_raw_tdxquant_run_start,
    record_raw_tdxquant_request as _record_raw_tdxquant_request,
    should_skip_tdxquant_request as _should_skip_tdxquant_request,
    update_raw_tdxquant_run_failure as _update_raw_tdxquant_run_failure,
    update_raw_tdxquant_run_success as _update_raw_tdxquant_run_success,
    upsert_tdxquant_checkpoint as _upsert_tdxquant_checkpoint,
)


# TdxQuant 桥接只负责把 none 日线原始事实沉淀进 raw_market，并维护请求审计与 checkpoint。
def run_tdxquant_daily_raw_sync(
    *,
    settings: WorkspaceRoots | None = None,
    strategy_path: Path | str,
    onboarding_instruments: list[str] | tuple[str, ...] | None = None,
    use_registry_scope: bool = True,
    end_trade_date: str | date | None = None,
    count: int = 120,
    limit: int = 100,
    continue_from_checkpoint: bool = True,
    run_id: str | None = None,
    summary_path: Path | None = None,
    client_factory: Callable[[Path], TdxQuantClient] | None = None,
) -> TdxQuantDailyRawSyncSummary:
    """把 `TdxQuant(dividend_type='none')` 的日更原始事实桥接进正式 `raw_market`。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    resolved_strategy_path = _resolve_tdxquant_strategy_path(strategy_path)
    normalized_end_trade_date = _coerce_date(end_trade_date) or datetime.now().date()
    normalized_count = _normalize_positive_number(count=count, field_name="count")
    normalized_limit = _normalize_limit(limit)
    normalized_onboarding = _normalize_tdxquant_codes(onboarding_instruments)
    materialization_run_id = run_id or _build_run_id(prefix="raw-tdxquant-sync")

    connection = duckdb.connect(str(raw_market_ledger_path(workspace)))
    base_connection: duckdb.DuckDBPyConnection | None = None
    client: TdxQuantClient | None = None
    run_terminal_recorded = False
    try:
        scope_codes, scope_source = _resolve_tdxquant_scope(
            connection,
            onboarding_instruments=normalized_onboarding,
            use_registry_scope=use_registry_scope,
            limit=normalized_limit,
        )
        _insert_raw_tdxquant_run_start(
            connection,
            run_id=materialization_run_id,
            strategy_path=resolved_strategy_path,
            scope_source=scope_source,
            requested_end_trade_date=normalized_end_trade_date,
            requested_count=normalized_count,
            candidate_instrument_count=len(scope_codes),
        )
        client = (client_factory or open_tdxquant_client)(resolved_strategy_path)
        processed_instrument_count = 0
        successful_request_count = 0
        failed_request_count = 0
        inserted_bar_count = 0
        reused_bar_count = 0
        rematerialized_bar_count = 0
        dirty_mark_count = 0
        requested_end_time = normalized_end_trade_date.strftime("%Y%m%d") + "150000"
        for code in scope_codes:
            request_nk = _build_tdxquant_request_nk(
                run_id=materialization_run_id,
                code=code,
                requested_dividend_type="none",
                requested_count=normalized_count,
                requested_end_time=requested_end_time,
            )
            instrument_info: TdxQuantInstrumentInfo | None = None
            response_bars: tuple[TdxQuantDailyBar, ...] = ()
            response_digest: str | None = None
            response_trade_date_min: date | None = None
            response_trade_date_max: date | None = None
            try:
                instrument_info = client.get_instrument_info(code)
                response_bars = tuple(
                    sorted(
                        client.get_daily_bars(
                            code=code,
                            end_trade_date=normalized_end_trade_date,
                            count=normalized_count,
                            dividend_type="none",
                        ),
                        key=lambda row: row.trade_date,
                    )
                )
                if not response_bars:
                    raise ValueError(f"TdxQuant returned empty daily bars for {code}")
                response_trade_date_min = response_bars[0].trade_date
                response_trade_date_max = response_bars[-1].trade_date
                response_digest = _build_tdxquant_response_digest(
                    instrument_info=instrument_info,
                    response_bars=response_bars,
                )
                checkpoint_row = None
                if continue_from_checkpoint:
                    checkpoint_row = _fetch_tdxquant_checkpoint(
                        connection,
                        code=instrument_info.code,
                        asset_type=instrument_info.asset_type,
                    )
                if checkpoint_row is not None and _should_skip_tdxquant_request(
                    checkpoint_row=checkpoint_row,
                    response_trade_date_max=response_trade_date_max,
                    response_digest=response_digest,
                ):
                    _record_raw_tdxquant_request(
                        connection,
                        request_nk=request_nk,
                        run_id=materialization_run_id,
                        asset_type=instrument_info.asset_type,
                        code=instrument_info.code,
                        name=instrument_info.name,
                        requested_dividend_type="none",
                        requested_count=normalized_count,
                        requested_end_time=requested_end_time,
                        response_trade_date_min=response_trade_date_min,
                        response_trade_date_max=response_trade_date_max,
                        response_row_count=len(response_bars),
                        response_digest=response_digest,
                        inserted_bar_count=0,
                        reused_bar_count=len(response_bars),
                        rematerialized_bar_count=0,
                        status="skipped_unchanged",
                        error_message=None,
                    )
                    _upsert_tdxquant_checkpoint(
                        connection,
                        code=instrument_info.code,
                        asset_type=instrument_info.asset_type,
                        last_success_trade_date=response_trade_date_max,
                        last_observed_trade_date=response_trade_date_max,
                        last_success_run_id=materialization_run_id,
                        last_response_digest=response_digest,
                    )
                    processed_instrument_count += 1
                    successful_request_count += 1
                    reused_bar_count += len(response_bars)
                    continue

                request_observed_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
                connection.execute("BEGIN TRANSACTION")
                inserted_count, reused_count, rematerialized_count = _replace_raw_bars_for_tdxquant_request(
                    connection,
                    request_nk=request_nk,
                    strategy_path=resolved_strategy_path,
                    instrument_info=instrument_info,
                    response_bars=response_bars,
                    request_observed_at=request_observed_at,
                    run_id=materialization_run_id,
                )
                _record_raw_tdxquant_request(
                    connection,
                    request_nk=request_nk,
                    run_id=materialization_run_id,
                    asset_type=instrument_info.asset_type,
                    code=instrument_info.code,
                    name=instrument_info.name,
                    requested_dividend_type="none",
                    requested_count=normalized_count,
                    requested_end_time=requested_end_time,
                    response_trade_date_min=response_trade_date_min,
                    response_trade_date_max=response_trade_date_max,
                    response_row_count=len(response_bars),
                    response_digest=response_digest,
                    inserted_bar_count=inserted_count,
                    reused_bar_count=reused_count,
                    rematerialized_bar_count=rematerialized_count,
                    status="completed",
                    error_message=None,
                )
                _upsert_tdxquant_checkpoint(
                    connection,
                    code=instrument_info.code,
                    asset_type=instrument_info.asset_type,
                    last_success_trade_date=response_trade_date_max,
                    last_observed_trade_date=response_trade_date_max,
                    last_success_run_id=materialization_run_id,
                    last_response_digest=response_digest,
                )
                connection.execute("COMMIT")
                if inserted_count > 0 or rematerialized_count > 0:
                    if base_connection is None:
                        base_connection = duckdb.connect(str(market_base_ledger_path(workspace)))
                    _upsert_dirty_instrument_on_connection(
                        base_connection,
                        table_name=BASE_DIRTY_INSTRUMENT_TABLE,
                        code=instrument_info.code,
                        adjust_method="none",
                        dirty_reason="raw_tdxquant_changed",
                        source_run_id=materialization_run_id,
                        source_file_nk=request_nk,
                    )
                    dirty_mark_count += 1
                processed_instrument_count += 1
                successful_request_count += 1
                inserted_bar_count += inserted_count
                reused_bar_count += reused_count
                rematerialized_bar_count += rematerialized_count
            except Exception as exc:
                failed_request_count += 1
                processed_instrument_count = successful_request_count + failed_request_count
                try:
                    connection.execute("ROLLBACK")
                except Exception:
                    pass
                _record_raw_tdxquant_request(
                    connection,
                    request_nk=request_nk,
                    run_id=materialization_run_id,
                    asset_type="stock" if instrument_info is None else instrument_info.asset_type,
                    code=code if instrument_info is None else instrument_info.code,
                    name=code if instrument_info is None else instrument_info.name,
                    requested_dividend_type="none",
                    requested_count=normalized_count,
                    requested_end_time=requested_end_time,
                    response_trade_date_min=response_trade_date_min,
                    response_trade_date_max=response_trade_date_max,
                    response_row_count=len(response_bars),
                    response_digest=response_digest,
                    inserted_bar_count=0,
                    reused_bar_count=0,
                    rematerialized_bar_count=0,
                    status="failed",
                    error_message=str(exc),
                )
                _update_raw_tdxquant_run_failure(
                    connection,
                    run_id=materialization_run_id,
                    strategy_path=resolved_strategy_path,
                    scope_source=scope_source,
                    requested_end_trade_date=normalized_end_trade_date,
                    requested_count=normalized_count,
                    candidate_instrument_count=len(scope_codes),
                    processed_instrument_count=processed_instrument_count,
                    successful_request_count=successful_request_count,
                    failed_request_count=failed_request_count,
                    inserted_bar_count=inserted_bar_count,
                    reused_bar_count=reused_bar_count,
                    rematerialized_bar_count=rematerialized_bar_count,
                    dirty_mark_count=dirty_mark_count,
                    error_message=str(exc),
                )
                run_terminal_recorded = True
                raise
        summary = TdxQuantDailyRawSyncSummary(
            run_id=materialization_run_id,
            strategy_path=str(resolved_strategy_path),
            scope_source=scope_source,
            requested_end_trade_date=normalized_end_trade_date.isoformat(),
            requested_count=normalized_count,
            candidate_instrument_count=len(scope_codes),
            processed_instrument_count=processed_instrument_count,
            successful_request_count=successful_request_count,
            failed_request_count=failed_request_count,
            inserted_bar_count=inserted_bar_count,
            reused_bar_count=reused_bar_count,
            rematerialized_bar_count=rematerialized_bar_count,
            dirty_mark_count=dirty_mark_count,
            raw_market_path=str(raw_market_ledger_path(workspace)),
            market_base_path=str(market_base_ledger_path(workspace)),
        )
        _update_raw_tdxquant_run_success(connection, summary=summary)
        run_terminal_recorded = True
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception as exc:
        if not run_terminal_recorded:
            _update_raw_tdxquant_run_failure(
                connection,
                run_id=materialization_run_id,
                strategy_path=resolved_strategy_path,
                scope_source="registry_onboarding_union" if use_registry_scope else "onboarding",
                requested_end_trade_date=normalized_end_trade_date,
                requested_count=normalized_count,
                candidate_instrument_count=0,
                processed_instrument_count=0,
                successful_request_count=0,
                failed_request_count=0,
                inserted_bar_count=0,
                reused_bar_count=0,
                rematerialized_bar_count=0,
                dirty_mark_count=0,
                error_message=str(exc),
            )
        raise
    finally:
        if client is not None:
            client.close()
        if base_connection is not None:
            base_connection.close()
        connection.close()



def _resolve_tdxquant_strategy_path(strategy_path: Path | str) -> Path:
    resolved = Path(strategy_path)
    if not str(resolved).strip():
        raise ValueError("strategy_path must not be empty")
    if resolved.suffix.lower() != ".py":
        raise ValueError("strategy_path must point to a unique .py strategy file")
    return resolved


def _normalize_tdxquant_codes(instruments: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    normalized_codes: list[str] = []
    for instrument in instruments or ():
        candidate = str(instrument).strip().upper()
        if not candidate:
            continue
        if "." not in candidate:
            raise ValueError(f"TdxQuant instruments must use full code format like 600000.SH: {instrument}")
        normalized_codes.append(candidate)
    return tuple(sorted(set(normalized_codes)))


def _normalize_positive_number(*, count: int, field_name: str) -> int:
    normalized = int(count)
    if normalized <= 0:
        raise ValueError(f"{field_name} must be positive")
    return normalized


def _resolve_tdxquant_scope(
    connection: duckdb.DuckDBPyConnection,
    *,
    onboarding_instruments: tuple[str, ...],
    use_registry_scope: bool,
    limit: int | None,
) -> tuple[tuple[str, ...], str]:
    scope_codes: set[str] = set(onboarding_instruments)
    if use_registry_scope:
        registry_rows = connection.execute(
            f"""
            SELECT DISTINCT code
            FROM {RAW_STOCK_FILE_REGISTRY_TABLE}
            ORDER BY code
            """
        ).fetchall()
        scope_codes.update(str(row[0]) for row in registry_rows if row[0] is not None)
    sorted_codes = tuple(sorted(scope_codes))
    if limit is not None:
        sorted_codes = sorted_codes[:limit]
    if not sorted_codes:
        raise ValueError("TdxQuant scope is empty; provide onboarding_instruments or enable registry scope")
    if use_registry_scope and onboarding_instruments:
        return sorted_codes, "registry_onboarding_union"
    if use_registry_scope:
        return sorted_codes, "registry"
    return sorted_codes, "onboarding"

def _replace_raw_bars_for_tdxquant_request(
    connection: duckdb.DuckDBPyConnection,
    *,
    request_nk: str,
    strategy_path: Path,
    instrument_info: TdxQuantInstrumentInfo,
    response_bars: tuple[TdxQuantDailyBar, ...],
    request_observed_at: datetime,
    run_id: str,
) -> tuple[int, int, int]:
    existing_rows = connection.execute(
        f"""
        SELECT
            bar_nk,
            name,
            open,
            high,
            low,
            close,
            volume,
            amount,
            first_seen_run_id,
            created_at
        FROM {RAW_STOCK_DAILY_BAR_TABLE}
        WHERE code = ?
          AND adjust_method = 'none'
        """,
        [instrument_info.code],
    ).fetchall()
    existing_by_bar_nk = {str(row[0]): row for row in existing_rows}
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    records: list[dict[str, object]] = []
    now = request_observed_at
    for row in response_bars:
        bar_nk = _build_bar_nk(code=row.code, trade_date=row.trade_date, adjust_method="none")
        existing = existing_by_bar_nk.get(bar_nk)
        fingerprint = (
            row.name,
            _normalize_float(row.open),
            _normalize_float(row.high),
            _normalize_float(row.low),
            _normalize_float(row.close),
            _normalize_float(row.volume),
            _normalize_float(row.amount),
        )
        if existing is None:
            inserted_count += 1
            first_seen_run_id = run_id
            created_at = now
        else:
            existing_fingerprint = (
                str(existing[1]),
                _normalize_float(existing[2]),
                _normalize_float(existing[3]),
                _normalize_float(existing[4]),
                _normalize_float(existing[5]),
                _normalize_float(existing[6]),
                _normalize_float(existing[7]),
            )
            if existing_fingerprint == fingerprint:
                reused_count += 1
            else:
                rematerialized_count += 1
            first_seen_run_id = str(existing[8]) if existing[8] is not None else run_id
            created_at = existing[9] if existing[9] is not None else now
        records.append(
            {
                "bar_nk": bar_nk,
                "source_file_nk": request_nk,
                "asset_type": instrument_info.asset_type,
                "code": row.code,
                "name": row.name,
                "trade_date": row.trade_date,
                "adjust_method": "none",
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
                "amount": row.amount,
                # TQ 是请求式源头；这里沿用共享 raw 合同，把本次 request 追溯键和观察时间桥接进旧字段。
                "source_path": str(strategy_path),
                "source_mtime_utc": request_observed_at,
                "first_seen_run_id": first_seen_run_id,
                "last_ingested_run_id": run_id,
                "created_at": created_at,
                "updated_at": now,
            }
        )
    if not records:
        return 0, 0, 0
    frame = pd.DataFrame.from_records(records)
    connection.register(RAW_STAGE_RELATION_NAME, frame)
    try:
        connection.execute(
            f"""
            INSERT OR REPLACE INTO {RAW_STOCK_DAILY_BAR_TABLE} (
                bar_nk,
                source_file_nk,
                asset_type,
                code,
                name,
                trade_date,
                adjust_method,
                open,
                high,
                low,
                close,
                volume,
                amount,
                source_path,
                source_mtime_utc,
                first_seen_run_id,
                last_ingested_run_id,
                created_at,
                updated_at
            )
            SELECT
                bar_nk,
                source_file_nk,
                asset_type,
                code,
                name,
                trade_date,
                adjust_method,
                open,
                high,
                low,
                close,
                volume,
                amount,
                source_path,
                source_mtime_utc,
                first_seen_run_id,
                last_ingested_run_id,
                created_at,
                updated_at
            FROM {RAW_STAGE_RELATION_NAME}
            """
        )
    finally:
        connection.unregister(RAW_STAGE_RELATION_NAME)
    return inserted_count, reused_count, rematerialized_count



