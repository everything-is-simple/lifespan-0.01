"""执行 `TDX -> raw_market -> market_base` 的正式 runner。"""

from __future__ import annotations

import json
import hashlib
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Final

import duckdb
import pandas as pd

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import (
    BASE_BUILD_ACTION_TABLE,
    BASE_BUILD_RUN_TABLE,
    BASE_BUILD_SCOPE_TABLE,
    BASE_DIRTY_INSTRUMENT_TABLE,
    MARKET_BASE_STOCK_DAILY_TABLE,
    RAW_INGEST_FILE_TABLE,
    RAW_INGEST_RUN_TABLE,
    RAW_STOCK_DAILY_BAR_TABLE,
    RAW_STOCK_FILE_REGISTRY_TABLE,
    RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE,
    RAW_TDXQUANT_REQUEST_TABLE,
    RAW_TDXQUANT_RUN_TABLE,
    bootstrap_market_base_ledger,
    bootstrap_raw_market_ledger,
    market_base_ledger_path,
    raw_market_ledger_path,
)
from mlq.data.tdx import parse_tdx_stock_file, resolve_adjust_method_folder
from mlq.data.tdxquant import (
    TdxQuantClient,
    TdxQuantDailyBar,
    TdxQuantInstrumentInfo,
    open_tdxquant_client,
)


DEFAULT_TDX_SOURCE_ROOT: Final[Path] = Path("H:/tdx_offline_Data")
DEFAULT_ASSET_TYPE: Final[str] = "stock"
RAW_STAGE_RELATION_NAME: Final[str] = "_raw_stock_daily_stage"
MARKET_BASE_STAGE_TABLE: Final[str] = "stage_market_base"
MARKET_BASE_EXISTING_STAGE_TABLE: Final[str] = "stage_market_base_existing"
MARKET_BASE_FINAL_STAGE_TABLE: Final[str] = "stage_market_base_final"
MARKET_BASE_ACTION_STAGE_TABLE: Final[str] = "stage_market_base_action"
RAW_INGEST_RUNNER_NAME: Final[str] = "run_tdx_stock_raw_ingest"
RAW_INGEST_RUNNER_VERSION: Final[str] = "2026-04-10-card17-slice5"
TDXQUANT_DAILY_RAW_SYNC_RUNNER_NAME: Final[str] = "run_tdxquant_daily_raw_sync"
TDXQUANT_DAILY_RAW_SYNC_RUNNER_VERSION: Final[str] = "2026-04-10-card19-slice2"
BASE_BUILD_RUNNER_NAME: Final[str] = "run_market_base_build"
BASE_BUILD_RUNNER_VERSION: Final[str] = "2026-04-10-card17-slice1"


@dataclass(frozen=True)
class TdxStockRawIngestSummary:
    run_id: str
    asset_type: str
    adjust_method: str
    run_mode: str
    candidate_file_count: int
    processed_file_count: int
    ingested_file_count: int
    skipped_unchanged_file_count: int
    failed_file_count: int
    bar_inserted_count: int
    bar_reused_count: int
    bar_rematerialized_count: int
    raw_market_path: str
    source_root: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MarketBaseBuildSummary:
    run_id: str
    adjust_method: str
    build_mode: str
    source_scope_kind: str
    source_row_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    consumed_dirty_count: int
    raw_market_path: str
    market_base_path: str
    raw_table: str
    market_table: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class TdxQuantDailyRawSyncSummary:
    run_id: str
    strategy_path: str
    scope_source: str
    requested_end_trade_date: str
    requested_count: int
    candidate_instrument_count: int
    processed_instrument_count: int
    successful_request_count: int
    failed_request_count: int
    inserted_bar_count: int
    reused_bar_count: int
    rematerialized_bar_count: int
    dirty_mark_count: int
    raw_market_path: str
    market_base_path: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BaseDirtyInstrumentEntry:
    dirty_nk: str
    code: str
    adjust_method: str
    dirty_reason: str
    source_run_id: str | None
    source_file_nk: str | None


@dataclass(frozen=True)
class BaseBuildScopePlan:
    source_scope_kind: str
    instruments: tuple[str, ...]
    scope_records: tuple[tuple[str, str], ...]
    dirty_entries: tuple[BaseDirtyInstrumentEntry, ...]
    scope_is_empty: bool


def run_tdx_stock_raw_ingest(
    *,
    settings: WorkspaceRoots | None = None,
    source_root: Path | str | None = None,
    adjust_method: str = "backward",
    run_mode: str = "incremental",
    force_hash: bool = False,
    continue_from_last_run: bool = False,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> TdxStockRawIngestSummary:
    """把 TDX 离线股票日线增量 ingest 到正式 `raw_market`。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    resolved_source_root = Path(source_root or DEFAULT_TDX_SOURCE_ROOT)
    folder_path = resolved_source_root / DEFAULT_ASSET_TYPE / resolve_adjust_method_folder(adjust_method)
    if not folder_path.exists():
        raise FileNotFoundError(f"Missing TDX source directory: {folder_path}")

    normalized_instruments = _normalize_instruments(instruments)
    normalized_limit = _normalize_limit(limit)
    normalized_run_mode = _normalize_raw_run_mode(run_mode)
    materialization_run_id = run_id or _build_run_id(prefix="raw-stock-ingest")
    matching_files = [
        path
        for path in sorted(folder_path.glob("*.txt"))
        if _match_instrument_filter(path, normalized_instruments)
    ]
    limited_candidate_files = matching_files if normalized_limit is None else matching_files[:normalized_limit]

    connection = duckdb.connect(str(raw_market_ledger_path(workspace)))
    base_connection: duckdb.DuckDBPyConnection | None = None
    try:
        candidate_files = _resolve_raw_candidate_files(
            connection,
            adjust_method=adjust_method,
            source_root=resolved_source_root,
            candidate_files=limited_candidate_files,
            continue_from_last_run=continue_from_last_run,
        )
        _insert_raw_ingest_run_start(
            connection,
            run_id=materialization_run_id,
            adjust_method=adjust_method,
            run_mode=normalized_run_mode,
            source_root=resolved_source_root,
            candidate_file_count=len(candidate_files),
        )
        ingested_file_count = 0
        skipped_unchanged_file_count = 0
        failed_file_count = 0
        bar_inserted_count = 0
        bar_reused_count = 0
        bar_rematerialized_count = 0
        for path in candidate_files:
            code = _resolve_code_from_filename(path)
            fallback_file_nk = _build_file_nk(
                asset_type=DEFAULT_ASSET_TYPE,
                adjust_method=adjust_method,
                code=code,
                name=code,
                source_path=path,
            )
            try:
                used_content_hash_fingerprint = bool(force_hash)
                stat_result = path.stat()
                source_size_bytes = stat_result.st_size
                source_mtime_utc = datetime.fromtimestamp(stat_result.st_mtime).replace(microsecond=0)
                registry_row = connection.execute(
                    f"""
                    SELECT source_size_bytes, source_mtime_utc, source_line_count, name, file_nk, source_content_hash
                    FROM {RAW_STOCK_FILE_REGISTRY_TABLE}
                    WHERE asset_type = ?
                      AND adjust_method = ?
                      AND code = ?
                      AND source_path = ?
                    """,
                    [DEFAULT_ASSET_TYPE, adjust_method, code, str(path)],
                ).fetchone()
                used_content_hash_fingerprint = force_hash or (
                    registry_row is not None
                    and int(registry_row[0]) == source_size_bytes
                    and _normalize_timestamp(registry_row[1]) != source_mtime_utc
                )
                fingerprint_mode = "size_mtime"
                current_content_hash: str | None = None
                if registry_row is not None and _should_skip_raw_file(
                    path=path,
                    source_size_bytes=source_size_bytes,
                    source_mtime_utc=source_mtime_utc,
                    registry_row=registry_row,
                    force_hash=force_hash,
                ):
                    if used_content_hash_fingerprint:
                        fingerprint_mode = "content_hash"
                        current_content_hash = _compute_file_content_hash(path)
                        stored_hash = None if registry_row[5] is None else str(registry_row[5])
                        if stored_hash == current_content_hash:
                            _refresh_file_registry_fingerprint(
                                connection,
                                file_nk=str(registry_row[4]),
                                source_size_bytes=source_size_bytes,
                                source_mtime_utc=source_mtime_utc,
                                source_content_hash=current_content_hash,
                            )
                    else:
                        fingerprint_mode = "size_mtime"
                    skipped_unchanged_file_count += 1
                    _record_raw_ingest_file(
                        connection,
                        run_id=materialization_run_id,
                        file_nk=str(registry_row[4]),
                        code=code,
                        name=str(registry_row[3]),
                        adjust_method=adjust_method,
                        source_path=path,
                        fingerprint_mode=fingerprint_mode,
                        action="skipped_unchanged",
                        row_count=int(registry_row[2]),
                        error_message=None,
                    )
                    continue

                parsed = parse_tdx_stock_file(path)
                current_content_hash = current_content_hash or _compute_file_content_hash(path)
                file_nk = _build_file_nk(
                    asset_type=DEFAULT_ASSET_TYPE,
                    adjust_method=adjust_method,
                    code=parsed.code,
                    name=parsed.name,
                    source_path=path,
                )
                connection.execute("BEGIN TRANSACTION")
                inserted_count, reused_count, rematerialized_count = _replace_raw_bars_for_file(
                    connection,
                    file_nk=file_nk,
                    adjust_method=adjust_method,
                    parsed=parsed,
                    source_path=path,
                    source_mtime_utc=source_mtime_utc,
                    run_id=materialization_run_id,
                )
                _upsert_file_registry(
                    connection,
                    file_nk=file_nk,
                    adjust_method=adjust_method,
                    parsed_name=parsed.name,
                    parsed_code=parsed.code,
                    source_path=path,
                    source_size_bytes=source_size_bytes,
                    source_mtime_utc=source_mtime_utc,
                    source_line_count=len(parsed.rows),
                    source_header=parsed.header,
                    source_content_hash=current_content_hash,
                    run_id=materialization_run_id,
                )
                raw_file_action = _resolve_raw_file_action(
                    inserted_count=inserted_count,
                    reused_count=reused_count,
                    rematerialized_count=rematerialized_count,
                )
                if inserted_count > 0 or rematerialized_count > 0:
                    if base_connection is None:
                        base_connection = duckdb.connect(str(market_base_ledger_path(workspace)))
                    _upsert_dirty_instrument_on_connection(
                        base_connection,
                        table_name=BASE_DIRTY_INSTRUMENT_TABLE,
                        code=parsed.code,
                        adjust_method=adjust_method,
                        dirty_reason=f"raw_{raw_file_action}",
                        source_run_id=materialization_run_id,
                        source_file_nk=file_nk,
                    )
                _record_raw_ingest_file(
                    connection,
                    run_id=materialization_run_id,
                    file_nk=file_nk,
                    code=parsed.code,
                    name=parsed.name,
                    adjust_method=adjust_method,
                    source_path=path,
                    fingerprint_mode="content_hash" if used_content_hash_fingerprint else "size_mtime",
                    action=raw_file_action,
                    row_count=len(parsed.rows),
                    error_message=None,
                )
                connection.execute("COMMIT")
                ingested_file_count += 1
                bar_inserted_count += inserted_count
                bar_reused_count += reused_count
                bar_rematerialized_count += rematerialized_count
            except Exception as exc:
                failed_file_count += 1
                try:
                    connection.execute("ROLLBACK")
                except Exception:
                    pass
                _record_raw_ingest_file(
                    connection,
                    run_id=materialization_run_id,
                    file_nk=fallback_file_nk,
                    code=code,
                    name=code,
                    adjust_method=adjust_method,
                    source_path=path,
                    fingerprint_mode="content_hash" if used_content_hash_fingerprint else "size_mtime",
                    action="failed",
                    row_count=0,
                    error_message=str(exc),
                )
                processed_file_count = ingested_file_count + skipped_unchanged_file_count + failed_file_count
                _update_raw_ingest_run_failure(
                    connection,
                    run_id=materialization_run_id,
                    adjust_method=adjust_method,
                    run_mode=normalized_run_mode,
                    source_root=resolved_source_root,
                    candidate_file_count=len(candidate_files),
                    processed_file_count=processed_file_count,
                    skipped_file_count=skipped_unchanged_file_count,
                    inserted_bar_count=bar_inserted_count,
                    reused_bar_count=bar_reused_count,
                    rematerialized_bar_count=bar_rematerialized_count,
                    failed_file_count=failed_file_count,
                    error_message=str(exc),
                )
                raise

        processed_file_count = ingested_file_count + skipped_unchanged_file_count + failed_file_count
        summary = TdxStockRawIngestSummary(
            run_id=materialization_run_id,
            asset_type=DEFAULT_ASSET_TYPE,
            adjust_method=adjust_method,
            run_mode=normalized_run_mode,
            candidate_file_count=len(candidate_files),
            processed_file_count=processed_file_count,
            ingested_file_count=ingested_file_count,
            skipped_unchanged_file_count=skipped_unchanged_file_count,
            failed_file_count=failed_file_count,
            bar_inserted_count=bar_inserted_count,
            bar_reused_count=bar_reused_count,
            bar_rematerialized_count=bar_rematerialized_count,
            raw_market_path=str(raw_market_ledger_path(workspace)),
            source_root=str(resolved_source_root),
        )
        _update_raw_ingest_run_success(connection, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    finally:
        if base_connection is not None:
            base_connection.close()
        connection.close()


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
    """从官方 `raw_market` 物化 `market_base.stock_daily_adjusted`。"""

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


def mark_base_instrument_dirty(
    *,
    settings: WorkspaceRoots | None = None,
    code: str,
    adjust_method: str,
    dirty_reason: str,
    source_run_id: str | None = None,
    source_file_nk: str | None = None,
) -> str:
    """向 `base_dirty_instrument` 追加或刷新待消费的脏标的。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_market_base_ledger(workspace)
    normalized_code = str(code).strip().upper()
    if not normalized_code:
        raise ValueError("code must not be empty")
    normalized_adjust_method = str(adjust_method).strip().lower()
    if normalized_adjust_method not in {"backward", "forward", "none"}:
        raise ValueError(f"Unsupported adjust method: {adjust_method}")
    normalized_reason = str(dirty_reason).strip()
    if not normalized_reason:
        raise ValueError("dirty_reason must not be empty")
    connection = duckdb.connect(str(market_base_ledger_path(workspace)))
    try:
        return _upsert_dirty_instrument_on_connection(
            connection,
            table_name=BASE_DIRTY_INSTRUMENT_TABLE,
            code=normalized_code,
            adjust_method=normalized_adjust_method,
            dirty_reason=normalized_reason,
            source_run_id=source_run_id,
            source_file_nk=source_file_nk,
        )
    finally:
        connection.close()


def _upsert_dirty_instrument_on_connection(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    code: str,
    adjust_method: str,
    dirty_reason: str,
    source_run_id: str | None,
    source_file_nk: str | None,
) -> str:
    normalized_code = str(code).strip().upper()
    normalized_adjust_method = str(adjust_method).strip().lower()
    normalized_reason = str(dirty_reason).strip()
    dirty_nk = _build_dirty_nk(code=normalized_code, adjust_method=normalized_adjust_method)
    existing = connection.execute(
        f"SELECT dirty_nk FROM {table_name} WHERE dirty_nk = ?",
        [dirty_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {table_name} (
                dirty_nk,
                code,
                adjust_method,
                dirty_reason,
                source_run_id,
                source_file_nk,
                dirty_status,
                last_consumed_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL)
            """,
            [
                dirty_nk,
                normalized_code,
                normalized_adjust_method,
                normalized_reason,
                source_run_id,
                source_file_nk,
            ],
        )
        return dirty_nk
    connection.execute(
        f"""
        UPDATE {table_name}
        SET
            dirty_reason = ?,
            source_run_id = ?,
            source_file_nk = ?,
            dirty_status = 'pending',
            last_marked_at = CURRENT_TIMESTAMP
        WHERE dirty_nk = ?
        """,
        [
            normalized_reason,
            source_run_id,
            source_file_nk,
            dirty_nk,
        ],
    )
    return dirty_nk


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


def _build_tdxquant_request_nk(
    *,
    run_id: str,
    code: str,
    requested_dividend_type: str,
    requested_count: int,
    requested_end_time: str,
) -> str:
    return "|".join([run_id, code, requested_dividend_type, str(requested_count), requested_end_time])


def _build_tdxquant_checkpoint_nk(*, code: str, asset_type: str) -> str:
    return "|".join([code, asset_type])


def _fetch_tdxquant_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    code: str,
    asset_type: str,
) -> tuple[object, ...] | None:
    return connection.execute(
        f"""
        SELECT
            checkpoint_nk,
            last_success_trade_date,
            last_observed_trade_date,
            last_success_run_id,
            last_response_digest
        FROM {RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE}
        WHERE checkpoint_nk = ?
        """,
        [_build_tdxquant_checkpoint_nk(code=code, asset_type=asset_type)],
    ).fetchone()


def _should_skip_tdxquant_request(
    *,
    checkpoint_row: tuple[object, ...],
    response_trade_date_max: date,
    response_digest: str,
) -> bool:
    last_observed = checkpoint_row[2]
    last_digest = checkpoint_row[4]
    if last_observed is None or last_digest is None:
        return False
    return _coerce_date(last_observed) == response_trade_date_max and str(last_digest) == response_digest


def _build_tdxquant_response_digest(
    *,
    instrument_info: TdxQuantInstrumentInfo,
    response_bars: tuple[TdxQuantDailyBar, ...],
) -> str:
    payload = {
        "code": instrument_info.code,
        "name": instrument_info.name,
        "asset_type": instrument_info.asset_type,
        "bars": [
            {
                "trade_date": row.trade_date.isoformat(),
                "open": _normalize_float(row.open),
                "high": _normalize_float(row.high),
                "low": _normalize_float(row.low),
                "close": _normalize_float(row.close),
                "volume": _normalize_float(row.volume),
                "amount": _normalize_float(row.amount),
            }
            for row in response_bars
        ],
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


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


def _record_raw_tdxquant_request(
    connection: duckdb.DuckDBPyConnection,
    *,
    request_nk: str,
    run_id: str,
    asset_type: str,
    code: str,
    name: str,
    requested_dividend_type: str,
    requested_count: int,
    requested_end_time: str,
    response_trade_date_min: date | None,
    response_trade_date_max: date | None,
    response_row_count: int,
    response_digest: str | None,
    inserted_bar_count: int,
    reused_bar_count: int,
    rematerialized_bar_count: int,
    status: str,
    error_message: str | None,
) -> None:
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {RAW_TDXQUANT_REQUEST_TABLE} (
            request_nk,
            run_id,
            asset_type,
            code,
            name,
            requested_dividend_type,
            requested_count,
            requested_end_time,
            response_trade_date_min,
            response_trade_date_max,
            response_row_count,
            response_digest,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            status,
            error_message,
            recorded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            request_nk,
            run_id,
            asset_type,
            code,
            name,
            requested_dividend_type,
            requested_count,
            requested_end_time,
            response_trade_date_min,
            response_trade_date_max,
            response_row_count,
            response_digest,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            status,
            error_message,
        ],
    )


def _upsert_tdxquant_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    code: str,
    asset_type: str,
    last_success_trade_date: date | None,
    last_observed_trade_date: date | None,
    last_success_run_id: str,
    last_response_digest: str | None,
) -> None:
    checkpoint_nk = _build_tdxquant_checkpoint_nk(code=code, asset_type=asset_type)
    existing = connection.execute(
        f"""
        SELECT checkpoint_nk
        FROM {RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE}
        WHERE checkpoint_nk = ?
        """,
        [checkpoint_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE} (
                checkpoint_nk,
                code,
                asset_type,
                last_success_trade_date,
                last_observed_trade_date,
                last_success_run_id,
                last_response_digest
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                checkpoint_nk,
                code,
                asset_type,
                last_success_trade_date,
                last_observed_trade_date,
                last_success_run_id,
                last_response_digest,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE}
        SET
            last_success_trade_date = ?,
            last_observed_trade_date = ?,
            last_success_run_id = ?,
            last_response_digest = ?,
            updated_at_utc = CURRENT_TIMESTAMP
        WHERE checkpoint_nk = ?
        """,
        [
            last_success_trade_date,
            last_observed_trade_date,
            last_success_run_id,
            last_response_digest,
            checkpoint_nk,
        ],
    )


def _insert_raw_tdxquant_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    strategy_path: Path,
    scope_source: str,
    requested_end_trade_date: date,
    requested_count: int,
    candidate_instrument_count: int,
) -> None:
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {RAW_TDXQUANT_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            strategy_path,
            scope_source,
            requested_end_trade_date,
            requested_count,
            candidate_instrument_count,
            processed_instrument_count,
            successful_request_count,
            failed_request_count,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            dirty_mark_count,
            run_status,
            started_at_utc,
            finished_at_utc,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 'running', CURRENT_TIMESTAMP, NULL, NULL)
        """,
        [
            run_id,
            TDXQUANT_DAILY_RAW_SYNC_RUNNER_NAME,
            TDXQUANT_DAILY_RAW_SYNC_RUNNER_VERSION,
            str(strategy_path),
            scope_source,
            requested_end_trade_date,
            requested_count,
            candidate_instrument_count,
        ],
    )


def _update_raw_tdxquant_run_success(
    connection: duckdb.DuckDBPyConnection,
    *,
    summary: TdxQuantDailyRawSyncSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {RAW_TDXQUANT_RUN_TABLE}
        SET
            processed_instrument_count = ?,
            successful_request_count = ?,
            failed_request_count = ?,
            inserted_bar_count = ?,
            reused_bar_count = ?,
            rematerialized_bar_count = ?,
            dirty_mark_count = ?,
            run_status = 'completed',
            finished_at_utc = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            summary.processed_instrument_count,
            summary.successful_request_count,
            summary.failed_request_count,
            summary.inserted_bar_count,
            summary.reused_bar_count,
            summary.rematerialized_bar_count,
            summary.dirty_mark_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            summary.run_id,
        ],
    )


def _update_raw_tdxquant_run_failure(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    strategy_path: Path,
    scope_source: str,
    requested_end_trade_date: date,
    requested_count: int,
    candidate_instrument_count: int,
    processed_instrument_count: int,
    successful_request_count: int,
    failed_request_count: int,
    inserted_bar_count: int,
    reused_bar_count: int,
    rematerialized_bar_count: int,
    dirty_mark_count: int,
    error_message: str,
) -> None:
    summary_payload = {
        "run_id": run_id,
        "strategy_path": str(strategy_path),
        "scope_source": scope_source,
        "requested_end_trade_date": requested_end_trade_date.isoformat(),
        "requested_count": requested_count,
        "candidate_instrument_count": candidate_instrument_count,
        "processed_instrument_count": processed_instrument_count,
        "successful_request_count": successful_request_count,
        "failed_request_count": failed_request_count,
        "inserted_bar_count": inserted_bar_count,
        "reused_bar_count": reused_bar_count,
        "rematerialized_bar_count": rematerialized_bar_count,
        "dirty_mark_count": dirty_mark_count,
        "error_message": error_message,
    }
    connection.execute(
        f"""
        UPDATE {RAW_TDXQUANT_RUN_TABLE}
        SET
            strategy_path = ?,
            scope_source = ?,
            requested_end_trade_date = ?,
            requested_count = ?,
            candidate_instrument_count = ?,
            processed_instrument_count = ?,
            successful_request_count = ?,
            failed_request_count = ?,
            inserted_bar_count = ?,
            reused_bar_count = ?,
            rematerialized_bar_count = ?,
            dirty_mark_count = ?,
            run_status = 'failed',
            finished_at_utc = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            str(strategy_path),
            scope_source,
            requested_end_trade_date,
            requested_count,
            candidate_instrument_count,
            processed_instrument_count,
            successful_request_count,
            failed_request_count,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            dirty_mark_count,
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def _normalize_raw_run_mode(run_mode: str) -> str:
    normalized = str(run_mode).strip().lower()
    if normalized not in {"incremental", "full"}:
        raise ValueError(f"Unsupported raw run mode: {run_mode}")
    return normalized


def _resolve_raw_candidate_files(
    connection: duckdb.DuckDBPyConnection,
    *,
    adjust_method: str,
    source_root: Path,
    candidate_files: list[Path],
    continue_from_last_run: bool,
) -> list[Path]:
    if not continue_from_last_run:
        return candidate_files
    last_failed_run = connection.execute(
        f"""
        SELECT run_id
        FROM {RAW_INGEST_RUN_TABLE}
        WHERE adjust_method = ?
          AND source_root = ?
          AND run_status = 'failed'
        ORDER BY started_at DESC NULLS LAST, run_id DESC
        LIMIT 1
        """,
        [adjust_method, str(source_root)],
    ).fetchone()
    if last_failed_run is None:
        return candidate_files
    completed_source_paths = {
        str(row[0])
        for row in connection.execute(
            f"""
            SELECT source_path
            FROM {RAW_INGEST_FILE_TABLE}
            WHERE run_id = ?
              AND action <> 'failed'
            """,
            [str(last_failed_run[0])],
        ).fetchall()
    }
    if not completed_source_paths:
        return candidate_files
    return [path for path in candidate_files if str(path) not in completed_source_paths]
    

def _should_skip_raw_file(
    *,
    path: Path,
    source_size_bytes: int,
    source_mtime_utc: datetime,
    registry_row: tuple[object, ...],
    force_hash: bool,
) -> bool:
    registry_size_bytes = int(registry_row[0])
    registry_mtime_utc = _normalize_timestamp(registry_row[1])
    same_size = registry_size_bytes == source_size_bytes
    same_mtime = registry_mtime_utc == source_mtime_utc
    if same_size and same_mtime and not force_hash:
        return True
    if not same_size and not force_hash:
        return False
    if same_size and not same_mtime or force_hash:
        stored_hash = None if registry_row[5] is None else str(registry_row[5])
        if not stored_hash:
            return False
        return stored_hash == _compute_file_content_hash(path)
    return False


def _compute_file_content_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _insert_raw_ingest_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    adjust_method: str,
    run_mode: str,
    source_root: Path,
    candidate_file_count: int,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {RAW_INGEST_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            adjust_method,
            run_mode,
            source_root,
            candidate_file_count,
            run_status,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'running', NULL)
        """,
        [
            run_id,
            RAW_INGEST_RUNNER_NAME,
            RAW_INGEST_RUNNER_VERSION,
            adjust_method,
            run_mode,
            str(source_root),
            candidate_file_count,
        ],
    )


def _record_raw_ingest_file(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    file_nk: str,
    code: str,
    name: str,
    adjust_method: str,
    source_path: Path,
    fingerprint_mode: str,
    action: str,
    row_count: int,
    error_message: str | None,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {RAW_INGEST_FILE_TABLE} (
            run_id,
            file_nk,
            code,
            name,
            adjust_method,
            source_path,
            fingerprint_mode,
            action,
            row_count,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            file_nk,
            code,
            name,
            adjust_method,
            str(source_path),
            fingerprint_mode,
            action,
            row_count,
            error_message,
        ],
    )


def _resolve_raw_file_action(
    *,
    inserted_count: int,
    reused_count: int,
    rematerialized_count: int,
) -> str:
    if rematerialized_count > 0:
        return "rematerialized"
    if inserted_count > 0:
        return "inserted"
    if reused_count > 0:
        return "reused"
    return "reused"


def _update_raw_ingest_run_success(
    connection: duckdb.DuckDBPyConnection,
    *,
    summary: TdxStockRawIngestSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {RAW_INGEST_RUN_TABLE}
        SET
            processed_file_count = ?,
            skipped_file_count = ?,
            inserted_bar_count = ?,
            reused_bar_count = ?,
            rematerialized_bar_count = ?,
            run_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            summary.processed_file_count,
            summary.skipped_unchanged_file_count,
            summary.bar_inserted_count,
            summary.bar_reused_count,
            summary.bar_rematerialized_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            summary.run_id,
        ],
    )


def _update_raw_ingest_run_failure(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    adjust_method: str,
    run_mode: str,
    source_root: Path,
    candidate_file_count: int,
    processed_file_count: int,
    skipped_file_count: int,
    inserted_bar_count: int,
    reused_bar_count: int,
    rematerialized_bar_count: int,
    failed_file_count: int,
    error_message: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {RAW_INGEST_RUN_TABLE}
        SET
            adjust_method = ?,
            run_mode = ?,
            source_root = ?,
            candidate_file_count = ?,
            processed_file_count = ?,
            skipped_file_count = ?,
            inserted_bar_count = ?,
            reused_bar_count = ?,
            rematerialized_bar_count = ?,
            run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            adjust_method,
            run_mode,
            str(source_root),
            candidate_file_count,
            processed_file_count,
            skipped_file_count,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            json.dumps(
                {
                    "error_message": error_message,
                    "failed_file_count": failed_file_count,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            run_id,
        ],
    )


def _normalize_limit(limit: int | None) -> int | None:
    if limit is None:
        return None
    normalized = int(limit)
    if normalized <= 0:
        return None
    return normalized


def _normalize_instruments(instruments: list[str] | tuple[str, ...] | None) -> set[str]:
    normalized: set[str] = set()
    for instrument in instruments or ():
        candidate = str(instrument).strip().upper()
        if not candidate:
            continue
        normalized.add(candidate)
        if "." in candidate:
            normalized.add(candidate.split(".", 1)[0])
    return normalized


def _normalize_build_mode(build_mode: str) -> str:
    normalized = str(build_mode).strip().lower()
    if normalized not in {"full", "incremental"}:
        raise ValueError(f"Unsupported build mode: {build_mode}")
    return normalized


def _resolve_consume_dirty_only(*, build_mode: str, consume_dirty_only: bool | None) -> bool:
    if consume_dirty_only is None:
        return build_mode == "incremental"
    return bool(consume_dirty_only)


def _resolve_initial_scope_kind(
    *,
    build_mode: str,
    instruments: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
    consume_dirty_only: bool,
) -> str:
    if build_mode == "incremental" and consume_dirty_only:
        return "dirty_queue"
    if instruments:
        return "instrument"
    if start_date is not None or end_date is not None:
        return "date_range"
    return "full"


def _resolve_market_base_stage_limit(*, source_scope_kind: str, limit: int | None) -> int | None:
    # dirty_queue 必须消费脏标的完整历史窗口，不能被全局 row limit 截断。
    if source_scope_kind == "dirty_queue":
        return None
    return limit


def _resolve_base_build_scope_plan(
    connection: duckdb.DuckDBPyConnection,
    *,
    adjust_method: str,
    build_mode: str,
    consume_dirty_only: bool,
    instruments: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
    limit: int | None,
) -> BaseBuildScopePlan:
    if build_mode == "incremental" and consume_dirty_only:
        dirty_entries = _fetch_pending_dirty_entries(
            connection,
            adjust_method=adjust_method,
            instruments=instruments,
            limit=limit,
        )
        scope_records = tuple(
            (
                "dirty_queue",
                json.dumps(
                    {
                        "dirty_nk": entry.dirty_nk,
                        "code": entry.code,
                        "dirty_reason": entry.dirty_reason,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
            for entry in dirty_entries
        )
        return BaseBuildScopePlan(
            source_scope_kind="dirty_queue",
            instruments=tuple(sorted({entry.code for entry in dirty_entries})),
            scope_records=scope_records or (("dirty_queue", "[]"),),
            dirty_entries=dirty_entries,
            scope_is_empty=not dirty_entries,
        )

    scope_records: list[tuple[str, str]] = []
    if instruments:
        scope_records.extend(("instrument", instrument) for instrument in instruments)
    if start_date is not None or end_date is not None:
        scope_records.append(
            (
                "date_range",
                json.dumps(
                    {
                        "start_date": start_date.isoformat() if start_date is not None else None,
                        "end_date": end_date.isoformat() if end_date is not None else None,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
    if not scope_records:
        scope_records.append(("full", adjust_method))
    return BaseBuildScopePlan(
        source_scope_kind=_resolve_initial_scope_kind(
            build_mode=build_mode,
            instruments=instruments,
            start_date=start_date,
            end_date=end_date,
            consume_dirty_only=consume_dirty_only,
        ),
        instruments=instruments,
        scope_records=tuple(scope_records),
        dirty_entries=(),
        scope_is_empty=False,
    )


def _fetch_pending_dirty_entries(
    connection: duckdb.DuckDBPyConnection,
    *,
    adjust_method: str,
    instruments: tuple[str, ...],
    limit: int | None,
) -> tuple[BaseDirtyInstrumentEntry, ...]:
    parameters: list[object] = [adjust_method]
    where_clauses = ["adjust_method = ?", "dirty_status = 'pending'"]
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"code IN ({placeholders})")
        parameters.extend(instruments)
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        parameters.append(limit)
    rows = connection.execute(
        f"""
        SELECT dirty_nk, code, adjust_method, dirty_reason, source_run_id, source_file_nk
        FROM {BASE_DIRTY_INSTRUMENT_TABLE}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY last_marked_at ASC, first_marked_at ASC, code ASC
        {limit_sql}
        """,
        parameters,
    ).fetchall()
    return tuple(
        BaseDirtyInstrumentEntry(
            dirty_nk=str(row[0]),
            code=str(row[1]),
            adjust_method=str(row[2]),
            dirty_reason=str(row[3]),
            source_run_id=None if row[4] is None else str(row[4]),
            source_file_nk=None if row[5] is None else str(row[5]),
        )
        for row in rows
    )


def _record_base_build_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    scope_records: tuple[tuple[str, str], ...],
) -> None:
    if not scope_records:
        return
    frame = pd.DataFrame.from_records(
        [
            {
                "run_id": run_id,
                "scope_type": scope_type,
                "scope_value": scope_value,
            }
            for scope_type, scope_value in scope_records
        ]
    )
    relation_name = "stage_base_build_scope"
    connection.register(relation_name, frame)
    try:
        connection.execute(
            f"""
            INSERT INTO {BASE_BUILD_SCOPE_TABLE} (
                run_id,
                scope_type,
                scope_value
            )
            SELECT
                run_id,
                scope_type,
                scope_value
            FROM {relation_name}
            """
        )
    finally:
        connection.unregister(relation_name)


def _stage_market_base_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    adjust_method: str,
    instruments: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
    limit: int | None,
    force_empty_result: bool,
) -> None:
    parameters: list[object] = [adjust_method]
    where_clauses = ["adjust_method = ?"]
    if force_empty_result:
        where_clauses.append("1 = 0")
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
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        parameters.append(limit)
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {MARKET_BASE_STAGE_TABLE} AS
        SELECT
            code || '|' || CAST(trade_date AS VARCHAR) || '|' || adjust_method AS daily_bar_nk,
            code,
            COALESCE(name, code) AS name,
            trade_date,
            adjust_method,
            open,
            high,
            low,
            close,
            volume,
            amount,
            bar_nk AS source_bar_nk
        FROM raw_source.{RAW_STOCK_DAILY_BAR_TABLE}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY code, trade_date
        {limit_sql}
        """,
        parameters,
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {MARKET_BASE_EXISTING_STAGE_TABLE} AS
        SELECT
            daily_bar_nk,
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
            source_bar_nk,
            first_seen_run_id,
            created_at,
            updated_at
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY code, trade_date, adjust_method
                    ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                ) AS row_number_in_key
            FROM {MARKET_BASE_STOCK_DAILY_TABLE}
        )
        WHERE row_number_in_key = 1
        """
    )


def _count_market_base_actions(connection: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    reused_condition = _build_market_base_reused_condition(stage_alias="stage", existing_alias="existing")
    inserted_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {MARKET_BASE_STAGE_TABLE} AS stage
            LEFT JOIN {MARKET_BASE_EXISTING_STAGE_TABLE} AS existing
              ON existing.code = stage.code
             AND existing.trade_date = stage.trade_date
             AND existing.adjust_method = stage.adjust_method
            WHERE existing.code IS NULL
            """
        ).fetchone()[0]
    )
    reused_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {MARKET_BASE_STAGE_TABLE} AS stage
            JOIN {MARKET_BASE_EXISTING_STAGE_TABLE} AS existing
              ON existing.code = stage.code
             AND existing.trade_date = stage.trade_date
             AND existing.adjust_method = stage.adjust_method
            WHERE {reused_condition}
            """
        ).fetchone()[0]
    )
    rematerialized_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {MARKET_BASE_STAGE_TABLE} AS stage
            JOIN {MARKET_BASE_EXISTING_STAGE_TABLE} AS existing
              ON existing.code = stage.code
             AND existing.trade_date = stage.trade_date
             AND existing.adjust_method = stage.adjust_method
            WHERE NOT ({reused_condition})
            """
        ).fetchone()[0]
    )
    return inserted_count, reused_count, rematerialized_count


def _stage_market_base_action_rows(connection: duckdb.DuckDBPyConnection) -> None:
    reused_condition = _build_market_base_reused_condition(stage_alias="stage", existing_alias="existing")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {MARKET_BASE_ACTION_STAGE_TABLE} AS
        SELECT
            stage.code,
            stage.adjust_method,
            CASE
                WHEN SUM(
                    CASE
                        WHEN existing.code IS NOT NULL AND NOT ({reused_condition}) THEN 1
                        ELSE 0
                    END
                ) > 0 THEN 'rematerialized'
                WHEN SUM(CASE WHEN existing.code IS NULL THEN 1 ELSE 0 END) > 0 THEN 'inserted'
                ELSE 'reused'
            END AS action,
            COUNT(*) AS row_count
        FROM {MARKET_BASE_STAGE_TABLE} AS stage
        LEFT JOIN {MARKET_BASE_EXISTING_STAGE_TABLE} AS existing
          ON existing.code = stage.code
         AND existing.trade_date = stage.trade_date
         AND existing.adjust_method = stage.adjust_method
        GROUP BY stage.code, stage.adjust_method
        """
    )


def _record_base_build_actions(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {BASE_BUILD_ACTION_TABLE} (
            run_id,
            code,
            adjust_method,
            action,
            row_count
        )
        SELECT
            ? AS run_id,
            code,
            adjust_method,
            action,
            row_count
        FROM {MARKET_BASE_ACTION_STAGE_TABLE}
        """,
        [run_id],
    )


def _materialize_market_base_stage(
    connection: duckdb.DuckDBPyConnection,
    *,
    adjust_method: str,
    run_id: str,
    full_scope: bool,
) -> None:
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {MARKET_BASE_FINAL_STAGE_TABLE} AS
        SELECT
            stage.daily_bar_nk,
            stage.code,
            stage.name,
            stage.trade_date,
            stage.adjust_method,
            stage.open,
            stage.high,
            stage.low,
            stage.close,
            stage.volume,
            stage.amount,
            stage.source_bar_nk,
            COALESCE(existing.first_seen_run_id, ?) AS first_seen_run_id,
            ? AS last_materialized_run_id,
            COALESCE(existing.created_at, CURRENT_TIMESTAMP) AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM {MARKET_BASE_STAGE_TABLE} AS stage
        LEFT JOIN {MARKET_BASE_EXISTING_STAGE_TABLE} AS existing
          ON existing.code = stage.code
         AND existing.trade_date = stage.trade_date
         AND existing.adjust_method = stage.adjust_method
        """,
        [run_id, run_id],
    )
    connection.execute(
        f"""
        MERGE INTO {MARKET_BASE_STOCK_DAILY_TABLE} AS target
        USING {MARKET_BASE_FINAL_STAGE_TABLE} AS source
          ON target.code = source.code
         AND target.trade_date = source.trade_date
         AND target.adjust_method = source.adjust_method
        WHEN MATCHED THEN UPDATE SET
            daily_bar_nk = source.daily_bar_nk,
            code = source.code,
            name = source.name,
            trade_date = source.trade_date,
            adjust_method = source.adjust_method,
            open = source.open,
            high = source.high,
            low = source.low,
            close = source.close,
            volume = source.volume,
            amount = source.amount,
            source_bar_nk = source.source_bar_nk,
            first_seen_run_id = source.first_seen_run_id,
            last_materialized_run_id = source.last_materialized_run_id,
            created_at = source.created_at,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            daily_bar_nk,
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
            source_bar_nk,
            first_seen_run_id,
            last_materialized_run_id,
            created_at,
            updated_at
        ) VALUES (
            source.daily_bar_nk,
            source.code,
            source.name,
            source.trade_date,
            source.adjust_method,
            source.open,
            source.high,
            source.low,
            source.close,
            source.volume,
            source.amount,
            source.source_bar_nk,
            source.first_seen_run_id,
            source.last_materialized_run_id,
            source.created_at,
            source.updated_at
        )
        """
    )
    if full_scope:
        connection.execute(
            f"""
            DELETE FROM {MARKET_BASE_STOCK_DAILY_TABLE} AS target
            WHERE target.adjust_method = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM {MARKET_BASE_FINAL_STAGE_TABLE} AS source
                  WHERE source.code = target.code
                    AND source.trade_date = target.trade_date
                    AND source.adjust_method = target.adjust_method
              )
            """,
            [adjust_method],
        )


def _insert_base_build_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    adjust_method: str,
    build_mode: str,
    source_scope_kind: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {BASE_BUILD_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            adjust_method,
            build_mode,
            source_scope_kind,
            run_status,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, 'running', NULL)
        """,
        [
            run_id,
            BASE_BUILD_RUNNER_NAME,
            BASE_BUILD_RUNNER_VERSION,
            adjust_method,
            build_mode,
            source_scope_kind,
        ],
    )


def _update_base_build_run_success(
    connection: duckdb.DuckDBPyConnection,
    *,
    summary: MarketBaseBuildSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {BASE_BUILD_RUN_TABLE}
        SET
            source_scope_kind = ?,
            source_row_count = ?,
            inserted_count = ?,
            reused_count = ?,
            rematerialized_count = ?,
            consumed_dirty_count = ?,
            run_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            summary.source_scope_kind,
            summary.source_row_count,
            summary.inserted_count,
            summary.reused_count,
            summary.rematerialized_count,
            summary.consumed_dirty_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            summary.run_id,
        ],
    )


def _update_base_build_run_failure(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    error_message: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {BASE_BUILD_RUN_TABLE}
        SET
            run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            json.dumps({"error_message": error_message}, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def _mark_dirty_entries_consumed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    dirty_entries: tuple[BaseDirtyInstrumentEntry, ...],
) -> None:
    if not dirty_entries:
        return
    dirty_nks = [entry.dirty_nk for entry in dirty_entries]
    placeholders = ", ".join("?" for _ in dirty_nks)
    connection.execute(
        f"""
        UPDATE {BASE_DIRTY_INSTRUMENT_TABLE}
        SET
            dirty_status = 'consumed',
            last_consumed_run_id = ?,
            last_marked_at = CURRENT_TIMESTAMP
        WHERE dirty_nk IN ({placeholders})
        """,
        [run_id, *dirty_nks],
    )


def _mark_scope_dirty_entries_consumed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    adjust_method: str,
    instruments: tuple[str, ...],
) -> int:
    parameters: list[object] = [run_id, adjust_method]
    where_clauses = ["adjust_method = ?", "dirty_status = 'pending'"]
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"code IN ({placeholders})")
        parameters.extend(instruments)
    updated_rows = connection.execute(
        f"""
        UPDATE {BASE_DIRTY_INSTRUMENT_TABLE}
        SET
            dirty_status = 'consumed',
            last_consumed_run_id = ?,
            last_marked_at = CURRENT_TIMESTAMP
        WHERE {' AND '.join(where_clauses)}
        RETURNING dirty_nk
        """,
        parameters,
    ).fetchall()
    return len(updated_rows)


def _build_market_base_reused_condition(*, stage_alias: str, existing_alias: str) -> str:
    comparisons = [
        f"COALESCE({stage_alias}.name, '') = COALESCE({existing_alias}.name, '')",
        f"COALESCE({stage_alias}.open, -1e308) = COALESCE({existing_alias}.open, -1e308)",
        f"COALESCE({stage_alias}.high, -1e308) = COALESCE({existing_alias}.high, -1e308)",
        f"COALESCE({stage_alias}.low, -1e308) = COALESCE({existing_alias}.low, -1e308)",
        f"COALESCE({stage_alias}.close, -1e308) = COALESCE({existing_alias}.close, -1e308)",
        f"COALESCE({stage_alias}.volume, -1e308) = COALESCE({existing_alias}.volume, -1e308)",
        f"COALESCE({stage_alias}.amount, -1e308) = COALESCE({existing_alias}.amount, -1e308)",
        f"COALESCE({stage_alias}.source_bar_nk, '') = COALESCE({existing_alias}.source_bar_nk, '')",
    ]
    return " AND ".join(comparisons)


def _match_instrument_filter(path: Path, normalized_instruments: set[str]) -> bool:
    if not normalized_instruments:
        return True
    stem = path.stem
    if "#" not in stem:
        return False
    exchange, code = stem.split("#", 1)
    return code.upper() in normalized_instruments or f"{code}.{exchange}".upper() in normalized_instruments


def _resolve_code_from_filename(path: Path) -> str:
    stem = path.stem
    if "#" not in stem:
        raise ValueError(f"Unexpected TDX file name: {path.name}")
    exchange, code = stem.split("#", 1)
    return f"{code}.{exchange}"


def _build_file_nk(
    *,
    asset_type: str,
    adjust_method: str,
    code: str,
    name: str,
    source_path: Path,
) -> str:
    return "|".join([asset_type, adjust_method, code, name, str(source_path)])


def _build_bar_nk(*, code: str, trade_date: date, adjust_method: str) -> str:
    return "|".join([code, trade_date.isoformat(), adjust_method])


def _build_dirty_nk(*, code: str, adjust_method: str) -> str:
    return "|".join([code, adjust_method])


def _upsert_file_registry(
    connection: duckdb.DuckDBPyConnection,
    *,
    file_nk: str,
    adjust_method: str,
    parsed_name: str,
    parsed_code: str,
    source_path: Path,
    source_size_bytes: int,
    source_mtime_utc: datetime,
    source_line_count: int,
    source_header: str,
    source_content_hash: str | None,
    run_id: str,
) -> None:
    existing = connection.execute(
        f"""
        SELECT file_nk
        FROM {RAW_STOCK_FILE_REGISTRY_TABLE}
        WHERE asset_type = ?
          AND adjust_method = ?
          AND code = ?
          AND source_path = ?
        """,
        [DEFAULT_ASSET_TYPE, adjust_method, parsed_code, str(source_path)],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {RAW_STOCK_FILE_REGISTRY_TABLE} (
                file_nk,
                asset_type,
                adjust_method,
                code,
                name,
                source_path,
                source_size_bytes,
                source_mtime_utc,
                source_line_count,
                source_header,
                source_content_hash,
                last_ingested_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                file_nk,
                DEFAULT_ASSET_TYPE,
                adjust_method,
                parsed_code,
                parsed_name,
                str(source_path),
                source_size_bytes,
                source_mtime_utc,
                source_line_count,
                source_header,
                source_content_hash,
                run_id,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {RAW_STOCK_FILE_REGISTRY_TABLE}
        SET
            code = ?,
            file_nk = ?,
            name = ?,
            source_path = ?,
            source_size_bytes = ?,
            source_mtime_utc = ?,
            source_line_count = ?,
            source_header = ?,
            source_content_hash = ?,
            last_ingested_run_id = ?,
            last_ingested_at = CURRENT_TIMESTAMP
        WHERE file_nk = ?
        """,
        [
            parsed_code,
            file_nk,
            parsed_name,
            str(source_path),
            source_size_bytes,
            source_mtime_utc,
            source_line_count,
            source_header,
            source_content_hash,
            run_id,
            str(existing[0]),
        ],
    )


def _refresh_file_registry_fingerprint(
    connection: duckdb.DuckDBPyConnection,
    *,
    file_nk: str,
    source_size_bytes: int,
    source_mtime_utc: datetime,
    source_content_hash: str | None,
) -> None:
    connection.execute(
        f"""
        UPDATE {RAW_STOCK_FILE_REGISTRY_TABLE}
        SET
            source_size_bytes = ?,
            source_mtime_utc = ?,
            source_content_hash = ?
        WHERE file_nk = ?
        """,
        [
            source_size_bytes,
            source_mtime_utc,
            source_content_hash,
            file_nk,
        ],
    )


def _replace_raw_bars_for_file(
    connection: duckdb.DuckDBPyConnection,
    *,
    file_nk: str,
    adjust_method: str,
    parsed,
    source_path: Path,
    source_mtime_utc: datetime,
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
          AND adjust_method = ?
        """,
        [parsed.code, adjust_method],
    ).fetchall()
    existing_by_bar_nk = {str(row[0]): row for row in existing_rows}
    now = datetime.now().replace(microsecond=0)
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    records: list[dict[str, object]] = []
    for row in parsed.rows:
        bar_nk = _build_bar_nk(code=row.code, trade_date=row.trade_date, adjust_method=adjust_method)
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
                "source_file_nk": file_nk,
                "asset_type": DEFAULT_ASSET_TYPE,
                "code": row.code,
                "name": row.name,
                "trade_date": row.trade_date,
                "adjust_method": adjust_method,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
                "amount": row.amount,
                "source_path": str(source_path),
                "source_mtime_utc": source_mtime_utc,
                "first_seen_run_id": first_seen_run_id,
                "last_ingested_run_id": run_id,
                "created_at": created_at,
                "updated_at": now,
            }
        )
    if records:
        frame = pd.DataFrame.from_records(records)
        connection.register(RAW_STAGE_RELATION_NAME, frame)
        try:
            connection.execute(
                f"""
                DELETE FROM {RAW_STOCK_DAILY_BAR_TABLE}
                WHERE code = ?
                  AND adjust_method = ?
                  AND bar_nk NOT IN (
                      SELECT bar_nk
                      FROM {RAW_STAGE_RELATION_NAME}
                  )
                """,
                [parsed.code, adjust_method],
            )
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
    else:
        connection.execute(
            f"""
            DELETE FROM {RAW_STOCK_DAILY_BAR_TABLE}
            WHERE code = ?
              AND adjust_method = ?
            """,
            [parsed.code, adjust_method],
        )
    return inserted_count, reused_count, rematerialized_count


def _attach_raw_market_ledger(
    connection: duckdb.DuckDBPyConnection,
    *,
    raw_market_path: Path,
) -> None:
    normalized_path = str(raw_market_path).replace("\\", "/")
    connection.execute(f"ATTACH '{normalized_path}' AS raw_source")


def _coerce_date(value: str | date | datetime | object | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(microsecond=0)
        return value.astimezone().replace(tzinfo=None, microsecond=0)
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(microsecond=0)
    return parsed.astimezone().replace(tzinfo=None, microsecond=0)


def _normalize_float(value: object | None) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _build_run_id(*, prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"


def _write_summary(payload: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
