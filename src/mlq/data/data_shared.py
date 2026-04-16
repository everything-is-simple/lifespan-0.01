"""`data` 模块的共享常量、摘要对象与通用状态对象。"""

from __future__ import annotations

import hashlib
import json
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
    MARKET_BASE_BLOCK_DAILY_TABLE,
    MARKET_BASE_DAILY_TABLE_BY_ASSET_TYPE,
    MARKET_BASE_TABLE_BY_ASSET_AND_TIMEFRAME,
    MARKET_BASE_INDEX_DAILY_TABLE,
    MARKET_BASE_STOCK_DAILY_TABLE,
    RAW_BLOCK_DAILY_BAR_TABLE,
    RAW_BLOCK_FILE_REGISTRY_TABLE,
    RAW_BAR_TABLE_BY_ASSET_AND_TIMEFRAME,
    RAW_DAILY_BAR_TABLE_BY_ASSET_TYPE,
    RAW_FILE_REGISTRY_TABLE_BY_ASSET_TYPE,
    RAW_INDEX_DAILY_BAR_TABLE,
    RAW_INDEX_FILE_REGISTRY_TABLE,
    RAW_INGEST_FILE_TABLE,
    RAW_INGEST_RUN_TABLE,
    RAW_STOCK_DAILY_BAR_TABLE,
    RAW_STOCK_FILE_REGISTRY_TABLE,
    TUSHARE_OBJECTIVE_CHECKPOINT_TABLE,
    TUSHARE_OBJECTIVE_EVENT_TABLE,
    TUSHARE_OBJECTIVE_REQUEST_TABLE,
    TUSHARE_OBJECTIVE_RUN_TABLE,
    OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE,
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_PROFILE_TABLE,
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_TABLE,
    RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE,
    RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE,
    RAW_TDXQUANT_REQUEST_TABLE,
    RAW_TDXQUANT_RUN_TABLE,
    TDX_ASSET_TYPES,
    TDX_TIMEFRAMES,
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
RAW_INGEST_RUNNER_NAME_BY_ASSET_TYPE: Final[dict[str, str]] = {
    "stock": "run_tdx_stock_raw_ingest",
    "index": "run_tdx_index_raw_ingest",
    "block": "run_tdx_block_raw_ingest",
}
RAW_STAGE_RELATION_NAME: Final[str] = "_raw_stock_daily_stage"
MARKET_BASE_STAGE_TABLE: Final[str] = "stage_market_base"
MARKET_BASE_EXISTING_STAGE_TABLE: Final[str] = "stage_market_base_existing"
MARKET_BASE_FINAL_STAGE_TABLE: Final[str] = "stage_market_base_final"
MARKET_BASE_ACTION_STAGE_TABLE: Final[str] = "stage_market_base_action"
RAW_INGEST_RUNNER_NAME: Final[str] = "run_tdx_stock_raw_ingest"
RAW_INGEST_RUNNER_VERSION: Final[str] = "2026-04-10-card17-slice5"
TDXQUANT_DAILY_RAW_SYNC_RUNNER_NAME: Final[str] = "run_tdxquant_daily_raw_sync"
TDXQUANT_DAILY_RAW_SYNC_RUNNER_VERSION: Final[str] = "2026-04-10-card19-slice2"
TUSHARE_OBJECTIVE_SOURCE_SYNC_RUNNER_NAME: Final[str] = "run_tushare_objective_source_sync"
TUSHARE_OBJECTIVE_SOURCE_SYNC_RUNNER_VERSION: Final[str] = "2026-04-15-card71-slice1"
OBJECTIVE_PROFILE_MATERIALIZATION_RUNNER_NAME: Final[str] = "run_tushare_objective_profile_materialization"
OBJECTIVE_PROFILE_MATERIALIZATION_RUNNER_VERSION: Final[str] = "2026-04-15-card71-slice1"
BASE_BUILD_RUNNER_NAME: Final[str] = "run_market_base_build"
BASE_BUILD_RUNNER_VERSION: Final[str] = "2026-04-10-card17-slice1"


@dataclass(frozen=True)
class TdxStockRawIngestSummary:
    run_id: str
    asset_type: str
    timeframe: str
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
    asset_type: str
    timeframe: str
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
class TushareObjectiveSourceSyncSummary:
    run_id: str
    source_api_scope: tuple[str, ...]
    signal_start_date: str | None
    signal_end_date: str | None
    candidate_cursor_count: int
    processed_request_count: int
    successful_request_count: int
    failed_request_count: int
    inserted_event_count: int
    reused_event_count: int
    rematerialized_event_count: int
    raw_market_path: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ObjectiveProfileMaterializationSummary:
    run_id: str
    signal_start_date: str | None
    signal_end_date: str | None
    candidate_profile_count: int
    processed_profile_count: int
    inserted_profile_count: int
    reused_profile_count: int
    rematerialized_profile_count: int
    raw_market_path: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BaseDirtyInstrumentEntry:
    dirty_nk: str
    asset_type: str
    timeframe: str
    code: str
    adjust_method: str
    dirty_reason: str
    source_run_id: str | None
    source_file_nk: str | None


@dataclass(frozen=True)
class BaseBuildScopePlan:
    source_scope_kind: str
    asset_type: str
    instruments: tuple[str, ...]
    scope_records: tuple[tuple[str, str], ...]
    dirty_entries: tuple[BaseDirtyInstrumentEntry, ...]
    scope_is_empty: bool


def _normalize_asset_type(asset_type: str) -> str:
    normalized = str(asset_type).strip().lower()
    if normalized not in TDX_ASSET_TYPES:
        raise ValueError(f"Unsupported asset type: {asset_type}")
    return normalized


def _normalize_timeframe(timeframe: str | None) -> str:
    normalized = "day" if timeframe is None else str(timeframe).strip().lower()
    if normalized not in TDX_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return normalized


def _build_dirty_nk(*, code: str, adjust_method: str, timeframe: str = "day") -> str:
    normalized_timeframe = _normalize_timeframe(timeframe)
    if normalized_timeframe == "day":
        return "|".join([code, adjust_method])
    return "|".join([code, adjust_method, normalized_timeframe])


def _build_dirty_nk_by_asset(*, asset_type: str, code: str, adjust_method: str, timeframe: str = "day") -> str:
    normalized_asset_type = _normalize_asset_type(asset_type)
    normalized_timeframe = _normalize_timeframe(timeframe)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        return _build_dirty_nk(code=code, adjust_method=adjust_method, timeframe=normalized_timeframe)
    if normalized_timeframe == "day":
        return "|".join([normalized_asset_type, code, adjust_method])
    return "|".join([normalized_asset_type, code, adjust_method, normalized_timeframe])


def _resolve_raw_bar_table(*, asset_type: str, timeframe: str) -> str:
    return RAW_BAR_TABLE_BY_ASSET_AND_TIMEFRAME[_normalize_asset_type(asset_type)][_normalize_timeframe(timeframe)]


def _resolve_market_base_table(*, asset_type: str, timeframe: str) -> str:
    return MARKET_BASE_TABLE_BY_ASSET_AND_TIMEFRAME[_normalize_asset_type(asset_type)][
        _normalize_timeframe(timeframe)
    ]


def mark_base_instrument_dirty(
    *,
    settings: WorkspaceRoots | None = None,
    code: str,
    timeframe: str = "day",
    adjust_method: str,
    dirty_reason: str,
    source_run_id: str | None = None,
    source_file_nk: str | None = None,
) -> str:
    """给 `base_dirty_instrument` 挂账或刷新待消费标的。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_market_base_ledger(workspace)
    normalized_code = str(code).strip().upper()
    if not normalized_code:
        raise ValueError("code must not be empty")
    normalized_timeframe = _normalize_timeframe(timeframe)
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
            timeframe=normalized_timeframe,
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
    timeframe: str,
    adjust_method: str,
    dirty_reason: str,
    source_run_id: str | None,
    source_file_nk: str | None,
) -> str:
    return _upsert_dirty_instrument_by_asset(
        connection,
        table_name=table_name,
        asset_type=DEFAULT_ASSET_TYPE,
        code=code,
        timeframe=timeframe,
        adjust_method=adjust_method,
        dirty_reason=dirty_reason,
        source_run_id=source_run_id,
        source_file_nk=source_file_nk,
    )


def _upsert_dirty_instrument_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    asset_type: str,
    code: str,
    timeframe: str,
    adjust_method: str,
    dirty_reason: str,
    source_run_id: str | None,
    source_file_nk: str | None,
) -> str:
    normalized_asset_type = _normalize_asset_type(asset_type)
    normalized_code = str(code).strip().upper()
    normalized_timeframe = _normalize_timeframe(timeframe)
    normalized_adjust_method = str(adjust_method).strip().lower()
    normalized_reason = str(dirty_reason).strip()
    dirty_nk = _build_dirty_nk_by_asset(
        asset_type=normalized_asset_type,
        code=normalized_code,
        adjust_method=normalized_adjust_method,
        timeframe=normalized_timeframe,
    )
    existing = connection.execute(
        f"SELECT dirty_nk FROM {table_name} WHERE dirty_nk = ?",
        [dirty_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {table_name} (
                dirty_nk,
                asset_type,
                timeframe,
                code,
                adjust_method,
                dirty_reason,
                source_run_id,
                source_file_nk,
                dirty_status,
                last_consumed_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL)
            """,
            [
                dirty_nk,
                normalized_asset_type,
                normalized_timeframe,
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
            asset_type = ?,
            timeframe = ?,
            dirty_reason = ?,
            source_run_id = ?,
            source_file_nk = ?,
            dirty_status = 'pending',
            last_marked_at = CURRENT_TIMESTAMP
        WHERE dirty_nk = ?
        """,
        [
            normalized_asset_type,
            normalized_timeframe,
            normalized_reason,
            source_run_id,
            source_file_nk,
            dirty_nk,
        ],
    )
    return dirty_nk


__all__ = [name for name in globals() if not name.startswith("__")]
