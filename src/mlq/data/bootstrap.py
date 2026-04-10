"""冻结 `data` 模块最小正式账本表结构。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


RAW_STOCK_FILE_REGISTRY_TABLE: Final[str] = "stock_file_registry"
RAW_STOCK_DAILY_BAR_TABLE: Final[str] = "stock_daily_bar"
RAW_INGEST_RUN_TABLE: Final[str] = "raw_ingest_run"
RAW_INGEST_FILE_TABLE: Final[str] = "raw_ingest_file"
RAW_TDXQUANT_RUN_TABLE: Final[str] = "raw_tdxquant_run"
RAW_TDXQUANT_REQUEST_TABLE: Final[str] = "raw_tdxquant_request"
RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE: Final[str] = "raw_tdxquant_instrument_checkpoint"
MARKET_BASE_STOCK_DAILY_TABLE: Final[str] = "stock_daily_adjusted"
BASE_DIRTY_INSTRUMENT_TABLE: Final[str] = "base_dirty_instrument"
BASE_BUILD_RUN_TABLE: Final[str] = "base_build_run"
BASE_BUILD_SCOPE_TABLE: Final[str] = "base_build_scope"
BASE_BUILD_ACTION_TABLE: Final[str] = "base_build_action"


RAW_MARKET_LEDGER_TABLES: Final[dict[str, str]] = {
    RAW_STOCK_FILE_REGISTRY_TABLE: """
        CREATE TABLE IF NOT EXISTS stock_file_registry (
            file_nk TEXT,
            asset_type TEXT NOT NULL,
            adjust_method TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            source_path TEXT NOT NULL,
            source_size_bytes BIGINT NOT NULL,
            source_mtime_utc TIMESTAMP NOT NULL,
            source_line_count BIGINT NOT NULL,
            source_header TEXT NOT NULL,
            source_content_hash TEXT,
            last_ingested_run_id TEXT NOT NULL,
            last_ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    RAW_STOCK_DAILY_BAR_TABLE: """
        CREATE TABLE IF NOT EXISTS stock_daily_bar (
            bar_nk TEXT,
            source_file_nk TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            trade_date DATE NOT NULL,
            adjust_method TEXT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            source_path TEXT NOT NULL,
            source_mtime_utc TIMESTAMP NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_ingested_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    RAW_INGEST_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS raw_ingest_run (
            run_id TEXT,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            adjust_method TEXT NOT NULL,
            run_mode TEXT NOT NULL,
            source_root TEXT NOT NULL,
            candidate_file_count BIGINT NOT NULL DEFAULT 0,
            processed_file_count BIGINT NOT NULL DEFAULT 0,
            skipped_file_count BIGINT NOT NULL DEFAULT 0,
            inserted_bar_count BIGINT NOT NULL DEFAULT 0,
            reused_bar_count BIGINT NOT NULL DEFAULT 0,
            rematerialized_bar_count BIGINT NOT NULL DEFAULT 0,
            run_status TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    RAW_INGEST_FILE_TABLE: """
        CREATE TABLE IF NOT EXISTS raw_ingest_file (
            run_id TEXT NOT NULL,
            file_nk TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            adjust_method TEXT NOT NULL,
            source_path TEXT NOT NULL,
            fingerprint_mode TEXT NOT NULL,
            action TEXT NOT NULL,
            row_count BIGINT NOT NULL DEFAULT 0,
            error_message TEXT,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    RAW_TDXQUANT_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS raw_tdxquant_run (
            run_id TEXT,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            strategy_path TEXT NOT NULL,
            scope_source TEXT NOT NULL,
            requested_end_trade_date DATE NOT NULL,
            requested_count BIGINT NOT NULL DEFAULT 0,
            candidate_instrument_count BIGINT NOT NULL DEFAULT 0,
            processed_instrument_count BIGINT NOT NULL DEFAULT 0,
            successful_request_count BIGINT NOT NULL DEFAULT 0,
            failed_request_count BIGINT NOT NULL DEFAULT 0,
            inserted_bar_count BIGINT NOT NULL DEFAULT 0,
            reused_bar_count BIGINT NOT NULL DEFAULT 0,
            rematerialized_bar_count BIGINT NOT NULL DEFAULT 0,
            dirty_mark_count BIGINT NOT NULL DEFAULT 0,
            run_status TEXT NOT NULL,
            started_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at_utc TIMESTAMP,
            summary_json TEXT
        )
    """,
    RAW_TDXQUANT_REQUEST_TABLE: """
        CREATE TABLE IF NOT EXISTS raw_tdxquant_request (
            request_nk TEXT,
            run_id TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            requested_dividend_type TEXT NOT NULL,
            requested_count BIGINT NOT NULL DEFAULT 0,
            requested_end_time TEXT NOT NULL,
            response_trade_date_min DATE,
            response_trade_date_max DATE,
            response_row_count BIGINT NOT NULL DEFAULT 0,
            response_digest TEXT,
            inserted_bar_count BIGINT NOT NULL DEFAULT 0,
            reused_bar_count BIGINT NOT NULL DEFAULT 0,
            rematerialized_bar_count BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            error_message TEXT,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS raw_tdxquant_instrument_checkpoint (
            checkpoint_nk TEXT,
            code TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            last_success_trade_date DATE,
            last_observed_trade_date DATE,
            last_success_run_id TEXT,
            last_response_digest TEXT,
            updated_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
}


MARKET_BASE_LEDGER_TABLES: Final[dict[str, str]] = {
    MARKET_BASE_STOCK_DAILY_TABLE: """
        CREATE TABLE IF NOT EXISTS stock_daily_adjusted (
            daily_bar_nk TEXT,
            code TEXT NOT NULL,
            name TEXT,
            trade_date DATE NOT NULL,
            adjust_method TEXT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            source_bar_nk TEXT,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    BASE_DIRTY_INSTRUMENT_TABLE: """
        CREATE TABLE IF NOT EXISTS base_dirty_instrument (
            dirty_nk TEXT,
            code TEXT NOT NULL,
            adjust_method TEXT NOT NULL,
            dirty_reason TEXT NOT NULL,
            source_run_id TEXT,
            source_file_nk TEXT,
            dirty_status TEXT NOT NULL DEFAULT 'pending',
            first_marked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_marked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_consumed_run_id TEXT
        )
    """,
    BASE_BUILD_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS base_build_run (
            run_id TEXT,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            adjust_method TEXT NOT NULL,
            build_mode TEXT NOT NULL,
            source_scope_kind TEXT NOT NULL,
            source_row_count BIGINT NOT NULL DEFAULT 0,
            inserted_count BIGINT NOT NULL DEFAULT 0,
            reused_count BIGINT NOT NULL DEFAULT 0,
            rematerialized_count BIGINT NOT NULL DEFAULT 0,
            consumed_dirty_count BIGINT NOT NULL DEFAULT 0,
            run_status TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    BASE_BUILD_SCOPE_TABLE: """
        CREATE TABLE IF NOT EXISTS base_build_scope (
            run_id TEXT NOT NULL,
            scope_type TEXT NOT NULL,
            scope_value TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    BASE_BUILD_ACTION_TABLE: """
        CREATE TABLE IF NOT EXISTS base_build_action (
            run_id TEXT NOT NULL,
            code TEXT NOT NULL,
            adjust_method TEXT NOT NULL,
            action TEXT NOT NULL,
            row_count BIGINT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """
}


RAW_MARKET_REQUIRED_COLUMNS: Final[dict[str, dict[str, str]]] = {
    RAW_STOCK_FILE_REGISTRY_TABLE: {
        "file_nk": "TEXT",
        "asset_type": "TEXT",
        "adjust_method": "TEXT",
        "code": "TEXT",
        "name": "TEXT",
        "source_path": "TEXT",
        "source_size_bytes": "BIGINT",
        "source_mtime_utc": "TIMESTAMP",
        "source_line_count": "BIGINT",
        "source_header": "TEXT",
        "source_content_hash": "TEXT",
        "last_ingested_run_id": "TEXT",
        "last_ingested_at": "TIMESTAMP",
    },
    RAW_STOCK_DAILY_BAR_TABLE: {
        "bar_nk": "TEXT",
        "source_file_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "name": "TEXT",
        "trade_date": "DATE",
        "adjust_method": "TEXT",
        "open": "DOUBLE",
        "high": "DOUBLE",
        "low": "DOUBLE",
        "close": "DOUBLE",
        "volume": "DOUBLE",
        "amount": "DOUBLE",
        "source_path": "TEXT",
        "source_mtime_utc": "TIMESTAMP",
        "first_seen_run_id": "TEXT",
        "last_ingested_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    RAW_INGEST_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "adjust_method": "TEXT",
        "run_mode": "TEXT",
        "source_root": "TEXT",
        "candidate_file_count": "BIGINT",
        "processed_file_count": "BIGINT",
        "skipped_file_count": "BIGINT",
        "inserted_bar_count": "BIGINT",
        "reused_bar_count": "BIGINT",
        "rematerialized_bar_count": "BIGINT",
        "run_status": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    RAW_INGEST_FILE_TABLE: {
        "run_id": "TEXT",
        "file_nk": "TEXT",
        "code": "TEXT",
        "name": "TEXT",
        "adjust_method": "TEXT",
        "source_path": "TEXT",
        "fingerprint_mode": "TEXT",
        "action": "TEXT",
        "row_count": "BIGINT",
        "error_message": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
    RAW_TDXQUANT_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "strategy_path": "TEXT",
        "scope_source": "TEXT",
        "requested_end_trade_date": "DATE",
        "requested_count": "BIGINT",
        "candidate_instrument_count": "BIGINT",
        "processed_instrument_count": "BIGINT",
        "successful_request_count": "BIGINT",
        "failed_request_count": "BIGINT",
        "inserted_bar_count": "BIGINT",
        "reused_bar_count": "BIGINT",
        "rematerialized_bar_count": "BIGINT",
        "dirty_mark_count": "BIGINT",
        "run_status": "TEXT",
        "started_at_utc": "TIMESTAMP",
        "finished_at_utc": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    RAW_TDXQUANT_REQUEST_TABLE: {
        "request_nk": "TEXT",
        "run_id": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "name": "TEXT",
        "requested_dividend_type": "TEXT",
        "requested_count": "BIGINT",
        "requested_end_time": "TEXT",
        "response_trade_date_min": "DATE",
        "response_trade_date_max": "DATE",
        "response_row_count": "BIGINT",
        "response_digest": "TEXT",
        "inserted_bar_count": "BIGINT",
        "reused_bar_count": "BIGINT",
        "rematerialized_bar_count": "BIGINT",
        "status": "TEXT",
        "error_message": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
    RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE: {
        "checkpoint_nk": "TEXT",
        "code": "TEXT",
        "asset_type": "TEXT",
        "last_success_trade_date": "DATE",
        "last_observed_trade_date": "DATE",
        "last_success_run_id": "TEXT",
        "last_response_digest": "TEXT",
        "updated_at_utc": "TIMESTAMP",
    },
}


MARKET_BASE_REQUIRED_COLUMNS: Final[dict[str, dict[str, str]]] = {
    MARKET_BASE_STOCK_DAILY_TABLE: {
        "daily_bar_nk": "TEXT",
        "code": "TEXT",
        "name": "TEXT",
        "trade_date": "DATE",
        "adjust_method": "TEXT",
        "open": "DOUBLE",
        "high": "DOUBLE",
        "low": "DOUBLE",
        "close": "DOUBLE",
        "volume": "DOUBLE",
        "amount": "DOUBLE",
        "source_bar_nk": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    BASE_DIRTY_INSTRUMENT_TABLE: {
        "dirty_nk": "TEXT",
        "code": "TEXT",
        "adjust_method": "TEXT",
        "dirty_reason": "TEXT",
        "source_run_id": "TEXT",
        "source_file_nk": "TEXT",
        "dirty_status": "TEXT",
        "first_marked_at": "TIMESTAMP",
        "last_marked_at": "TIMESTAMP",
        "last_consumed_run_id": "TEXT",
    },
    BASE_BUILD_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "adjust_method": "TEXT",
        "build_mode": "TEXT",
        "source_scope_kind": "TEXT",
        "source_row_count": "BIGINT",
        "inserted_count": "BIGINT",
        "reused_count": "BIGINT",
        "rematerialized_count": "BIGINT",
        "consumed_dirty_count": "BIGINT",
        "run_status": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    BASE_BUILD_SCOPE_TABLE: {
        "run_id": "TEXT",
        "scope_type": "TEXT",
        "scope_value": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
    BASE_BUILD_ACTION_TABLE: {
        "run_id": "TEXT",
        "code": "TEXT",
        "adjust_method": "TEXT",
        "action": "TEXT",
        "row_count": "BIGINT",
        "recorded_at": "TIMESTAMP",
    }
}


RAW_MARKET_NOT_NULL_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    RAW_STOCK_FILE_REGISTRY_TABLE: (
        "file_nk",
        "asset_type",
        "adjust_method",
        "code",
        "name",
        "source_path",
        "source_size_bytes",
        "source_mtime_utc",
        "source_line_count",
        "source_header",
        "last_ingested_run_id",
        "last_ingested_at",
    ),
    RAW_STOCK_DAILY_BAR_TABLE: (
        "bar_nk",
        "source_file_nk",
        "asset_type",
        "code",
        "name",
        "trade_date",
        "adjust_method",
        "source_path",
        "source_mtime_utc",
        "first_seen_run_id",
        "last_ingested_run_id",
        "created_at",
        "updated_at",
    ),
    RAW_TDXQUANT_RUN_TABLE: (
        "run_id",
        "runner_name",
        "runner_version",
        "strategy_path",
        "scope_source",
        "requested_end_trade_date",
        "requested_count",
        "candidate_instrument_count",
        "processed_instrument_count",
        "successful_request_count",
        "failed_request_count",
        "inserted_bar_count",
        "reused_bar_count",
        "rematerialized_bar_count",
        "dirty_mark_count",
        "run_status",
        "started_at_utc",
    ),
    RAW_TDXQUANT_REQUEST_TABLE: (
        "request_nk",
        "run_id",
        "asset_type",
        "code",
        "name",
        "requested_dividend_type",
        "requested_count",
        "requested_end_time",
        "response_row_count",
        "status",
        "recorded_at",
    ),
    RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE: (
        "checkpoint_nk",
        "code",
        "asset_type",
        "updated_at_utc",
    ),
}


MARKET_BASE_NOT_NULL_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    MARKET_BASE_STOCK_DAILY_TABLE: (
        "daily_bar_nk",
        "code",
        "trade_date",
        "adjust_method",
        "created_at",
        "updated_at",
    ),
    BASE_DIRTY_INSTRUMENT_TABLE: (
        "dirty_nk",
        "code",
        "adjust_method",
        "dirty_reason",
        "dirty_status",
        "first_marked_at",
        "last_marked_at",
    ),
}


RAW_MARKET_UNIQUE_INDEXES: Final[dict[str, tuple[tuple[str, tuple[str, ...]], ...]]] = {
    RAW_STOCK_FILE_REGISTRY_TABLE: (("ux_stock_file_registry_file_nk", ("file_nk",)),),
    RAW_STOCK_DAILY_BAR_TABLE: (("ux_stock_daily_bar_bar_nk", ("bar_nk",)),),
    RAW_TDXQUANT_RUN_TABLE: (("ux_raw_tdxquant_run_run_id", ("run_id",)),),
    RAW_TDXQUANT_REQUEST_TABLE: (("ux_raw_tdxquant_request_request_nk", ("request_nk",)),),
    RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE: (
        ("ux_raw_tdxquant_instrument_checkpoint_checkpoint_nk", ("checkpoint_nk",)),
    ),
}


MARKET_BASE_UNIQUE_INDEXES: Final[dict[str, tuple[tuple[str, tuple[str, ...]], ...]]] = {
    MARKET_BASE_STOCK_DAILY_TABLE: (
        ("ux_stock_daily_adjusted_code_trade_date_adjust_method", ("code", "trade_date", "adjust_method")),
    ),
    BASE_DIRTY_INSTRUMENT_TABLE: (("ux_base_dirty_instrument_dirty_nk", ("dirty_nk",)),),
}


def raw_market_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `raw_market` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.raw_market


def market_base_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `market_base` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.market_base


def connect_raw_market_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `raw_market` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(raw_market_ledger_path(workspace)), read_only=read_only)


def connect_market_base_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `market_base` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(market_base_ledger_path(workspace)), read_only=read_only)


def bootstrap_raw_market_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> Path:
    """创建或补齐 `raw_market` 最小正式表族。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    owns_connection = connection is None
    conn = connection or connect_raw_market_ledger(workspace)
    try:
        for ddl in RAW_MARKET_LEDGER_TABLES.values():
            conn.execute(ddl)
        for table_name, required_columns in RAW_MARKET_REQUIRED_COLUMNS.items():
            _ensure_columns(conn, table_name=table_name, required_columns=required_columns)
        _cleanup_raw_market_ledger(conn)
        for table_name, columns in RAW_MARKET_NOT_NULL_COLUMNS.items():
            _ensure_not_null_columns(conn, table_name=table_name, column_names=columns)
        for table_name, index_specs in RAW_MARKET_UNIQUE_INDEXES.items():
            for index_name, column_names in index_specs:
                _ensure_unique_index(conn, table_name=table_name, index_name=index_name, column_names=column_names)
        return raw_market_ledger_path(workspace)
    finally:
        if owns_connection:
            conn.close()


def bootstrap_market_base_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> Path:
    """创建或补齐 `market_base` 最小正式表族。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    owns_connection = connection is None
    conn = connection or connect_market_base_ledger(workspace)
    try:
        for ddl in MARKET_BASE_LEDGER_TABLES.values():
            conn.execute(ddl)
        for table_name, required_columns in MARKET_BASE_REQUIRED_COLUMNS.items():
            _ensure_columns(conn, table_name=table_name, required_columns=required_columns)
        _cleanup_market_base_ledger(conn)
        for table_name, columns in MARKET_BASE_NOT_NULL_COLUMNS.items():
            _ensure_not_null_columns(conn, table_name=table_name, column_names=columns)
        for table_name, index_specs in MARKET_BASE_UNIQUE_INDEXES.items():
            for index_name, column_names in index_specs:
                _ensure_unique_index(conn, table_name=table_name, index_name=index_name, column_names=column_names)
        return market_base_ledger_path(workspace)
    finally:
        if owns_connection:
            conn.close()


def _ensure_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    required_columns: dict[str, str],
) -> None:
    existing_rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    existing_columns = {str(row[1]) for row in existing_rows}
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def _cleanup_raw_market_ledger(connection: duckdb.DuckDBPyConnection) -> None:
    _delete_rows_with_nulls(
        connection,
        table_name=RAW_STOCK_FILE_REGISTRY_TABLE,
        required_columns=RAW_MARKET_NOT_NULL_COLUMNS[RAW_STOCK_FILE_REGISTRY_TABLE],
    )
    _deduplicate_table(
        connection,
        table_name=RAW_STOCK_FILE_REGISTRY_TABLE,
        key_columns=("file_nk",),
        order_columns=("last_ingested_at", "source_mtime_utc"),
    )
    _delete_rows_with_nulls(
        connection,
        table_name=RAW_STOCK_DAILY_BAR_TABLE,
        required_columns=RAW_MARKET_NOT_NULL_COLUMNS[RAW_STOCK_DAILY_BAR_TABLE],
    )
    _deduplicate_table(
        connection,
        table_name=RAW_STOCK_DAILY_BAR_TABLE,
        key_columns=("bar_nk",),
        order_columns=("updated_at", "created_at"),
    )
    _delete_rows_with_nulls(
        connection,
        table_name=RAW_TDXQUANT_RUN_TABLE,
        required_columns=RAW_MARKET_NOT_NULL_COLUMNS[RAW_TDXQUANT_RUN_TABLE],
    )
    _deduplicate_table(
        connection,
        table_name=RAW_TDXQUANT_RUN_TABLE,
        key_columns=("run_id",),
        order_columns=("finished_at_utc", "started_at_utc"),
    )
    _delete_rows_with_nulls(
        connection,
        table_name=RAW_TDXQUANT_REQUEST_TABLE,
        required_columns=RAW_MARKET_NOT_NULL_COLUMNS[RAW_TDXQUANT_REQUEST_TABLE],
    )
    _deduplicate_table(
        connection,
        table_name=RAW_TDXQUANT_REQUEST_TABLE,
        key_columns=("request_nk",),
        order_columns=("recorded_at",),
    )
    _delete_rows_with_nulls(
        connection,
        table_name=RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE,
        required_columns=RAW_MARKET_NOT_NULL_COLUMNS[RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE],
    )
    _deduplicate_table(
        connection,
        table_name=RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE,
        key_columns=("checkpoint_nk",),
        order_columns=("updated_at_utc", "last_success_trade_date"),
    )


def _cleanup_market_base_ledger(connection: duckdb.DuckDBPyConnection) -> None:
    _delete_rows_with_nulls(
        connection,
        table_name=MARKET_BASE_STOCK_DAILY_TABLE,
        required_columns=MARKET_BASE_NOT_NULL_COLUMNS[MARKET_BASE_STOCK_DAILY_TABLE],
    )
    _deduplicate_table(
        connection,
        table_name=MARKET_BASE_STOCK_DAILY_TABLE,
        key_columns=("code", "trade_date", "adjust_method"),
        order_columns=("updated_at", "created_at"),
    )
    _delete_rows_with_nulls(
        connection,
        table_name=BASE_DIRTY_INSTRUMENT_TABLE,
        required_columns=MARKET_BASE_NOT_NULL_COLUMNS[BASE_DIRTY_INSTRUMENT_TABLE],
    )
    _deduplicate_table(
        connection,
        table_name=BASE_DIRTY_INSTRUMENT_TABLE,
        key_columns=("dirty_nk",),
        order_columns=("last_marked_at", "first_marked_at"),
    )


def _delete_rows_with_nulls(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    required_columns: tuple[str, ...],
) -> None:
    if not required_columns:
        return
    predicate = " OR ".join(f"{column_name} IS NULL" for column_name in required_columns)
    connection.execute(f"DELETE FROM {table_name} WHERE {predicate}")


def _deduplicate_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    key_columns: tuple[str, ...],
    order_columns: tuple[str, ...],
) -> None:
    if not key_columns:
        return
    partition_sql = ", ".join(key_columns)
    order_sql = ", ".join(f"{column_name} DESC NULLS LAST" for column_name in order_columns) or "1"
    connection.execute(
        f"""
        DELETE FROM {table_name}
        USING (
            SELECT rowid
            FROM (
                SELECT
                    rowid,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_sql}
                        ORDER BY {order_sql}
                    ) AS duplicate_rank
                FROM {table_name}
            )
            WHERE duplicate_rank > 1
        ) AS duplicated_rows
        WHERE {table_name}.rowid = duplicated_rows.rowid
        """
    )


def _ensure_not_null_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    column_names: tuple[str, ...],
) -> None:
    if not column_names:
        return
    existing_rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    nullable_by_column = {str(row[1]): bool(row[3] == 0) for row in existing_rows}
    for column_name in column_names:
        if column_name not in nullable_by_column:
            continue
        if not nullable_by_column[column_name]:
            continue
        connection.execute(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET NOT NULL")


def _ensure_unique_index(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    index_name: str,
    column_names: tuple[str, ...],
) -> None:
    columns_sql = ", ".join(column_names)
    connection.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns_sql})")
