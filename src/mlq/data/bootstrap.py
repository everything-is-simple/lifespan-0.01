"""冻结 `data` 模块最小正式账本表结构。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


RAW_STOCK_FILE_REGISTRY_TABLE: Final[str] = "stock_file_registry"
RAW_STOCK_DAILY_BAR_TABLE: Final[str] = "stock_daily_bar"
MARKET_BASE_STOCK_DAILY_TABLE: Final[str] = "stock_daily_adjusted"


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
    }
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
