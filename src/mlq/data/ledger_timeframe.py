"""raw/base 日周月官方库的路径与连接辅助。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings

TDX_TIMEFRAMES: tuple[str, ...] = ("day", "week", "month")


def _normalize_timeframe(timeframe: str) -> str:
    normalized = timeframe.strip().lower()
    if normalized not in TDX_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return normalized


def _raw_market_database_path(workspace: WorkspaceRoots, *, timeframe: str) -> Path:
    return {
        "day": workspace.databases.raw_market_day,
        "week": workspace.databases.raw_market_week,
        "month": workspace.databases.raw_market_month,
    }[timeframe]


def _market_base_database_path(workspace: WorkspaceRoots, *, timeframe: str) -> Path:
    return {
        "day": workspace.databases.market_base_day,
        "week": workspace.databases.market_base_week,
        "month": workspace.databases.market_base_month,
    }[timeframe]


def raw_market_timeframe_ledger_path(
    settings: WorkspaceRoots | None = None,
    *,
    timeframe: str = "day",
) -> Path:
    """返回指定 timeframe 对应的正式 `raw_market` 账本路径。"""

    workspace = settings or default_settings()
    return _raw_market_database_path(workspace, timeframe=_normalize_timeframe(timeframe))


def market_base_timeframe_ledger_path(
    settings: WorkspaceRoots | None = None,
    *,
    timeframe: str = "day",
) -> Path:
    """返回指定 timeframe 对应的正式 `market_base` 账本路径。"""

    workspace = settings or default_settings()
    return _market_base_database_path(workspace, timeframe=_normalize_timeframe(timeframe))


def _connect_timeframe_ledger(
    settings: WorkspaceRoots | None,
    *,
    timeframe: str,
    read_only: bool,
    path_resolver,
) -> duckdb.DuckDBPyConnection:
    workspace = settings or default_settings()
    normalized = _normalize_timeframe(timeframe)
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(path_resolver(workspace, timeframe=normalized)), read_only=read_only)


def connect_raw_market_timeframe_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    timeframe: str = "day",
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接指定 timeframe 对应的正式 `raw_market` 历史账本。"""

    return _connect_timeframe_ledger(
        settings,
        timeframe=timeframe,
        read_only=read_only,
        path_resolver=_raw_market_database_path,
    )


def connect_market_base_timeframe_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    timeframe: str = "day",
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接指定 timeframe 对应的正式 `market_base` 历史账本。"""

    return _connect_timeframe_ledger(
        settings,
        timeframe=timeframe,
        read_only=read_only,
        path_resolver=_market_base_database_path,
    )


def raw_market_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """兼容旧调用：返回 day 官方 `raw_market` 账本路径。"""

    return raw_market_timeframe_ledger_path(settings, timeframe="day")


def market_base_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """兼容旧调用：返回 day 官方 `market_base` 账本路径。"""

    return market_base_timeframe_ledger_path(settings, timeframe="day")


def connect_raw_market_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """兼容旧调用：连接 day 官方 `raw_market` 历史账本。"""

    return connect_raw_market_timeframe_ledger(settings, timeframe="day", read_only=read_only)


def connect_market_base_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """兼容旧调用：连接 day 官方 `market_base` 历史账本。"""

    return connect_market_base_timeframe_ledger(settings, timeframe="day", read_only=read_only)
