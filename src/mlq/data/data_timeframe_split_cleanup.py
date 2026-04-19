"""清理 day 官方库中遗留的 week/month price ledger 尾巴。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import (
    BASE_BUILD_ACTION_TABLE,
    BASE_BUILD_RUN_TABLE,
    BASE_BUILD_SCOPE_TABLE,
    BASE_DIRTY_INSTRUMENT_TABLE,
    MARKET_BASE_TABLE_BY_ASSET_AND_TIMEFRAME,
    RAW_BAR_TABLE_BY_ASSET_AND_TIMEFRAME,
    RAW_FILE_REGISTRY_TABLE_BY_ASSET_TYPE,
    RAW_INGEST_FILE_TABLE,
    RAW_INGEST_RUN_TABLE,
)
from mlq.data.ledger_timeframe import market_base_ledger_path, raw_market_ledger_path


NON_DAY_TIMEFRAMES: tuple[str, ...] = ("week", "month")


def _list_tables(connection: duckdb.DuckDBPyConnection) -> set[str]:
    return {str(row[0]) for row in connection.execute("SHOW TABLES").fetchall()}


def _count_rows(connection: duckdb.DuckDBPyConnection, *, table_name: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _count_timeframe_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    timeframes: tuple[str, ...],
) -> int:
    placeholders = ", ".join("?" for _ in timeframes)
    return int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE COALESCE(timeframe, 'day') IN ({placeholders})
            """,
            list(timeframes),
        ).fetchone()[0]
    )


def _delete_timeframe_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    timeframes: tuple[str, ...],
) -> int:
    index_rows = connection.execute(
        """
        SELECT index_name, sql
        FROM duckdb_indexes()
        WHERE schema_name = 'main'
          AND table_name = ?
          AND sql IS NOT NULL
        ORDER BY index_name
        """,
        [table_name],
    ).fetchall()
    placeholders = ", ".join("?" for _ in timeframes)
    before_count = _count_timeframe_rows(connection, table_name=table_name, timeframes=timeframes)
    for index_name, _ in index_rows:
        connection.execute(f"DROP INDEX IF EXISTS {index_name}")
    connection.execute(
        f"""
        DELETE FROM {table_name}
        WHERE COALESCE(timeframe, 'day') IN ({placeholders})
        """,
        list(timeframes),
    )
    existing_index_names = {
        str(row[0])
        for row in connection.execute(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE schema_name = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchall()
    }
    for index_name, index_sql in index_rows:
        if str(index_name) in existing_index_names:
            continue
        try:
            connection.execute(str(index_sql))
        except duckdb.CatalogException as exc:
            if "already exists" not in str(exc):
                raise
    after_count = _count_timeframe_rows(connection, table_name=table_name, timeframes=timeframes)
    return before_count - after_count


def _build_non_day_price_tables(table_map: dict[str, dict[str, str]]) -> tuple[str, ...]:
    return tuple(
        table_name
        for timeframe in NON_DAY_TIMEFRAMES
        for asset_tables in table_map.values()
        for candidate_timeframe, table_name in asset_tables.items()
        if candidate_timeframe == timeframe
    )


def purge_day_timeframe_split_tail(
    *,
    settings: WorkspaceRoots | None = None,
    execute: bool = False,
) -> dict[str, object]:
    """清理 day raw/base 官方库中的 week/month price/audit 残留。"""

    workspace = settings or default_settings()
    raw_path = raw_market_ledger_path(workspace)
    base_path = market_base_ledger_path(workspace)

    raw_drop_tables = _build_non_day_price_tables(RAW_BAR_TABLE_BY_ASSET_AND_TIMEFRAME)
    base_drop_tables = _build_non_day_price_tables(MARKET_BASE_TABLE_BY_ASSET_AND_TIMEFRAME)
    raw_timeframe_tables = (
        RAW_INGEST_RUN_TABLE,
        RAW_INGEST_FILE_TABLE,
        *RAW_FILE_REGISTRY_TABLE_BY_ASSET_TYPE.values(),
    )
    base_timeframe_tables = (
        BASE_DIRTY_INSTRUMENT_TABLE,
        BASE_BUILD_RUN_TABLE,
        BASE_BUILD_SCOPE_TABLE,
        BASE_BUILD_ACTION_TABLE,
    )

    raw_connection = duckdb.connect(str(raw_path))
    base_connection = duckdb.connect(str(base_path))
    try:
        raw_existing_tables = _list_tables(raw_connection)
        base_existing_tables = _list_tables(base_connection)
        summary: dict[str, object] = {
            "execute": bool(execute),
            "raw_market_path": str(raw_path),
            "market_base_path": str(base_path),
            "raw": {
                "drop_tables": [
                    {
                        "table": table_name,
                        "present": table_name in raw_existing_tables,
                        "row_count": _count_rows(raw_connection, table_name=table_name)
                        if table_name in raw_existing_tables
                        else 0,
                    }
                    for table_name in raw_drop_tables
                ],
                "delete_rows": [
                    {
                        "table": table_name,
                        "row_count": _count_timeframe_rows(
                            raw_connection,
                            table_name=table_name,
                            timeframes=NON_DAY_TIMEFRAMES,
                        ),
                    }
                    for table_name in raw_timeframe_tables
                    if table_name in raw_existing_tables
                ],
            },
            "base": {
                "drop_tables": [
                    {
                        "table": table_name,
                        "present": table_name in base_existing_tables,
                        "row_count": _count_rows(base_connection, table_name=table_name)
                        if table_name in base_existing_tables
                        else 0,
                    }
                    for table_name in base_drop_tables
                ],
                "delete_rows": [
                    {
                        "table": table_name,
                        "row_count": _count_timeframe_rows(
                            base_connection,
                            table_name=table_name,
                            timeframes=NON_DAY_TIMEFRAMES,
                        ),
                    }
                    for table_name in base_timeframe_tables
                    if table_name in base_existing_tables
                ],
            },
        }
        if not execute:
            return summary

        raw_connection.execute("BEGIN TRANSACTION")
        try:
            for table_name in raw_drop_tables:
                raw_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            for table_name in raw_timeframe_tables:
                if table_name in raw_existing_tables:
                    _delete_timeframe_rows(
                        raw_connection,
                        table_name=table_name,
                        timeframes=NON_DAY_TIMEFRAMES,
                    )
            raw_connection.execute("COMMIT")
        except Exception:
            raw_connection.execute("ROLLBACK")
            raise

        base_connection.execute("BEGIN TRANSACTION")
        try:
            for table_name in base_drop_tables:
                base_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            for table_name in base_timeframe_tables:
                if table_name in base_existing_tables:
                    _delete_timeframe_rows(
                        base_connection,
                        table_name=table_name,
                        timeframes=NON_DAY_TIMEFRAMES,
                    )
            base_connection.execute("COMMIT")
        except Exception:
            base_connection.execute("ROLLBACK")
            raise

        return purge_day_timeframe_split_tail(settings=workspace, execute=False)
    finally:
        raw_connection.close()
        base_connection.close()
