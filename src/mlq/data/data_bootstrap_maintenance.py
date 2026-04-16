"""`data` 账本 bootstrap 的约束修复与清理辅助函数。"""

from __future__ import annotations

import duckdb

from mlq.data.bootstrap import (
    BASE_DIRTY_INSTRUMENT_TABLE,
    MARKET_BASE_NOT_NULL_COLUMNS,
    MARKET_BASE_TABLE_BY_ASSET_AND_TIMEFRAME,
    RAW_BAR_TABLE_BY_ASSET_AND_TIMEFRAME,
    RAW_FILE_REGISTRY_TABLE_BY_ASSET_TYPE,
    RAW_MARKET_NOT_NULL_COLUMNS,
    RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE,
    RAW_TDXQUANT_REQUEST_TABLE,
    RAW_TDXQUANT_RUN_TABLE,
)


def ensure_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    required_columns: dict[str, str],
) -> None:
    """补齐治理要求中的缺失列。"""

    existing_rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    existing_columns = {str(row[1]) for row in existing_rows}
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def cleanup_raw_market_ledger(connection: duckdb.DuckDBPyConnection) -> None:
    """清理 raw_market 历史账本中的空键与重复记录。"""

    _backfill_legacy_timeframe_columns(connection)
    for table_name in RAW_FILE_REGISTRY_TABLE_BY_ASSET_TYPE.values():
        delete_rows_with_nulls(
            connection,
            table_name=table_name,
            required_columns=RAW_MARKET_NOT_NULL_COLUMNS[table_name],
        )
        deduplicate_table(
            connection,
            table_name=table_name,
            key_columns=("file_nk",),
            order_columns=("last_ingested_at", "source_mtime_utc"),
        )
    for asset_tables in RAW_BAR_TABLE_BY_ASSET_AND_TIMEFRAME.values():
        for table_name in asset_tables.values():
            delete_rows_with_nulls(
                connection,
                table_name=table_name,
                required_columns=RAW_MARKET_NOT_NULL_COLUMNS[table_name],
            )
            deduplicate_table(
                connection,
                table_name=table_name,
                key_columns=("bar_nk",),
                order_columns=("updated_at", "created_at"),
            )
    delete_rows_with_nulls(
        connection,
        table_name=RAW_TDXQUANT_RUN_TABLE,
        required_columns=RAW_MARKET_NOT_NULL_COLUMNS[RAW_TDXQUANT_RUN_TABLE],
    )
    deduplicate_table(
        connection,
        table_name=RAW_TDXQUANT_RUN_TABLE,
        key_columns=("run_id",),
        order_columns=("finished_at_utc", "started_at_utc"),
    )
    delete_rows_with_nulls(
        connection,
        table_name=RAW_TDXQUANT_REQUEST_TABLE,
        required_columns=RAW_MARKET_NOT_NULL_COLUMNS[RAW_TDXQUANT_REQUEST_TABLE],
    )
    deduplicate_table(
        connection,
        table_name=RAW_TDXQUANT_REQUEST_TABLE,
        key_columns=("request_nk",),
        order_columns=("recorded_at",),
    )
    delete_rows_with_nulls(
        connection,
        table_name=RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE,
        required_columns=RAW_MARKET_NOT_NULL_COLUMNS[RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE],
    )
    deduplicate_table(
        connection,
        table_name=RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE,
        key_columns=("checkpoint_nk",),
        order_columns=("updated_at_utc", "last_success_trade_date"),
    )


def cleanup_market_base_ledger(connection: duckdb.DuckDBPyConnection) -> None:
    """清理 market_base 历史账本中的空键与重复记录。"""

    _backfill_legacy_timeframe_columns(connection)
    for asset_tables in MARKET_BASE_TABLE_BY_ASSET_AND_TIMEFRAME.values():
        for table_name in asset_tables.values():
            delete_rows_with_nulls(
                connection,
                table_name=table_name,
                required_columns=MARKET_BASE_NOT_NULL_COLUMNS[table_name],
            )
            deduplicate_table(
                connection,
                table_name=table_name,
                key_columns=("code", "trade_date", "adjust_method"),
                order_columns=("updated_at", "created_at"),
            )
    delete_rows_with_nulls(
        connection,
        table_name=BASE_DIRTY_INSTRUMENT_TABLE,
        required_columns=MARKET_BASE_NOT_NULL_COLUMNS[BASE_DIRTY_INSTRUMENT_TABLE],
    )
    deduplicate_table(
        connection,
        table_name=BASE_DIRTY_INSTRUMENT_TABLE,
        key_columns=("dirty_nk",),
        order_columns=("last_marked_at", "first_marked_at"),
    )


def _backfill_legacy_timeframe_columns(connection: duckdb.DuckDBPyConnection) -> None:
    """把历史日线行的空 timeframe 回填成 `day`，并为周/月表补默认值。"""

    candidate_tables = [
        *RAW_FILE_REGISTRY_TABLE_BY_ASSET_TYPE.values(),
        *[table_name for tables in RAW_BAR_TABLE_BY_ASSET_AND_TIMEFRAME.values() for table_name in tables.values()],
        *[table_name for tables in MARKET_BASE_TABLE_BY_ASSET_AND_TIMEFRAME.values() for table_name in tables.values()],
        BASE_DIRTY_INSTRUMENT_TABLE,
        "raw_ingest_run",
        "raw_ingest_file",
        "base_build_run",
        "base_build_scope",
        "base_build_action",
    ]
    for table_name in candidate_tables:
        try:
            columns = {str(row[1]) for row in connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
        except duckdb.CatalogException:
            continue
        if "timeframe" not in columns:
            continue
        if table_name.endswith("_weekly_bar") or table_name.endswith("_weekly_adjusted"):
            default_timeframe = "week"
        elif table_name.endswith("_monthly_bar") or table_name.endswith("_monthly_adjusted"):
            default_timeframe = "month"
        else:
            default_timeframe = "day"
        connection.execute(
            f"""
            UPDATE {table_name}
            SET timeframe = ?
            WHERE timeframe IS NULL OR TRIM(CAST(timeframe AS VARCHAR)) = ''
            """,
            [default_timeframe],
        )


def delete_rows_with_nulls(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    required_columns: tuple[str, ...],
) -> None:
    """删除不满足 not-null 合同的历史脏行。"""

    if not required_columns:
        return
    predicate = " OR ".join(f"{column_name} IS NULL" for column_name in required_columns)
    connection.execute(f"DELETE FROM {table_name} WHERE {predicate}")


def deduplicate_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    key_columns: tuple[str, ...],
    order_columns: tuple[str, ...],
) -> None:
    """保留同一自然键下最新一行，删除历史重复行。"""

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


def ensure_not_null_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    column_names: tuple[str, ...],
) -> None:
    """把应为 not-null 的列修正回正式约束。"""

    if not column_names:
        return
    existing_rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    nullable_by_column = {str(row[1]): bool(row[3] == 0) for row in existing_rows}
    for column_name in column_names:
        if column_name not in nullable_by_column:
            continue
        if not nullable_by_column[column_name]:
            continue
        try:
            connection.execute(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET NOT NULL")
        except duckdb.DependencyException:
            # 存量官方库上的旧索引/依赖可能阻止 DuckDB 原地收紧列约束；
            # 此时保留元数据为 nullable，但前面的 cleanup/backfill 已确保正式数据不再写入空值。
            continue


def ensure_unique_index(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    index_name: str,
    column_names: tuple[str, ...],
) -> None:
    """确保正式自然键唯一索引存在。"""

    columns_sql = ", ".join(column_names)
    connection.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns_sql})")
