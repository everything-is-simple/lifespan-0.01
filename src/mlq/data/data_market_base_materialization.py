"""market_base 构建的落表与审计辅助函数。"""

from __future__ import annotations

from mlq.data.data_common import *
from mlq.data.data_shared import *

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


def _stage_market_base_rows_by_asset(
    *,
    connection: duckdb.DuckDBPyConnection,
    asset_type: str,
    raw_table: str,
    market_table: str,
    adjust_method: str,
    instruments: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
    limit: int | None,
    force_empty_result: bool,
) -> None:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        _stage_market_base_rows(
            connection=connection,
            adjust_method=adjust_method,
            instruments=instruments,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            force_empty_result=force_empty_result,
        )
        return
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
        FROM raw_source.{raw_table}
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
            FROM {market_table}
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


def _record_base_build_actions_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
) -> None:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        _record_base_build_actions(connection, run_id=run_id)
        return
    connection.execute(
        f"""
        INSERT INTO {BASE_BUILD_ACTION_TABLE} (
            run_id,
            asset_type,
            code,
            adjust_method,
            action,
            row_count
        )
        SELECT
            ? AS run_id,
            ? AS asset_type,
            code,
            adjust_method,
            action,
            row_count
        FROM {MARKET_BASE_ACTION_STAGE_TABLE}
        """,
        [run_id, normalized_asset_type],
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


def _materialize_market_base_stage_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    market_table: str,
    adjust_method: str,
    run_id: str,
    full_scope: bool,
) -> None:
    if market_table == MARKET_BASE_STOCK_DAILY_TABLE:
        _materialize_market_base_stage(
            connection,
            adjust_method=adjust_method,
            run_id=run_id,
            full_scope=full_scope,
        )
        return
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
        MERGE INTO {market_table} AS target
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
            DELETE FROM {market_table} AS target
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
            asset_type,
            runner_name,
            runner_version,
            adjust_method,
            build_mode,
            source_scope_kind,
            run_status,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'running', NULL)
        """,
        [
            run_id,
            DEFAULT_ASSET_TYPE,
            BASE_BUILD_RUNNER_NAME,
            BASE_BUILD_RUNNER_VERSION,
            adjust_method,
            build_mode,
            source_scope_kind,
        ],
    )


def _insert_base_build_run_start_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    adjust_method: str,
    build_mode: str,
    source_scope_kind: str,
) -> None:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        _insert_base_build_run_start(
            connection,
            run_id=run_id,
            adjust_method=adjust_method,
            build_mode=build_mode,
            source_scope_kind=source_scope_kind,
        )
        return
    connection.execute(
        f"""
        INSERT INTO {BASE_BUILD_RUN_TABLE} (
            run_id,
            asset_type,
            runner_name,
            runner_version,
            adjust_method,
            build_mode,
            source_scope_kind,
            run_status,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'running', NULL)
        """,
        [
            run_id,
            normalized_asset_type,
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


def _update_base_build_run_failure_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    error_message: str,
) -> None:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        _update_base_build_run_failure(connection, run_id=run_id, error_message=error_message)
        return
    connection.execute(
        f"""
        UPDATE {BASE_BUILD_RUN_TABLE}
        SET
            asset_type = ?,
            run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            normalized_asset_type,
            json.dumps(
                {"asset_type": normalized_asset_type, "error_message": error_message},
                ensure_ascii=False,
                sort_keys=True,
            ),
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


def _mark_scope_dirty_entries_consumed_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    adjust_method: str,
    instruments: tuple[str, ...],
) -> int:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        return _mark_scope_dirty_entries_consumed(
            connection,
            run_id=run_id,
            adjust_method=adjust_method,
            instruments=instruments,
        )
    parameters: list[object] = [run_id, DEFAULT_ASSET_TYPE, normalized_asset_type, adjust_method]
    where_clauses = ["COALESCE(asset_type, ?) = ?", "adjust_method = ?", "dirty_status = 'pending'"]
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



__all__ = [name for name in globals() if not name.startswith("__")]

