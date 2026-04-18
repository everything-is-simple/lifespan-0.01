"""market_base 构建的 run audit / dirty queue 辅助。"""

from __future__ import annotations

import json

from mlq.data.data_common import *
from mlq.data.data_shared import *


def insert_base_build_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    timeframe: str,
    adjust_method: str,
    build_mode: str,
    source_scope_kind: str,
) -> None:
    """登记股票 `market_base` build run 起点。"""

    connection.execute(
        f"""
        INSERT INTO {BASE_BUILD_RUN_TABLE} (
            run_id,
            asset_type,
            timeframe,
            runner_name,
            runner_version,
            adjust_method,
            build_mode,
            source_scope_kind,
            run_status,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running', NULL)
        """,
        [
            run_id,
            DEFAULT_ASSET_TYPE,
            _normalize_timeframe(timeframe),
            BASE_BUILD_RUNNER_NAME,
            BASE_BUILD_RUNNER_VERSION,
            adjust_method,
            build_mode,
            source_scope_kind,
        ],
    )


def insert_base_build_run_start_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    timeframe: str,
    adjust_method: str,
    build_mode: str,
    source_scope_kind: str,
) -> None:
    """登记资产级 `market_base` build run 起点。"""

    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        insert_base_build_run_start(
            connection,
            run_id=run_id,
            timeframe=timeframe,
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
            timeframe,
            runner_name,
            runner_version,
            adjust_method,
            build_mode,
            source_scope_kind,
            run_status,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running', NULL)
        """,
        [
            run_id,
            normalized_asset_type,
            _normalize_timeframe(timeframe),
            BASE_BUILD_RUNNER_NAME,
            BASE_BUILD_RUNNER_VERSION,
            adjust_method,
            build_mode,
            source_scope_kind,
        ],
    )


def update_base_build_run_success(
    connection: duckdb.DuckDBPyConnection,
    *,
    summary: MarketBaseBuildSummary,
) -> None:
    """把 build run 标记为 completed。"""

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


def update_base_build_run_failure(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    timeframe: str,
    error_message: str,
) -> None:
    """把股票 build run 标记为 failed。"""

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
            json.dumps(
                {"timeframe": _normalize_timeframe(timeframe), "error_message": error_message},
                ensure_ascii=False,
                sort_keys=True,
            ),
            run_id,
        ],
    )


def update_base_build_run_failure_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    timeframe: str,
    error_message: str,
) -> None:
    """把资产级 build run 标记为 failed。"""

    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        update_base_build_run_failure(
            connection,
            run_id=run_id,
            timeframe=timeframe,
            error_message=error_message,
        )
        return
    connection.execute(
        f"""
        UPDATE {BASE_BUILD_RUN_TABLE}
        SET
            asset_type = ?,
            timeframe = ?,
            run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            normalized_asset_type,
            _normalize_timeframe(timeframe),
            json.dumps(
                {
                    "asset_type": normalized_asset_type,
                    "timeframe": _normalize_timeframe(timeframe),
                    "error_message": error_message,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            run_id,
        ],
    )


def mark_dirty_entries_consumed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    dirty_entries: tuple[BaseDirtyInstrumentEntry, ...],
) -> int:
    """按 dirty entry 集合回写 consumed 状态。"""

    if not dirty_entries:
        return 0
    relation_name = "stage_base_dirty_entries_to_consume"
    frame = pd.DataFrame.from_records(
        [
            {
                "dirty_nk": entry.dirty_nk,
                "asset_type": _normalize_asset_type(entry.asset_type),
                "timeframe": _normalize_timeframe(entry.timeframe),
                "code": entry.code,
                "adjust_method": entry.adjust_method,
            }
            for entry in dirty_entries
        ]
    ).drop_duplicates()
    connection.register(relation_name, frame)
    try:
        updated_rows = connection.execute(
            f"""
            UPDATE {BASE_DIRTY_INSTRUMENT_TABLE} AS target
            SET
                dirty_status = 'consumed',
                last_consumed_run_id = ?,
                last_marked_at = CURRENT_TIMESTAMP
            FROM {relation_name} AS stage
            WHERE target.dirty_status = 'pending'
              AND (
                    target.dirty_nk = stage.dirty_nk
                    OR (
                        COALESCE(target.asset_type, ?) = stage.asset_type
                        AND COALESCE(target.timeframe, 'day') = stage.timeframe
                        AND target.code = stage.code
                        AND target.adjust_method = stage.adjust_method
                    )
                )
            RETURNING target.dirty_nk
            """,
            [run_id, DEFAULT_ASSET_TYPE],
        ).fetchall()
    finally:
        connection.unregister(relation_name)
    return len(updated_rows)


def mark_scope_dirty_entries_consumed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    timeframe: str,
    adjust_method: str,
    instruments: tuple[str, ...],
) -> int:
    """按股票 scope 批量消费 dirty queue。"""

    parameters: list[object] = [run_id, _normalize_timeframe(timeframe), adjust_method]
    where_clauses = ["COALESCE(timeframe, 'day') = ?", "adjust_method = ?", "dirty_status = 'pending'"]
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


def mark_scope_dirty_entries_consumed_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    timeframe: str,
    adjust_method: str,
    instruments: tuple[str, ...],
) -> int:
    """按资产类型 scope 批量消费 dirty queue。"""

    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        return mark_scope_dirty_entries_consumed(
            connection,
            run_id=run_id,
            timeframe=timeframe,
            adjust_method=adjust_method,
            instruments=instruments,
        )
    parameters: list[object] = [
        run_id,
        DEFAULT_ASSET_TYPE,
        normalized_asset_type,
        _normalize_timeframe(timeframe),
        adjust_method,
    ]
    where_clauses = [
        "COALESCE(asset_type, ?) = ?",
        "COALESCE(timeframe, 'day') = ?",
        "adjust_method = ?",
        "dirty_status = 'pending'",
    ]
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
