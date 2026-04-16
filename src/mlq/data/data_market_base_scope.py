"""market_base 构建的范围规划与作用域辅助函数。"""

from __future__ import annotations

from mlq.data.data_common import *
from mlq.data.data_shared import *

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
    timeframe: str,
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
            timeframe=timeframe,
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
                        "timeframe": entry.timeframe,
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
            asset_type=DEFAULT_ASSET_TYPE,
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
        asset_type=DEFAULT_ASSET_TYPE,
        instruments=instruments,
        scope_records=tuple(scope_records),
        dirty_entries=(),
        scope_is_empty=False,
    )


def _resolve_base_build_scope_plan_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    timeframe: str,
    adjust_method: str,
    build_mode: str,
    consume_dirty_only: bool,
    instruments: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
    limit: int | None,
) -> BaseBuildScopePlan:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        return _resolve_base_build_scope_plan(
            connection,
            timeframe=timeframe,
            adjust_method=adjust_method,
            build_mode=build_mode,
            consume_dirty_only=consume_dirty_only,
            instruments=instruments,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    if build_mode == "incremental" and consume_dirty_only:
        dirty_entries = _fetch_pending_dirty_entries_by_asset(
            connection,
            asset_type=normalized_asset_type,
            timeframe=timeframe,
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
                        "asset_type": entry.asset_type,
                        "timeframe": entry.timeframe,
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
            asset_type=normalized_asset_type,
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
        asset_type=normalized_asset_type,
        instruments=instruments,
        scope_records=tuple(scope_records),
        dirty_entries=(),
        scope_is_empty=False,
    )


def _fetch_pending_dirty_entries(
    connection: duckdb.DuckDBPyConnection,
    *,
    timeframe: str,
    adjust_method: str,
    instruments: tuple[str, ...],
    limit: int | None,
) -> tuple[BaseDirtyInstrumentEntry, ...]:
    parameters: list[object] = [_normalize_timeframe(timeframe), adjust_method]
    where_clauses = ["COALESCE(timeframe, 'day') = ?", "adjust_method = ?", "dirty_status = 'pending'"]
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
            asset_type=DEFAULT_ASSET_TYPE,
            timeframe=_normalize_timeframe(timeframe),
            code=str(row[1]),
            adjust_method=str(row[2]),
            dirty_reason=str(row[3]),
            source_run_id=None if row[4] is None else str(row[4]),
            source_file_nk=None if row[5] is None else str(row[5]),
        )
        for row in rows
    )


def _fetch_pending_dirty_entries_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    timeframe: str,
    adjust_method: str,
    instruments: tuple[str, ...],
    limit: int | None,
) -> tuple[BaseDirtyInstrumentEntry, ...]:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        return _fetch_pending_dirty_entries(
            connection,
            timeframe=timeframe,
            adjust_method=adjust_method,
            instruments=instruments,
            limit=limit,
        )
    parameters: list[object] = [
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
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        parameters.append(limit)
    rows = connection.execute(
        f"""
        SELECT dirty_nk, asset_type, COALESCE(timeframe, 'day') AS timeframe, code, adjust_method, dirty_reason, source_run_id, source_file_nk
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
            asset_type=DEFAULT_ASSET_TYPE if row[1] is None else str(row[1]),
            timeframe=_normalize_timeframe(row[2]),
            code=str(row[3]),
            adjust_method=str(row[4]),
            dirty_reason=str(row[5]),
            source_run_id=None if row[6] is None else str(row[6]),
            source_file_nk=None if row[7] is None else str(row[7]),
        )
        for row in rows
    )


def _record_base_build_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    timeframe: str,
    scope_records: tuple[tuple[str, str], ...],
) -> None:
    if not scope_records:
        return
    frame = pd.DataFrame.from_records(
        [
            {
                "run_id": run_id,
                "timeframe": _normalize_timeframe(timeframe),
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
                timeframe,
                scope_type,
                scope_value
            )
            SELECT
                run_id,
                timeframe,
                scope_type,
                scope_value
            FROM {relation_name}
            """
        )
    finally:
        connection.unregister(relation_name)


def _record_base_build_scopes_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    timeframe: str,
    scope_records: tuple[tuple[str, str], ...],
) -> None:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        _record_base_build_scopes(
            connection,
            run_id=run_id,
            timeframe=timeframe,
            scope_records=scope_records,
        )
        return
    if not scope_records:
        return
    frame = pd.DataFrame.from_records(
        [
            {
                "run_id": run_id,
                "asset_type": normalized_asset_type,
                "timeframe": _normalize_timeframe(timeframe),
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
                asset_type,
                timeframe,
                scope_type,
                scope_value
            )
            SELECT
                run_id,
                asset_type,
                timeframe,
                scope_type,
                scope_value
            FROM {relation_name}
            """
        )
    finally:
        connection.unregister(relation_name)



__all__ = [name for name in globals() if not name.startswith("__")]

