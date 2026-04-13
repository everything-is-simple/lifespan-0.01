"""`structure snapshot` 的上游读取、脏队列与 checkpoint 读写。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from mlq.malf.bootstrap import MALF_CANONICAL_CHECKPOINT_TABLE
from mlq.structure.structure_query import (
    _load_break_confirmation_rows,
    _load_stats_snapshot_rows,
    _load_table_columns,
    _resolve_existing_column,
    _resolve_optional_column,
)
from mlq.structure.bootstrap import STRUCTURE_CHECKPOINT_TABLE, STRUCTURE_WORK_QUEUE_TABLE
from mlq.structure.structure_shared import (
    _StructureContextRow,
    _StructureInputRow,
    _build_canonical_context_nk,
    _build_queue_nk,
    _build_scope_nk,
    _derive_failure_type_from_major_state,
    _map_major_state_to_context_code,
    _normalize_date_value,
    _normalize_optional_int,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
    _to_python_date,
)


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _resolve_optional_sidecar_table(
    *,
    malf_path: Path,
    requested_table: str | None,
    fallback_table: str | None,
) -> str | None:
    if requested_table and _database_table_exists(malf_path, requested_table):
        return requested_table
    if fallback_table and _database_table_exists(malf_path, fallback_table):
        return fallback_table
    return None


def _database_table_exists(path: Path, table_name: str | None) -> bool:
    if table_name is None or table_name == "" or not path.exists():
        return False
    connection = duckdb.connect(str(path), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            LIMIT 1
            """,
            [table_name],
        ).fetchall()
        return bool(rows)
    finally:
        connection.close()


def _load_structure_dirty_scopes(
    *,
    malf_path: Path,
    limit: int,
    timeframe: str,
) -> list[dict[str, object]]:
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        rows = connection.execute(
            f"""
            SELECT asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, last_run_id
            FROM {MALF_CANONICAL_CHECKPOINT_TABLE}
            WHERE timeframe IN ('D', 'W', 'M')
            ORDER BY code, timeframe
            """
        ).fetchall()
    finally:
        connection.close()
    grouped: dict[tuple[str, str], list[tuple[object, ...]]] = {}
    for row in rows:
        grouped.setdefault((str(row[0]), str(row[1])), []).append(row)
    scope_rows: list[dict[str, object]] = []
    for (asset_type, code), checkpoint_rows in sorted(grouped.items())[:limit]:
        checkpoint_map = {str(row[2]): row for row in checkpoint_rows}
        daily_row = checkpoint_map.get(timeframe)
        if daily_row is None:
            continue
        replay_start_candidates = [
            _to_python_date(row[4])
            for row in checkpoint_rows
            if _to_python_date(row[4]) is not None
        ]
        replay_end = _to_python_date(daily_row[3]) or max(
            (_to_python_date(row[3]) for row in checkpoint_rows if _to_python_date(row[3]) is not None),
            default=None,
        )
        if replay_end is None:
            continue
        source_fingerprint = _build_structure_source_fingerprint(checkpoint_rows)
        scope_rows.append(
            {
                "scope_nk": _build_scope_nk(asset_type=asset_type, code=code, timeframe=timeframe),
                "asset_type": asset_type,
                "code": code,
                "timeframe": timeframe,
                "replay_start_bar_dt": min(replay_start_candidates) if replay_start_candidates else replay_end,
                "replay_confirm_until_dt": replay_end,
                "source_fingerprint": source_fingerprint,
            }
        )
    return scope_rows


def _build_structure_source_fingerprint(checkpoint_rows: list[tuple[object, ...]]) -> str:
    payload = [
        {
            "timeframe": str(row[2]),
            "last_completed_bar_dt": None if _to_python_date(row[3]) is None else _to_python_date(row[3]).isoformat(),
            "tail_start_bar_dt": None if _to_python_date(row[4]) is None else _to_python_date(row[4]).isoformat(),
            "tail_confirm_until_dt": None if _to_python_date(row[5]) is None else _to_python_date(row[5]).isoformat(),
            "last_run_id": None if row[6] is None else str(row[6]),
        }
        for row in sorted(checkpoint_rows, key=lambda item: str(item[2]))
    ]
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _enqueue_structure_dirty_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    scope_rows: list[dict[str, object]],
    run_id: str,
) -> dict[str, int]:
    queue_enqueued_count = 0
    for scope_row in scope_rows:
        checkpoint_row = connection.execute(
            f"""
            SELECT last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint
            FROM {STRUCTURE_CHECKPOINT_TABLE}
            WHERE asset_type = ?
              AND code = ?
              AND timeframe = ?
            """,
            [scope_row["asset_type"], scope_row["code"], scope_row["timeframe"]],
        ).fetchone()
        replay_start = _to_python_date(scope_row["replay_start_bar_dt"])
        replay_end = _to_python_date(scope_row["replay_confirm_until_dt"])
        dirty_reason = _derive_structure_dirty_reason(
            checkpoint_row=checkpoint_row,
            replay_start_bar_dt=replay_start,
            replay_confirm_until_dt=replay_end,
            source_fingerprint=str(scope_row["source_fingerprint"]),
        )
        if dirty_reason is None:
            continue
        queue_nk = _build_queue_nk(scope_nk=str(scope_row["scope_nk"]), dirty_reason=dirty_reason)
        existing = connection.execute(
            f"SELECT queue_nk FROM {STRUCTURE_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
            [queue_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {STRUCTURE_WORK_QUEUE_TABLE} (
                    queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
                    replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint,
                    queue_status, first_seen_run_id, last_materialized_run_id, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    queue_nk,
                    scope_row["scope_nk"],
                    scope_row["asset_type"],
                    scope_row["code"],
                    scope_row["timeframe"],
                    dirty_reason,
                    replay_start,
                    replay_end,
                    scope_row["source_fingerprint"],
                    run_id,
                    run_id,
                ],
            )
            queue_enqueued_count += 1
            continue
        connection.execute(
            f"""
            UPDATE {STRUCTURE_WORK_QUEUE_TABLE}
            SET replay_start_bar_dt = ?,
                replay_confirm_until_dt = ?,
                source_fingerprint = ?,
                queue_status = 'pending',
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [replay_start, replay_end, scope_row["source_fingerprint"], queue_nk],
        )
    return {"queue_enqueued_count": queue_enqueued_count}


def _derive_structure_dirty_reason(
    *,
    checkpoint_row: tuple[object, ...] | None,
    replay_start_bar_dt: date | None,
    replay_confirm_until_dt: date | None,
    source_fingerprint: str,
) -> str | None:
    if checkpoint_row is None:
        return "bootstrap_missing_checkpoint"
    last_completed_bar_dt = _to_python_date(checkpoint_row[0])
    tail_start_bar_dt = _to_python_date(checkpoint_row[1])
    tail_confirm_until_dt = _to_python_date(checkpoint_row[2])
    checkpoint_fingerprint = "" if checkpoint_row[3] is None else str(checkpoint_row[3])
    if checkpoint_fingerprint != source_fingerprint:
        return "source_fingerprint_changed"
    if last_completed_bar_dt is None or (replay_confirm_until_dt is not None and replay_confirm_until_dt > last_completed_bar_dt):
        return "source_advanced"
    if tail_start_bar_dt is None or (replay_start_bar_dt is not None and replay_start_bar_dt < tail_start_bar_dt):
        return "source_replayed"
    if tail_confirm_until_dt is None or (replay_confirm_until_dt is not None and replay_confirm_until_dt > tail_confirm_until_dt):
        return "tail_confirm_advanced"
    return None


def _claim_structure_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    timeframe: str,
) -> list[dict[str, object]]:
    rows = connection.execute(
        f"""
        SELECT queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
               replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint
        FROM {STRUCTURE_WORK_QUEUE_TABLE}
        WHERE timeframe = ?
          AND queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY code, enqueued_at
        """,
        [timeframe],
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {STRUCTURE_WORK_QUEUE_TABLE}
            SET queue_status = 'claimed',
                claimed_at = CURRENT_TIMESTAMP,
                last_claimed_run_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [run_id, str(row[0])],
        )
        claimed_rows.append(
            {
                "queue_nk": str(row[0]),
                "scope_nk": str(row[1]),
                "asset_type": str(row[2]),
                "code": str(row[3]),
                "timeframe": str(row[4]),
                "dirty_reason": str(row[5]),
                "replay_start_bar_dt": _to_python_date(row[6]),
                "replay_confirm_until_dt": _to_python_date(row[7]),
                "source_fingerprint": str(row[8]),
            }
        )
    return claimed_rows


def _mark_structure_queue_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {STRUCTURE_WORK_QUEUE_TABLE}
        SET queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _mark_structure_queue_failed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {STRUCTURE_WORK_QUEUE_TABLE}
        SET queue_status = 'failed',
            last_claimed_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _upsert_structure_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    code: str,
    timeframe: str,
    last_completed_bar_dt: date | None,
    tail_start_bar_dt: date | None,
    tail_confirm_until_dt: date | None,
    source_fingerprint: str,
    last_run_id: str,
) -> None:
    existing = connection.execute(
        f"""
        SELECT asset_type
        FROM {STRUCTURE_CHECKPOINT_TABLE}
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [asset_type, code, timeframe],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {STRUCTURE_CHECKPOINT_TABLE} (
                asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt,
                tail_confirm_until_dt, source_fingerprint, last_run_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                asset_type,
                code,
                timeframe,
                last_completed_bar_dt,
                tail_start_bar_dt,
                tail_confirm_until_dt,
                source_fingerprint,
                last_run_id,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {STRUCTURE_CHECKPOINT_TABLE}
        SET last_completed_bar_dt = ?,
            tail_start_bar_dt = ?,
            tail_confirm_until_dt = ?,
            source_fingerprint = ?,
            last_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [
            last_completed_bar_dt,
            tail_start_bar_dt,
            tail_confirm_until_dt,
            source_fingerprint,
            last_run_id,
            asset_type,
            code,
            timeframe,
        ],
    )


def _load_structure_input_rows(
    *,
    malf_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    timeframe: str,
) -> list[_StructureInputRow]:
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        return _load_canonical_input_rows(
            connection,
            table_name=table_name,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=instruments,
            limit=limit,
            timeframe=timeframe,
        )
    finally:
        connection.close()


def _load_context_rows(
    *,
    malf_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> list[_StructureContextRow]:
    if not instruments:
        return []
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        return _load_canonical_context_rows(
            connection,
            table_name=table_name,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=instruments,
            timeframe=timeframe,
        )
    finally:
        connection.close()


def _load_read_only_context_rows(
    *,
    malf_path: Path,
    table_name: str,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> list[_StructureContextRow]:
    if not instruments:
        return []
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        return _load_canonical_context_rows(
            connection,
            table_name=table_name,
            signal_start_date=None,
            signal_end_date=signal_end_date,
            instruments=instruments,
            timeframe=timeframe,
        )
    finally:
        connection.close()


def _load_canonical_input_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    timeframe: str,
) -> list[_StructureInputRow]:
    available_columns = _load_table_columns(connection, table_name)
    code_column = _resolve_existing_column(
        available_columns,
        ("code", "instrument", "entity_code"),
        field_name="code",
        table_name=table_name,
    )
    asof_date_column = _resolve_existing_column(
        available_columns,
        ("asof_bar_dt", "asof_date", "signal_date"),
        field_name="asof_bar_dt",
        table_name=table_name,
    )
    timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
    asset_type_column = _resolve_optional_column(available_columns, ("asset_type",))
    parameters: list[object] = []
    where_clauses: list[str] = []
    if signal_start_date is not None:
        where_clauses.append(f"{asof_date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{asof_date_column} <= ?")
        parameters.append(signal_end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{code_column} IN ({placeholders})")
        parameters.extend(instruments)
    if timeframe_column is not None:
        where_clauses.append(f"{timeframe_column} = ?")
        parameters.append(timeframe)
    if asset_type_column is not None:
        where_clauses.append(f"{asset_type_column} = ?")
        parameters.append("stock")
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = connection.execute(
        f"""
        SELECT
            {code_column} AS instrument,
            {asof_date_column} AS signal_date,
            {asof_date_column} AS asof_date,
            COALESCE(current_hh_count, 0) AS current_hh_count,
            COALESCE(current_ll_count, 0) AS current_ll_count,
            major_state,
            trend_direction,
            reversal_stage
        FROM {table_name}
        {where_sql}
        ORDER BY {asof_date_column}, {code_column}
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    input_rows: list[_StructureInputRow] = []
    for row in rows:
        major_state = _normalize_optional_str(row[5], default="牛逆")
        trend_direction = _normalize_optional_str(row[6], default="down").lower()
        reversal_stage = _normalize_optional_str(row[7], default="none").lower()
        bullish_context = _map_major_state_to_context_code(major_state).startswith("BULL_")
        new_high_count = _normalize_optional_int(row[3]) if bullish_context else 0
        new_low_count = _normalize_optional_int(row[4]) if not bullish_context else 0
        refresh_density = min(float(new_high_count) / 4.0, 1.0) if new_high_count > 0 else 0.0
        advancement_density = 1.0 if trend_direction == "up" and reversal_stage in {"none", "expand"} else 0.0
        failure_type = _derive_failure_type_from_major_state(major_state)
        input_rows.append(
            _StructureInputRow(
                instrument=str(row[0]),
                signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                new_high_count=new_high_count,
                new_low_count=new_low_count,
                refresh_density=refresh_density,
                advancement_density=advancement_density,
                is_failed_extreme=failure_type is not None,
                failure_type=failure_type,
            )
        )
    return input_rows


def _load_canonical_context_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> list[_StructureContextRow]:
    available_columns = _load_table_columns(connection, table_name)
    code_column = _resolve_existing_column(
        available_columns,
        ("code", "instrument", "entity_code"),
        field_name="code",
        table_name=table_name,
    )
    asof_date_column = _resolve_existing_column(
        available_columns,
        ("asof_bar_dt", "asof_date", "signal_date"),
        field_name="asof_bar_dt",
        table_name=table_name,
    )
    snapshot_nk_column = _resolve_optional_column(available_columns, ("snapshot_nk",))
    timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
    asset_type_column = _resolve_optional_column(available_columns, ("asset_type",))
    parameters: list[object] = []
    where_clauses: list[str] = []
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{code_column} IN ({placeholders})")
        parameters.extend(instruments)
    if signal_start_date is not None:
        where_clauses.append(f"{asof_date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{asof_date_column} <= ?")
        parameters.append(signal_end_date)
    if timeframe_column is not None:
        where_clauses.append(f"{timeframe_column} = ?")
        parameters.append(timeframe)
    if asset_type_column is not None:
        where_clauses.append(f"{asset_type_column} = ?")
        parameters.append("stock")
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = connection.execute(
        f"""
        SELECT
            {code_column} AS instrument,
            {asof_date_column} AS signal_date,
            {asof_date_column} AS asof_date,
            major_state,
            trend_direction,
            reversal_stage,
            COALESCE(wave_id, 0) AS wave_id,
            COALESCE(current_hh_count, 0) AS current_hh_count,
            COALESCE(current_ll_count, 0) AS current_ll_count,
            {snapshot_nk_column if snapshot_nk_column is not None else "NULL"} AS snapshot_nk
        FROM {table_name}
        {where_sql}
        ORDER BY {asof_date_column}, {code_column}
        """,
        parameters,
    ).fetchall()
    context_rows: list[_StructureContextRow] = []
    for row in rows:
        signal_date_value = _normalize_date_value(row[1], field_name="signal_date")
        asof_date_value = _normalize_date_value(row[2], field_name="asof_date")
        major_state = _normalize_optional_str(row[3], default="牛逆")
        trend_direction = _normalize_optional_str(row[4], default="down").lower()
        reversal_stage = _normalize_optional_str(row[5], default="none").lower()
        wave_id = _normalize_optional_int(row[6])
        current_hh_count = _normalize_optional_int(row[7])
        current_ll_count = _normalize_optional_int(row[8])
        source_context_nk = _normalize_optional_str(
            row[9],
            default=_build_canonical_context_nk(
                instrument=str(row[0]),
                signal_date=signal_date_value,
                asof_date=asof_date_value,
                major_state=major_state,
                trend_direction=trend_direction,
                reversal_stage=reversal_stage,
                wave_id=wave_id,
            ),
        )
        context_rows.append(
            _StructureContextRow(
                instrument=str(row[0]),
                signal_date=signal_date_value,
                asof_date=asof_date_value,
                major_state=major_state,
                trend_direction=trend_direction,
                reversal_stage=reversal_stage,
                wave_id=wave_id,
                current_hh_count=current_hh_count,
                current_ll_count=current_ll_count,
                source_context_nk=source_context_nk,
            )
        )
    return context_rows
