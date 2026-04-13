"""`alpha formal signal` 的队列、checkpoint 与官方上游读取。"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import duckdb

from mlq.alpha.bootstrap import (
    ALPHA_FAMILY_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_CHECKPOINT_TABLE,
    ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE,
    ALPHA_TRIGGER_CHECKPOINT_TABLE,
)
from mlq.alpha.formal_signal_shared import (
    _ContextRow,
    _FamilyRow,
    _TriggerRow,
    _build_alpha_formal_signal_run_id,
    _build_queue_nk,
    _build_scope_nk,
    _derive_lifecycle_rank_high,
    _map_major_state_to_context_code,
    _normalize_date_value,
    _normalize_formal_signal_status,
    _normalize_optional_int,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
    _to_python_date,
)


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _load_alpha_formal_signal_dirty_scopes(
    *,
    connection: duckdb.DuckDBPyConnection,
    limit: int,
) -> list[dict[str, object]]:
    rows = connection.execute(
        f"""
        SELECT code, last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id
        FROM {ALPHA_TRIGGER_CHECKPOINT_TABLE}
        WHERE timeframe = 'D'
        ORDER BY code
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    scope_rows: list[dict[str, object]] = []
    for row in rows:
        replay_end = _to_python_date(row[1])
        if replay_end is None:
            continue
        family_scope_fingerprint = _build_family_scope_fingerprint(
            connection=connection,
            instrument=str(row[0]),
            replay_start_bar_dt=_to_python_date(row[2]) or replay_end,
            replay_confirm_until_dt=replay_end,
        )
        source_fingerprint = json.dumps(
            {
                "last_completed_bar_dt": replay_end.isoformat(),
                "tail_start_bar_dt": None if _to_python_date(row[2]) is None else _to_python_date(row[2]).isoformat(),
                "tail_confirm_until_dt": None if _to_python_date(row[3]) is None else _to_python_date(row[3]).isoformat(),
                "source_fingerprint": str(row[4]),
                "last_run_id": None if row[5] is None else str(row[5]),
                "family_scope_fingerprint": family_scope_fingerprint,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        scope_rows.append(
            {
                "scope_nk": _build_scope_nk(asset_type="stock", code=str(row[0]), timeframe="D"),
                "asset_type": "stock",
                "code": str(row[0]),
                "timeframe": "D",
                "replay_start_bar_dt": _to_python_date(row[2]) or replay_end,
                "replay_confirm_until_dt": replay_end,
                "source_fingerprint": source_fingerprint,
            }
        )
    return scope_rows


def _build_family_scope_fingerprint(
    *,
    connection: duckdb.DuckDBPyConnection,
    instrument: str,
    replay_start_bar_dt: date,
    replay_confirm_until_dt: date,
    table_name: str = ALPHA_FAMILY_EVENT_TABLE,
) -> dict[str, object]:
    if not _table_exists(connection, table_name):
        return {"family_table_present": False}
    rows = connection.execute(
        f"""
        SELECT
            family_event_nk,
            trigger_event_nk,
            family_code,
            family_contract_version,
            payload_json,
            last_materialized_run_id
        FROM {table_name}
        WHERE instrument = ?
          AND signal_date >= ?
          AND signal_date <= ?
        ORDER BY signal_date, trigger_event_nk, family_event_nk
        """,
        [instrument, replay_start_bar_dt, replay_confirm_until_dt],
    ).fetchall()
    return {
        "family_table_present": True,
        "family_event_count": len(rows),
        "family_events": [
            {
                "family_event_nk": str(row[0]),
                "trigger_event_nk": str(row[1]),
                "family_code": _normalize_optional_nullable_str(row[2]),
                "family_contract_version": _normalize_optional_nullable_str(row[3]),
                "payload_sha256": hashlib.sha256(_normalize_optional_str(row[4]).encode("utf-8")).hexdigest(),
                "last_materialized_run_id": _normalize_optional_nullable_str(row[5]),
            }
            for row in rows
        ],
    }


def _enqueue_alpha_formal_signal_dirty_scopes(
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
            FROM {ALPHA_FORMAL_SIGNAL_CHECKPOINT_TABLE}
            WHERE asset_type = ?
              AND code = ?
              AND timeframe = ?
            """,
            [scope_row["asset_type"], scope_row["code"], scope_row["timeframe"]],
        ).fetchone()
        dirty_reason = _derive_alpha_formal_signal_dirty_reason(
            checkpoint_row=checkpoint_row,
            replay_start_bar_dt=_to_python_date(scope_row["replay_start_bar_dt"]),
            replay_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
            source_fingerprint=str(scope_row["source_fingerprint"]),
        )
        if dirty_reason is None:
            continue
        queue_nk = _build_queue_nk(scope_nk=str(scope_row["scope_nk"]), dirty_reason=dirty_reason)
        existing = connection.execute(
            f"SELECT queue_nk FROM {ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
            [queue_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE} (
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
                    _to_python_date(scope_row["replay_start_bar_dt"]),
                    _to_python_date(scope_row["replay_confirm_until_dt"]),
                    scope_row["source_fingerprint"],
                    run_id,
                    run_id,
                ],
            )
            queue_enqueued_count += 1
            continue
        connection.execute(
            f"""
            UPDATE {ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE}
            SET replay_start_bar_dt = ?,
                replay_confirm_until_dt = ?,
                source_fingerprint = ?,
                queue_status = 'pending',
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [
                _to_python_date(scope_row["replay_start_bar_dt"]),
                _to_python_date(scope_row["replay_confirm_until_dt"]),
                scope_row["source_fingerprint"],
                queue_nk,
            ],
        )
    return {"queue_enqueued_count": queue_enqueued_count}


def _derive_alpha_formal_signal_dirty_reason(
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


def _claim_alpha_formal_signal_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
) -> list[dict[str, object]]:
    rows = connection.execute(
        f"""
        SELECT queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
               replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint
        FROM {ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE}
        WHERE queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY code, enqueued_at
        """
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE}
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


def _mark_alpha_formal_signal_queue_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE}
        SET queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _mark_alpha_formal_signal_queue_failed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE}
        SET queue_status = 'failed',
            last_claimed_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _upsert_alpha_formal_signal_checkpoint(
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
        FROM {ALPHA_FORMAL_SIGNAL_CHECKPOINT_TABLE}
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [asset_type, code, timeframe],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {ALPHA_FORMAL_SIGNAL_CHECKPOINT_TABLE} (
                asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt,
                tail_confirm_until_dt, source_fingerprint, last_run_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id],
        )
        return
    connection.execute(
        f"""
        UPDATE {ALPHA_FORMAL_SIGNAL_CHECKPOINT_TABLE}
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
        [last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id, asset_type, code, timeframe],
    )


def _load_family_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    trigger_rows: list[_TriggerRow],
) -> dict[str, _FamilyRow]:
    if not trigger_rows or not _table_exists(connection, table_name):
        return {}
    trigger_event_nks = tuple(
        dict.fromkeys(row.source_trigger_event_nk for row in trigger_rows if row.source_trigger_event_nk)
    )
    if not trigger_event_nks:
        return {}
    placeholders = ", ".join("?" for _ in trigger_event_nks)
    rows = connection.execute(
        f"""
        WITH ranked_family AS (
            SELECT
                family_event_nk,
                trigger_event_nk,
                family_code,
                family_contract_version,
                payload_json,
                ROW_NUMBER() OVER (
                    PARTITION BY trigger_event_nk
                    ORDER BY updated_at DESC, last_materialized_run_id DESC, family_event_nk DESC
                ) AS row_rank
            FROM {table_name}
            WHERE trigger_event_nk IN ({placeholders})
        )
        SELECT
            family_event_nk,
            trigger_event_nk,
            family_code,
            family_contract_version,
            payload_json
        FROM ranked_family
        WHERE row_rank = 1
        """,
        [*trigger_event_nks],
    ).fetchall()
    family_map: dict[str, _FamilyRow] = {}
    for row in rows:
        payload = _parse_payload_json(row[4])
        family_map[str(row[1])] = _FamilyRow(
            source_family_event_nk=str(row[0]),
            source_trigger_event_nk=str(row[1]),
            family_code=_normalize_optional_nullable_str(row[2]),
            source_family_contract_version=_normalize_optional_nullable_str(row[3]),
            family_role=_normalize_optional_nullable_str(payload.get("family_role")),
            family_bias=_normalize_optional_nullable_str(payload.get("family_bias")),
            malf_alignment=_normalize_optional_nullable_str(payload.get("malf_alignment")),
            malf_phase_bucket=_normalize_optional_nullable_str(payload.get("malf_phase_bucket")),
            family_source_context_fingerprint=_normalize_optional_nullable_str(
                payload.get("source_context_fingerprint")
            ),
        )
    return family_map


def _load_trigger_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_TriggerRow]:
    available_columns = _load_table_columns(connection, table_name)
    signal_date_column = _resolve_existing_column(
        available_columns,
        ("signal_date",),
        field_name="signal_date",
        table_name=table_name,
    )
    instrument_column = _resolve_existing_column(
        available_columns,
        ("instrument", "code"),
        field_name="instrument",
        table_name=table_name,
    )
    select_sql = _build_trigger_select_sql(table_name=table_name, available_columns=available_columns)
    parameters: list[object] = []
    where_clauses: list[str] = []
    if signal_start_date is not None:
        where_clauses.append(f"{signal_date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{signal_date_column} <= ?")
        parameters.append(signal_end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{instrument_column} IN ({placeholders})")
        parameters.extend(instruments)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = connection.execute(
        f"""
        {select_sql}
        {where_sql}
        ORDER BY signal_date, instrument, source_trigger_event_nk
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        _TriggerRow(
            source_trigger_event_nk=str(row[0]),
            instrument=str(row[1]),
            signal_date=_normalize_date_value(row[2], field_name="signal_date"),
            asof_date=_normalize_date_value(row[3], field_name="asof_date"),
            trigger_family=str(row[4]),
            trigger_type=str(row[5]),
            pattern_code=str(row[6]),
        )
        for row in rows
    ]


def _build_trigger_select_sql(*, table_name: str, available_columns: set[str]) -> str:
    source_trigger_column = _resolve_existing_column(
        available_columns,
        ("source_trigger_event_nk", "signal_id", "trigger_event_nk"),
        field_name="source_trigger_event_nk",
        table_name=table_name,
    )
    instrument_column = _resolve_existing_column(
        available_columns,
        ("instrument", "code"),
        field_name="instrument",
        table_name=table_name,
    )
    signal_date_column = _resolve_existing_column(
        available_columns,
        ("signal_date",),
        field_name="signal_date",
        table_name=table_name,
    )
    asof_date_column = _resolve_optional_column(available_columns, ("asof_date",)) or signal_date_column
    trigger_family_column = _resolve_optional_column(available_columns, ("trigger_family",))
    trigger_type_column = _resolve_existing_column(
        available_columns,
        ("trigger_type",),
        field_name="trigger_type",
        table_name=table_name,
    )
    pattern_code_column = _resolve_existing_column(
        available_columns,
        ("pattern_code", "pattern", "trigger_type"),
        field_name="pattern_code",
        table_name=table_name,
    )
    return f"""
        SELECT
            {source_trigger_column} AS source_trigger_event_nk,
            {instrument_column} AS instrument,
            {signal_date_column} AS signal_date,
            {asof_date_column} AS asof_date,
            {("'PAS'" if trigger_family_column is None else trigger_family_column)} AS trigger_family,
            {trigger_type_column} AS trigger_type,
            {pattern_code_column} AS pattern_code
        FROM {table_name}
    """


def _load_official_context_rows(
    *,
    filter_path: Path,
    structure_path: Path,
    filter_table_name: str,
    structure_table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> list[_ContextRow]:
    if not instruments:
        return []
    connection = duckdb.connect(str(filter_path), read_only=False)
    try:
        structure_path_sql = str(structure_path).replace("\\", "/").replace("'", "''")
        connection.execute(f"ATTACH '{structure_path_sql}' AS structure_db")
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"instrument IN ({placeholders})"]
        if signal_start_date is not None:
            where_clauses.append("signal_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("signal_date <= ?")
            parameters.append(signal_end_date)
        rows = connection.execute(
            f"""
            WITH ranked_filter AS (
                SELECT
                    instrument,
                    signal_date,
                    asof_date,
                    structure_snapshot_nk,
                    trigger_admissible,
                    daily_source_context_nk,
                    weekly_major_state,
                    weekly_trend_direction,
                    weekly_reversal_stage,
                    weekly_source_context_nk,
                    monthly_major_state,
                    monthly_trend_direction,
                    monthly_reversal_stage,
                    monthly_source_context_nk,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument, signal_date, asof_date
                        ORDER BY last_materialized_run_id DESC
                    ) AS row_rank
                FROM {filter_table_name}
                WHERE {' AND '.join(where_clauses)}
            )
            SELECT
                rf.instrument,
                rf.signal_date,
                rf.asof_date,
                CASE WHEN rf.trigger_admissible THEN 'admitted' ELSE 'blocked' END AS formal_signal_status,
                rf.trigger_admissible,
                s.major_state,
                s.trend_direction,
                s.reversal_stage,
                s.wave_id,
                s.current_hh_count,
                s.current_ll_count,
                rf.daily_source_context_nk,
                rf.weekly_major_state,
                rf.weekly_trend_direction,
                rf.weekly_reversal_stage,
                rf.weekly_source_context_nk,
                rf.monthly_major_state,
                rf.monthly_trend_direction,
                rf.monthly_reversal_stage,
                rf.monthly_source_context_nk
            FROM ranked_filter AS rf
            INNER JOIN structure_db.main.{structure_table_name} AS s
                ON s.structure_snapshot_nk = rf.structure_snapshot_nk
            WHERE rf.row_rank = 1
            """,
            parameters,
        ).fetchall()
        return [
            _build_context_row(
                instrument=str(row[0]),
                signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                formal_signal_status=_normalize_formal_signal_status(row[3]),
                trigger_admissible=bool(row[4]),
                major_state=_normalize_optional_str(row[5], default="牛逆"),
                trend_direction=_normalize_optional_str(row[6], default="down").lower(),
                reversal_stage=_normalize_optional_str(row[7], default="none").lower(),
                wave_id=_normalize_optional_int(row[8]),
                current_hh_count=_normalize_optional_int(row[9]),
                current_ll_count=_normalize_optional_int(row[10]),
                daily_source_context_nk=_normalize_optional_nullable_str(row[11]),
                weekly_major_state=_normalize_optional_nullable_str(row[12]),
                weekly_trend_direction=_normalize_optional_nullable_str(row[13]),
                weekly_reversal_stage=_normalize_optional_nullable_str(row[14]),
                weekly_source_context_nk=_normalize_optional_nullable_str(row[15]),
                monthly_major_state=_normalize_optional_nullable_str(row[16]),
                monthly_trend_direction=_normalize_optional_nullable_str(row[17]),
                monthly_reversal_stage=_normalize_optional_nullable_str(row[18]),
                monthly_source_context_nk=_normalize_optional_nullable_str(row[19]),
            )
            for row in rows
        ]
    finally:
        connection.close()


def _build_context_row(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    formal_signal_status: str,
    trigger_admissible: bool,
    major_state: str,
    trend_direction: str,
    reversal_stage: str,
    wave_id: int,
    current_hh_count: int,
    current_ll_count: int,
    daily_source_context_nk: str | None,
    weekly_major_state: str | None,
    weekly_trend_direction: str | None,
    weekly_reversal_stage: str | None,
    weekly_source_context_nk: str | None,
    monthly_major_state: str | None,
    monthly_trend_direction: str | None,
    monthly_reversal_stage: str | None,
    monthly_source_context_nk: str | None,
) -> _ContextRow:
    malf_context_4 = _map_major_state_to_context_code(major_state)
    lifecycle_rank_high = _derive_lifecycle_rank_high(
        malf_context_4=malf_context_4,
        current_hh_count=current_hh_count,
        current_ll_count=current_ll_count,
    )
    return _ContextRow(
        instrument=instrument,
        signal_date=signal_date,
        asof_date=asof_date,
        formal_signal_status=formal_signal_status,
        trigger_admissible=trigger_admissible,
        major_state=major_state,
        trend_direction=trend_direction,
        reversal_stage=reversal_stage,
        wave_id=wave_id,
        current_hh_count=current_hh_count,
        current_ll_count=current_ll_count,
        malf_context_4=malf_context_4,
        lifecycle_rank_high=lifecycle_rank_high,
        lifecycle_rank_total=4,
        daily_source_context_nk=daily_source_context_nk,
        weekly_major_state=weekly_major_state,
        weekly_trend_direction=weekly_trend_direction,
        weekly_reversal_stage=weekly_reversal_stage,
        weekly_source_context_nk=weekly_source_context_nk,
        monthly_major_state=monthly_major_state,
        monthly_trend_direction=monthly_trend_direction,
        monthly_reversal_stage=monthly_reversal_stage,
        monthly_source_context_nk=monthly_source_context_nk,
    )


def _load_table_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchall()
    if not rows:
        raise ValueError(f"Missing table: {table_name}")
    return {str(row[0]) for row in rows}


def _resolve_existing_column(
    available_columns: set[str],
    candidates: tuple[str, ...],
    *,
    field_name: str,
    table_name: str,
) -> str:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    raise ValueError(f"Missing required column `{field_name}` in table `{table_name}`.")


def _resolve_optional_column(available_columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    return None


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


def _parse_payload_json(payload_json: object) -> dict[str, object]:
    if payload_json is None:
        return {}
    try:
        parsed = json.loads(str(payload_json))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
