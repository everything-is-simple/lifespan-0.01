"""`portfolio_plan` runner 的 queue / checkpoint 辅助。"""

from __future__ import annotations

import json
from datetime import date

import duckdb

from .bootstrap import (
    PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE,
    PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_CHECKPOINT_TABLE,
    PORTFOLIO_PLAN_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_WORK_QUEUE_TABLE,
)
from .materialization import _PositionBridgeRow
from .runner_source import normalize_date_value


def build_queue_nk(
    *,
    portfolio_id: str,
    candidate_nk: str,
    reference_trade_date: date,
) -> str:
    return f"{portfolio_id}|{candidate_nk}|{reference_trade_date.isoformat()}"


def build_candidate_checkpoint_nk(*, portfolio_id: str, candidate_nk: str) -> str:
    return f"{portfolio_id}|candidate|{candidate_nk}"


def build_portfolio_checkpoint_nk(
    *,
    portfolio_id: str,
    checkpoint_scope: str,
) -> str:
    return f"{portfolio_id}|{checkpoint_scope}"


def build_source_fingerprint(
    *,
    bridge_row: _PositionBridgeRow,
    portfolio_gross_cap_weight: float,
    portfolio_plan_contract_version: str,
) -> str:
    payload = {
        "binding_cap_code": bridge_row.binding_cap_code,
        "blocked_reason_code": bridge_row.blocked_reason_code,
        "candidate_nk": bridge_row.candidate_nk,
        "candidate_status": bridge_row.candidate_status,
        "capacity_source_code": bridge_row.capacity_source_code,
        "contract_version": portfolio_plan_contract_version,
        "final_allowed_position_weight": round(
            float(bridge_row.final_allowed_position_weight),
            12,
        ),
        "instrument": bridge_row.instrument,
        "policy_id": bridge_row.policy_id,
        "portfolio_gross_cap_weight": round(float(portfolio_gross_cap_weight), 12),
        "position_action_decision": bridge_row.position_action_decision,
        "reference_trade_date": bridge_row.reference_trade_date.isoformat(),
        "remaining_portfolio_capacity_weight": round(
            float(bridge_row.remaining_portfolio_capacity_weight),
            12,
        ),
        "remaining_single_name_capacity_weight": round(
            float(bridge_row.remaining_single_name_capacity_weight),
            12,
        ),
        "required_reduction_weight": round(
            float(bridge_row.required_reduction_weight),
            12,
        ),
        "schedule_lag_days": int(bridge_row.schedule_lag_days),
        "schedule_stage": bridge_row.schedule_stage,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def load_portfolio_plan_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    checkpoint_nk: str,
) -> dict[str, object] | None:
    row = connection.execute(
        f"""
        SELECT
            checkpoint_nk,
            portfolio_id,
            checkpoint_scope,
            COALESCE(last_completed_reference_trade_date, latest_reference_trade_date),
            COALESCE(last_completed_candidate_nk, last_candidate_nk),
            COALESCE(last_run_id, last_success_run_id),
            checkpoint_payload_json
        FROM {PORTFOLIO_PLAN_CHECKPOINT_TABLE}
        WHERE checkpoint_nk = ?
        """,
        [checkpoint_nk],
    ).fetchone()
    if row is None:
        return None
    return {
        "checkpoint_nk": str(row[0]),
        "portfolio_id": str(row[1]),
        "checkpoint_scope": str(row[2]),
        "last_completed_reference_trade_date": (
            None
            if row[3] is None
            else normalize_date_value(row[3], field_name="checkpoint.reference_trade_date")
        ),
        "last_completed_candidate_nk": None if row[4] is None else str(row[4]),
        "last_run_id": None if row[5] is None else str(row[5]),
        "checkpoint_payload_json": None if row[6] is None else str(row[6]),
    }


def load_candidate_scope_presence(
    connection: duckdb.DuckDBPyConnection,
    *,
    portfolio_id: str,
    candidate_nk: str,
    reference_trade_date: date,
    portfolio_plan_contract_version: str,
) -> dict[str, bool]:
    row = connection.execute(
        f"""
        SELECT
            EXISTS(
                SELECT 1
                FROM {PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE}
                WHERE portfolio_id = ?
                  AND candidate_nk = ?
                  AND reference_trade_date = ?
                  AND portfolio_plan_contract_version = ?
            ),
            EXISTS(
                SELECT 1
                FROM {PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE}
                WHERE portfolio_id = ?
                  AND candidate_nk = ?
                  AND reference_trade_date = ?
                  AND portfolio_plan_contract_version = ?
            ),
            EXISTS(
                SELECT 1
                FROM {PORTFOLIO_PLAN_SNAPSHOT_TABLE}
                WHERE portfolio_id = ?
                  AND candidate_nk = ?
                  AND reference_trade_date = ?
                  AND portfolio_plan_contract_version = ?
            ),
            EXISTS(
                SELECT 1
                FROM {PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE}
                WHERE portfolio_id = ?
                  AND reference_trade_date = ?
                  AND portfolio_plan_contract_version = ?
            )
        """,
        [
            portfolio_id,
            candidate_nk,
            reference_trade_date,
            portfolio_plan_contract_version,
            portfolio_id,
            candidate_nk,
            reference_trade_date,
            portfolio_plan_contract_version,
            portfolio_id,
            candidate_nk,
            reference_trade_date,
            portfolio_plan_contract_version,
            portfolio_id,
            reference_trade_date,
            portfolio_plan_contract_version,
        ],
    ).fetchone()
    return {
        "decision_present": bool(row[0]),
        "allocation_present": bool(row[1]),
        "snapshot_present": bool(row[2]),
        "capacity_present": bool(row[3]),
    }


def derive_queue_reason(
    *,
    checkpoint_row: dict[str, object] | None,
    candidate_scope_presence: dict[str, bool],
    source_fingerprint: str,
    force_replay: bool,
) -> str | None:
    if force_replay:
        return "replay_request"
    if checkpoint_row is None:
        return "bootstrap_missing_checkpoint"
    if not all(candidate_scope_presence.values()):
        return "missing_materialization"
    checkpoint_payload_json = checkpoint_row.get("checkpoint_payload_json")
    checkpoint_payload = (
        {}
        if not checkpoint_payload_json
        else json.loads(str(checkpoint_payload_json))
    )
    if str(checkpoint_payload.get("source_fingerprint", "")) != source_fingerprint:
        return "source_fingerprint_changed"
    return None


def enqueue_portfolio_plan_dirty_candidates(
    connection: duckdb.DuckDBPyConnection,
    *,
    bridge_rows: list[_PositionBridgeRow],
    run_id: str,
    portfolio_id: str,
    portfolio_plan_contract_version: str,
    portfolio_gross_cap_weight: float,
    force_replay: bool,
) -> int:
    queue_enqueued_count = 0
    for bridge_row in bridge_rows:
        checkpoint_nk = build_candidate_checkpoint_nk(
            portfolio_id=portfolio_id,
            candidate_nk=bridge_row.candidate_nk,
        )
        checkpoint_row = load_portfolio_plan_checkpoint(
            connection,
            checkpoint_nk=checkpoint_nk,
        )
        source_fingerprint = build_source_fingerprint(
            bridge_row=bridge_row,
            portfolio_gross_cap_weight=portfolio_gross_cap_weight,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
        )
        presence = load_candidate_scope_presence(
            connection,
            portfolio_id=portfolio_id,
            candidate_nk=bridge_row.candidate_nk,
            reference_trade_date=bridge_row.reference_trade_date,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
        )
        queue_reason = derive_queue_reason(
            checkpoint_row=checkpoint_row,
            candidate_scope_presence=presence,
            source_fingerprint=source_fingerprint,
            force_replay=force_replay,
        )
        if queue_reason is None:
            continue
        queue_nk = build_queue_nk(
            portfolio_id=portfolio_id,
            candidate_nk=bridge_row.candidate_nk,
            reference_trade_date=bridge_row.reference_trade_date,
        )
        existing = connection.execute(
            f"SELECT queue_nk FROM {PORTFOLIO_PLAN_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
            [queue_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {PORTFOLIO_PLAN_WORK_QUEUE_TABLE} (
                    queue_nk,
                    portfolio_id,
                    candidate_nk,
                    reference_trade_date,
                    checkpoint_nk,
                    queue_reason,
                    queued_at,
                    queue_status,
                    source_fingerprint,
                    source_run_id,
                    source_candidate_nk,
                    first_enqueued_at,
                    last_enqueued_at
                )
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'pending', ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [
                    queue_nk,
                    portfolio_id,
                    bridge_row.candidate_nk,
                    bridge_row.reference_trade_date,
                    checkpoint_nk,
                    queue_reason,
                    source_fingerprint,
                    run_id,
                    bridge_row.candidate_nk,
                ],
            )
        else:
            connection.execute(
                f"""
                UPDATE {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
                SET
                    reference_trade_date = ?,
                    checkpoint_nk = ?,
                    queue_reason = ?,
                    queued_at = CURRENT_TIMESTAMP,
                    queue_status = 'pending',
                    source_fingerprint = ?,
                    source_run_id = ?,
                    source_candidate_nk = ?,
                    last_enqueued_at = CURRENT_TIMESTAMP,
                    last_error_text = NULL
                WHERE queue_nk = ?
                """,
                [
                    bridge_row.reference_trade_date,
                    checkpoint_nk,
                    queue_reason,
                    source_fingerprint,
                    run_id,
                    bridge_row.candidate_nk,
                    queue_nk,
                ],
            )
        queue_enqueued_count += 1
    return queue_enqueued_count


def claim_portfolio_plan_queue_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    portfolio_id: str,
    candidate_nks: set[str],
    limit: int,
) -> list[dict[str, object]]:
    if not candidate_nks:
        return []
    placeholders = ", ".join("?" for _ in candidate_nks)
    rows = connection.execute(
        f"""
        SELECT
            queue_nk,
            candidate_nk,
            reference_trade_date,
            checkpoint_nk,
            queue_reason,
            source_fingerprint
        FROM {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
        WHERE portfolio_id = ?
          AND candidate_nk IN ({placeholders})
          AND queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY reference_trade_date, candidate_nk, COALESCE(queued_at, last_enqueued_at, first_enqueued_at)
        LIMIT ?
        """,
        [portfolio_id, *sorted(candidate_nks), limit],
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
            SET
                queue_status = 'claimed',
                claimed_at = CURRENT_TIMESTAMP,
                last_claimed_run_id = ?,
                last_error_text = NULL
            WHERE queue_nk = ?
            """,
            [run_id, str(row[0])],
        )
        claimed_rows.append(
            {
                "queue_nk": str(row[0]),
                "candidate_nk": str(row[1]),
                "reference_trade_date": normalize_date_value(
                    row[2],
                    field_name="queue.reference_trade_date",
                ),
                "checkpoint_nk": str(row[3]),
                "queue_reason": str(row[4]),
                "source_fingerprint": None if row[5] is None else str(row[5]),
            }
        )
    return claimed_rows


def mark_queue_rows_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nks: list[str],
    run_id: str,
) -> None:
    if not queue_nks:
        return
    placeholders = ", ".join("?" for _ in queue_nks)
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
        SET
            queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_success_run_id = ?,
            last_error_text = NULL
        WHERE queue_nk IN ({placeholders})
        """,
        [run_id, *queue_nks],
    )


def mark_queue_rows_failed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nks: list[str],
    run_id: str,
    error_text: str,
) -> None:
    if not queue_nks:
        return
    placeholders = ", ".join("?" for _ in queue_nks)
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
        SET
            queue_status = 'failed',
            last_claimed_run_id = ?,
            last_error_text = ?
        WHERE queue_nk IN ({placeholders})
        """,
        [run_id, error_text, *queue_nks],
    )


def delete_stale_date_scope_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    portfolio_id: str,
    reference_trade_date: date,
    current_candidate_nks: set[str],
    portfolio_plan_contract_version: str,
) -> None:
    rows = connection.execute(
        f"""
        SELECT candidate_nk
        FROM {PORTFOLIO_PLAN_SNAPSHOT_TABLE}
        WHERE portfolio_id = ?
          AND reference_trade_date = ?
          AND portfolio_plan_contract_version = ?
        """,
        [portfolio_id, reference_trade_date, portfolio_plan_contract_version],
    ).fetchall()
    stale_candidate_nks = sorted(
        {
            str(row[0])
            for row in rows
            if str(row[0]) not in current_candidate_nks
        }
    )
    if not stale_candidate_nks:
        return
    placeholders = ", ".join("?" for _ in stale_candidate_nks)
    parameters = [
        portfolio_id,
        reference_trade_date,
        portfolio_plan_contract_version,
        *stale_candidate_nks,
    ]
    for table_name in (
        PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE,
        PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE,
        PORTFOLIO_PLAN_SNAPSHOT_TABLE,
    ):
        connection.execute(
            f"""
            DELETE FROM {table_name}
            WHERE portfolio_id = ?
              AND reference_trade_date = ?
              AND portfolio_plan_contract_version = ?
              AND candidate_nk IN ({placeholders})
            """,
            parameters,
        )


def upsert_portfolio_plan_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    checkpoint_nk: str,
    portfolio_id: str,
    checkpoint_scope: str,
    last_completed_reference_trade_date: date | None,
    last_completed_candidate_nk: str | None,
    last_run_id: str,
    checkpoint_payload: dict[str, object],
) -> None:
    existing = connection.execute(
        f"SELECT checkpoint_nk FROM {PORTFOLIO_PLAN_CHECKPOINT_TABLE} WHERE checkpoint_nk = ?",
        [checkpoint_nk],
    ).fetchone()
    payload_json = json.dumps(checkpoint_payload, ensure_ascii=False, sort_keys=True)
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {PORTFOLIO_PLAN_CHECKPOINT_TABLE} (
                checkpoint_nk,
                portfolio_id,
                checkpoint_scope,
                latest_reference_trade_date,
                last_candidate_nk,
                last_completed_reference_trade_date,
                last_completed_candidate_nk,
                last_success_run_id,
                last_run_id,
                checkpoint_payload_json,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                checkpoint_nk,
                portfolio_id,
                checkpoint_scope,
                last_completed_reference_trade_date,
                last_completed_candidate_nk,
                last_completed_reference_trade_date,
                last_completed_candidate_nk,
                last_run_id,
                last_run_id,
                payload_json,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_CHECKPOINT_TABLE}
        SET
            portfolio_id = ?,
            checkpoint_scope = ?,
            latest_reference_trade_date = ?,
            last_candidate_nk = ?,
            last_completed_reference_trade_date = ?,
            last_completed_candidate_nk = ?,
            last_success_run_id = ?,
            last_run_id = ?,
            checkpoint_payload_json = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE checkpoint_nk = ?
        """,
        [
            portfolio_id,
            checkpoint_scope,
            last_completed_reference_trade_date,
            last_completed_candidate_nk,
            last_completed_reference_trade_date,
            last_completed_candidate_nk,
            last_run_id,
            last_run_id,
            payload_json,
            checkpoint_nk,
        ],
    )
