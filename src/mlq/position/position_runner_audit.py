"""承载 `position` data-grade runner 的 queue/checkpoint/run 审计 helper。"""

from __future__ import annotations

import json
from datetime import date

import duckdb

from mlq.position.position_bootstrap_schema import (
    POSITION_CHECKPOINT_TABLE,
    POSITION_RUN_SNAPSHOT_TABLE,
    POSITION_RUN_TABLE,
    POSITION_WORK_QUEUE_TABLE,
)
from mlq.position.position_runner_shared import (
    DEFAULT_POSITION_RUNNER_NAME,
    DEFAULT_POSITION_RUNNER_VERSION,
    PositionCandidateMaterializationInput,
    PositionFormalSignalRunnerSummary,
    coerce_date,
)
from mlq.position.position_runner_support import (
    load_candidate_scope_stats,
    load_position_checkpoint,
)


def derive_position_queue_reason(
    *,
    checkpoint_row: dict[str, object] | None,
    candidate_stats: dict[str, object],
    source_signal_fingerprint: str,
) -> str | None:
    if checkpoint_row is None:
        return "bootstrap_missing_checkpoint"
    if str(checkpoint_row["last_source_signal_fingerprint"]) != source_signal_fingerprint:
        return "source_fingerprint_changed"
    if not bool(candidate_stats["core_complete"]):
        return "missing_materialization"
    return None


def build_queue_nk(*, candidate_nk: str, queue_reason: str) -> str:
    return f"{candidate_nk}|{queue_reason}"


def enqueue_position_dirty_candidates(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidates: list[PositionCandidateMaterializationInput],
    run_id: str,
) -> int:
    queue_enqueued_count = 0
    for candidate in candidates:
        checkpoint_row = load_position_checkpoint(connection, checkpoint_nk=candidate.checkpoint_nk)
        candidate_stats = load_candidate_scope_stats(connection, candidate_nk=candidate.candidate_nk)
        queue_reason = derive_position_queue_reason(
            checkpoint_row=checkpoint_row,
            candidate_stats=candidate_stats,
            source_signal_fingerprint=candidate.source_signal_fingerprint,
        )
        if queue_reason is None:
            continue
        queue_nk = build_queue_nk(candidate_nk=candidate.candidate_nk, queue_reason=queue_reason)
        existing = connection.execute(
            f"SELECT queue_nk FROM {POSITION_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
            [queue_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {POSITION_WORK_QUEUE_TABLE} (
                    queue_nk,
                    candidate_nk,
                    checkpoint_nk,
                    signal_nk,
                    instrument,
                    reference_trade_date,
                    source_signal_fingerprint,
                    queue_reason,
                    queue_status,
                    first_seen_run_id,
                    last_materialized_run_id,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    queue_nk,
                    candidate.candidate_nk,
                    candidate.checkpoint_nk,
                    candidate.signal.signal_nk,
                    candidate.signal.instrument,
                    coerce_date(candidate.signal.reference_trade_date),
                    candidate.source_signal_fingerprint,
                    queue_reason,
                    run_id,
                    run_id,
                ],
            )
            queue_enqueued_count += 1
            continue
        connection.execute(
            f"""
            UPDATE {POSITION_WORK_QUEUE_TABLE}
            SET
                checkpoint_nk = ?,
                signal_nk = ?,
                instrument = ?,
                reference_trade_date = ?,
                source_signal_fingerprint = ?,
                queue_reason = ?,
                queue_status = 'pending',
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [
                candidate.checkpoint_nk,
                candidate.signal.signal_nk,
                candidate.signal.instrument,
                coerce_date(candidate.signal.reference_trade_date),
                candidate.source_signal_fingerprint,
                queue_reason,
                queue_nk,
            ],
        )
    return queue_enqueued_count


def claim_position_queue_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
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
            checkpoint_nk,
            signal_nk,
            instrument,
            reference_trade_date,
            source_signal_fingerprint,
            queue_reason
        FROM {POSITION_WORK_QUEUE_TABLE}
        WHERE candidate_nk IN ({placeholders})
          AND queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY reference_trade_date, instrument, candidate_nk, queued_at
        LIMIT ?
        """,
        [*sorted(candidate_nks), limit],
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {POSITION_WORK_QUEUE_TABLE}
            SET
                queue_status = 'claimed',
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
                "candidate_nk": str(row[1]),
                "checkpoint_nk": str(row[2]),
                "signal_nk": str(row[3]),
                "instrument": str(row[4]),
                "reference_trade_date": coerce_date(row[5]),
                "source_signal_fingerprint": str(row[6]),
                "queue_reason": str(row[7]),
            }
        )
    return claimed_rows


def mark_position_queue_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {POSITION_WORK_QUEUE_TABLE}
        SET
            queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def mark_position_queue_failed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {POSITION_WORK_QUEUE_TABLE}
        SET
            queue_status = 'failed',
            last_claimed_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def upsert_position_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    checkpoint_nk: str,
    candidate_nk: str,
    instrument: str,
    checkpoint_scope: str,
    last_signal_nk: str,
    last_reference_trade_date: date | None,
    last_source_signal_fingerprint: str,
    last_run_id: str,
) -> None:
    existing = connection.execute(
        f"SELECT checkpoint_nk FROM {POSITION_CHECKPOINT_TABLE} WHERE checkpoint_nk = ?",
        [checkpoint_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {POSITION_CHECKPOINT_TABLE} (
                checkpoint_nk,
                candidate_nk,
                instrument,
                checkpoint_scope,
                last_signal_nk,
                last_reference_trade_date,
                last_source_signal_fingerprint,
                last_completed_at,
                last_run_id,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
            """,
            [
                checkpoint_nk,
                candidate_nk,
                instrument,
                checkpoint_scope,
                last_signal_nk,
                last_reference_trade_date,
                last_source_signal_fingerprint,
                last_run_id,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {POSITION_CHECKPOINT_TABLE}
        SET
            candidate_nk = ?,
            instrument = ?,
            checkpoint_scope = ?,
            last_signal_nk = ?,
            last_reference_trade_date = ?,
            last_source_signal_fingerprint = ?,
            last_completed_at = CURRENT_TIMESTAMP,
            last_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE checkpoint_nk = ?
        """,
        [
            candidate_nk,
            instrument,
            checkpoint_scope,
            last_signal_nk,
            last_reference_trade_date,
            last_source_signal_fingerprint,
            last_run_id,
            checkpoint_nk,
        ],
    )


def delete_position_candidate_scope(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
) -> None:
    connection.execute(
        """
        DELETE FROM position_exit_leg
        WHERE exit_plan_nk IN (
            SELECT exit_plan_nk
            FROM position_exit_plan
            WHERE candidate_nk = ?
        )
        """,
        [candidate_nk],
    )
    connection.execute("DELETE FROM position_exit_plan WHERE candidate_nk = ?", [candidate_nk])
    connection.execute("DELETE FROM position_funding_fixed_notional_snapshot WHERE candidate_nk = ?", [candidate_nk])
    connection.execute("DELETE FROM position_funding_single_lot_snapshot WHERE candidate_nk = ?", [candidate_nk])
    connection.execute("DELETE FROM position_entry_leg_plan WHERE candidate_nk = ?", [candidate_nk])
    connection.execute("DELETE FROM position_sizing_snapshot WHERE candidate_nk = ?", [candidate_nk])
    connection.execute("DELETE FROM position_capacity_snapshot WHERE candidate_nk = ?", [candidate_nk])
    connection.execute("DELETE FROM position_risk_budget_snapshot WHERE candidate_nk = ?", [candidate_nk])
    connection.execute("DELETE FROM position_candidate_audit WHERE candidate_nk = ?", [candidate_nk])


def insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    policy_id: str,
    execution_mode: str,
    bounded_signal_count: int,
    source_signal_contract_version: str | None,
    source_signal_run_id: str | None,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {POSITION_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            execution_mode,
            policy_id,
            bounded_signal_count,
            source_signal_contract_version,
            source_signal_run_id,
            notes
        )
        SELECT ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM {POSITION_RUN_TABLE}
            WHERE run_id = ?
        )
        """,
        [
            run_id,
            DEFAULT_POSITION_RUNNER_NAME,
            DEFAULT_POSITION_RUNNER_VERSION,
            execution_mode,
            policy_id,
            bounded_signal_count,
            source_signal_contract_version,
            source_signal_run_id,
            "position data-grade materialization from alpha formal signal",
            run_id,
        ],
    )


def insert_run_snapshot_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    candidate_nk: str,
    signal_nk: str,
    reference_trade_date: date | None,
    materialization_action: str,
    queue_row: dict[str, object] | None,
    candidate_status: str,
    position_action_decision: str | None,
) -> None:
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {POSITION_RUN_SNAPSHOT_TABLE} (
            run_id,
            candidate_nk,
            signal_nk,
            reference_trade_date,
            materialization_action,
            queue_nk,
            queue_reason,
            candidate_status,
            position_action_decision
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            candidate_nk,
            signal_nk,
            reference_trade_date,
            materialization_action,
            None if queue_row is None else str(queue_row["queue_nk"]),
            None if queue_row is None else str(queue_row["queue_reason"]),
            candidate_status,
            position_action_decision,
        ],
    )


def mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: PositionFormalSignalRunnerSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {POSITION_RUN_TABLE}
        SET
            run_status = 'completed',
            run_completed_at = CURRENT_TIMESTAMP,
            inserted_count = ?,
            reused_count = ?,
            rematerialized_count = ?,
            queue_enqueued_count = ?,
            queue_claimed_count = ?,
            checkpoint_upserted_count = ?,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            summary.inserted_count,
            summary.reused_count,
            summary.rematerialized_count,
            summary.queue_enqueued_count,
            summary.queue_claimed_count,
            summary.checkpoint_upserted_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def mark_run_failed(connection: duckdb.DuckDBPyConnection, *, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {POSITION_RUN_TABLE}
        SET
            run_status = 'failed',
            run_completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            json.dumps({"run_status": "failed"}, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )
