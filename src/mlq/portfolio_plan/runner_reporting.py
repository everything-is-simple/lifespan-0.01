"""`portfolio_plan` runner 的 run-state / freshness / summary 辅助。"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from .bootstrap import (
    PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE,
    PORTFOLIO_PLAN_RUN_TABLE,
    PORTFOLIO_PLAN_SNAPSHOT_TABLE,
)
from .runner_source import normalize_date_value

if TYPE_CHECKING:
    from .runner import PortfolioPlanBuildSummary


def insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    execution_mode: str,
    portfolio_id: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_candidate_count: int,
    source_position_table: str,
    portfolio_plan_contract_version: str,
) -> None:
    """登记 `portfolio_plan_run` 的 running 批次。"""

    connection.execute(
        f"""
        INSERT INTO {PORTFOLIO_PLAN_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            execution_mode,
            portfolio_id,
            signal_start_date,
            signal_end_date,
            bounded_candidate_count,
            source_position_table,
            portfolio_plan_contract_version
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            execution_mode,
            portfolio_id,
            signal_start_date,
            signal_end_date,
            bounded_candidate_count,
            source_position_table,
            portfolio_plan_contract_version,
        ],
    )


def upsert_portfolio_plan_freshness_audit(
    connection: duckdb.DuckDBPyConnection,
    *,
    portfolio_id: str,
    expected_reference_trade_date: date | None,
    last_success_run_id: str,
) -> tuple[date | None, date | None, str]:
    """回写 `portfolio_plan_freshness_audit`。"""

    latest_reference_trade_date = load_latest_reference_trade_date(
        connection,
        portfolio_id=portfolio_id,
    )
    freshness_status = derive_freshness_status(
        latest_reference_trade_date=latest_reference_trade_date,
        expected_reference_trade_date=expected_reference_trade_date,
    )
    audit_date = datetime.now().date()
    existing = connection.execute(
        f"SELECT portfolio_id FROM {PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE} WHERE portfolio_id = ?",
        [portfolio_id],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE} (
                portfolio_id,
                audit_date,
                latest_reference_trade_date,
                expected_reference_trade_date,
                freshness_status,
                last_success_run_id,
                last_run_id,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                portfolio_id,
                audit_date,
                latest_reference_trade_date,
                expected_reference_trade_date,
                freshness_status,
                last_success_run_id,
                last_success_run_id,
            ],
        )
    else:
        connection.execute(
            f"""
            UPDATE {PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE}
            SET
                audit_date = ?,
                latest_reference_trade_date = ?,
                expected_reference_trade_date = ?,
                freshness_status = ?,
                last_success_run_id = ?,
                last_run_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE portfolio_id = ?
            """,
            [
                audit_date,
                latest_reference_trade_date,
                expected_reference_trade_date,
                freshness_status,
                last_success_run_id,
                last_success_run_id,
                portfolio_id,
            ],
        )
    return latest_reference_trade_date, expected_reference_trade_date, freshness_status


def load_latest_reference_trade_date(
    connection: duckdb.DuckDBPyConnection,
    *,
    portfolio_id: str,
) -> date | None:
    """读取当前组合在 snapshot 中的最新 reference_trade_date。"""

    row = connection.execute(
        f"""
        SELECT MAX(reference_trade_date)
        FROM {PORTFOLIO_PLAN_SNAPSHOT_TABLE}
        WHERE portfolio_id = ?
        """,
        [portfolio_id],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return normalize_date_value(row[0], field_name="snapshot.reference_trade_date")


def derive_freshness_status(
    *,
    latest_reference_trade_date: date | None,
    expected_reference_trade_date: date | None,
) -> str:
    """根据期望日期与已落表日期给出 freshness。"""

    if latest_reference_trade_date is None and expected_reference_trade_date is None:
        return "no_source_data"
    if expected_reference_trade_date is None:
        return "fresh"
    if latest_reference_trade_date is None:
        return "stale"
    if latest_reference_trade_date < expected_reference_trade_date:
        return "stale"
    return "fresh"


def mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: "PortfolioPlanBuildSummary",
) -> None:
    """把当前运行标记为 completed 并回写 summary。"""

    update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        summary=summary,
    )


def mark_run_failed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
) -> None:
    """把当前运行标记为 failed。"""

    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_RUN_TABLE}
        SET
            run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            json.dumps({"run_status": "failed"}, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    summary: "PortfolioPlanBuildSummary",
) -> None:
    """更新 run 表中的计数与 summary_json。"""

    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_RUN_TABLE}
        SET
            run_status = ?,
            admitted_count = ?,
            blocked_count = ?,
            trimmed_count = ?,
            deferred_count = ?,
            inserted_count = ?,
            reused_count = ?,
            rematerialized_count = ?,
            queue_enqueued_count = ?,
            queue_claimed_count = ?,
            checkpoint_upserted_count = ?,
            freshness_updated_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            summary.admitted_count,
            summary.blocked_count,
            summary.trimmed_count,
            summary.deferred_count,
            summary.inserted_count,
            summary.reused_count,
            summary.rematerialized_count,
            summary.queue_enqueued_count,
            summary.queue_claimed_count,
            summary.checkpoint_upserted_count,
            summary.freshness_updated_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def write_summary(summary: "PortfolioPlanBuildSummary", summary_path: Path | None) -> None:
    """在需要时把 summary 落为 JSON 文件。"""

    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
