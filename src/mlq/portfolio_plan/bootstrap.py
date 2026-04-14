"""冻结并演进 `portfolio_plan` 官方账本族与 bootstrap 入口。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


PORTFOLIO_PLAN_RUN_TABLE: Final[str] = "portfolio_plan_run"
PORTFOLIO_PLAN_WORK_QUEUE_TABLE: Final[str] = "portfolio_plan_work_queue"
PORTFOLIO_PLAN_CHECKPOINT_TABLE: Final[str] = "portfolio_plan_checkpoint"
PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE: Final[str] = (
    "portfolio_plan_candidate_decision"
)
PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE: Final[str] = (
    "portfolio_plan_capacity_snapshot"
)
PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE: Final[str] = (
    "portfolio_plan_allocation_snapshot"
)
PORTFOLIO_PLAN_SNAPSHOT_TABLE: Final[str] = "portfolio_plan_snapshot"
PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE: Final[str] = "portfolio_plan_run_snapshot"
PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE: Final[str] = "portfolio_plan_freshness_audit"


PORTFOLIO_PLAN_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    PORTFOLIO_PLAN_RUN_TABLE,
    PORTFOLIO_PLAN_WORK_QUEUE_TABLE,
    PORTFOLIO_PLAN_CHECKPOINT_TABLE,
    PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE,
    PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE,
)


PORTFOLIO_PLAN_LEDGER_DDL: Final[dict[str, str]] = {
    PORTFOLIO_PLAN_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS portfolio_plan_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_candidate_count BIGINT NOT NULL DEFAULT 0,
            admitted_count BIGINT NOT NULL DEFAULT 0,
            blocked_count BIGINT NOT NULL DEFAULT 0,
            trimmed_count BIGINT NOT NULL DEFAULT 0,
            deferred_count BIGINT NOT NULL DEFAULT 0,
            source_position_table TEXT NOT NULL,
            portfolio_plan_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    PORTFOLIO_PLAN_WORK_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS portfolio_plan_work_queue (
            queue_nk TEXT PRIMARY KEY,
            portfolio_id TEXT NOT NULL,
            candidate_nk TEXT NOT NULL,
            reference_trade_date DATE NOT NULL,
            queue_reason TEXT NOT NULL,
            queue_status TEXT NOT NULL DEFAULT 'pending',
            source_candidate_nk TEXT,
            first_enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_success_run_id TEXT,
            last_error_text TEXT
        )
    """,
    PORTFOLIO_PLAN_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS portfolio_plan_checkpoint (
            checkpoint_nk TEXT PRIMARY KEY,
            portfolio_id TEXT NOT NULL,
            checkpoint_scope TEXT NOT NULL,
            latest_reference_trade_date DATE,
            last_candidate_nk TEXT,
            last_success_run_id TEXT,
            checkpoint_payload_json TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE: """
        CREATE TABLE IF NOT EXISTS portfolio_plan_candidate_decision (
            candidate_decision_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            reference_trade_date DATE NOT NULL,
            position_action_decision TEXT NOT NULL,
            decision_status TEXT NOT NULL,
            decision_reason_code TEXT NOT NULL,
            blocking_reason_code TEXT,
            decision_rank BIGINT NOT NULL DEFAULT 0,
            decision_order_code TEXT NOT NULL,
            source_candidate_status TEXT NOT NULL,
            source_blocked_reason_code TEXT,
            source_binding_cap_code TEXT NOT NULL DEFAULT 'no_binding_cap',
            source_capacity_source_code TEXT,
            source_required_reduction_weight DOUBLE NOT NULL DEFAULT 0,
            source_remaining_single_name_capacity_weight DOUBLE NOT NULL DEFAULT 0,
            source_remaining_portfolio_capacity_weight DOUBLE NOT NULL DEFAULT 0,
            capacity_before_weight DOUBLE NOT NULL DEFAULT 0,
            capacity_after_weight DOUBLE NOT NULL DEFAULT 0,
            trade_readiness_status TEXT NOT NULL DEFAULT 'not_trade_ready',
            schedule_stage TEXT,
            schedule_lag_days INTEGER NOT NULL DEFAULT 1,
            requested_weight DOUBLE NOT NULL DEFAULT 0,
            admitted_weight DOUBLE NOT NULL DEFAULT 0,
            trimmed_weight DOUBLE NOT NULL DEFAULT 0,
            capacity_snapshot_nk TEXT NOT NULL,
            portfolio_plan_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS portfolio_plan_capacity_snapshot (
            capacity_snapshot_nk TEXT PRIMARY KEY,
            portfolio_id TEXT NOT NULL,
            capacity_scope TEXT NOT NULL,
            reference_trade_date DATE NOT NULL,
            portfolio_gross_cap_weight DOUBLE NOT NULL DEFAULT 0,
            portfolio_gross_used_weight DOUBLE NOT NULL DEFAULT 0,
            portfolio_gross_remaining_weight DOUBLE NOT NULL DEFAULT 0,
            requested_candidate_count BIGINT NOT NULL DEFAULT 0,
            admitted_candidate_count BIGINT NOT NULL DEFAULT 0,
            blocked_candidate_count BIGINT NOT NULL DEFAULT 0,
            trimmed_candidate_count BIGINT NOT NULL DEFAULT 0,
            deferred_candidate_count BIGINT NOT NULL DEFAULT 0,
            requested_total_weight DOUBLE NOT NULL DEFAULT 0,
            admitted_total_weight DOUBLE NOT NULL DEFAULT 0,
            trimmed_total_weight DOUBLE NOT NULL DEFAULT 0,
            blocked_total_weight DOUBLE NOT NULL DEFAULT 0,
            deferred_total_weight DOUBLE NOT NULL DEFAULT 0,
            binding_constraint_code TEXT NOT NULL DEFAULT 'no_binding_cap',
            capacity_decision_reason_code TEXT NOT NULL DEFAULT 'no_candidates',
            capacity_reason_summary_json TEXT,
            portfolio_plan_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS portfolio_plan_allocation_snapshot (
            allocation_snapshot_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            allocation_scene TEXT NOT NULL,
            reference_trade_date DATE NOT NULL,
            requested_weight DOUBLE NOT NULL DEFAULT 0,
            admitted_weight DOUBLE NOT NULL DEFAULT 0,
            trimmed_weight DOUBLE NOT NULL DEFAULT 0,
            final_allocated_weight DOUBLE NOT NULL DEFAULT 0,
            plan_status TEXT NOT NULL,
            decision_reason_code TEXT NOT NULL,
            blocking_reason_code TEXT,
            decision_rank BIGINT NOT NULL DEFAULT 0,
            decision_order_code TEXT NOT NULL,
            trade_readiness_status TEXT NOT NULL DEFAULT 'not_trade_ready',
            schedule_stage TEXT,
            schedule_lag_days INTEGER NOT NULL DEFAULT 1,
            source_binding_cap_code TEXT NOT NULL DEFAULT 'no_binding_cap',
            candidate_decision_nk TEXT NOT NULL,
            capacity_snapshot_nk TEXT NOT NULL,
            portfolio_plan_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    PORTFOLIO_PLAN_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS portfolio_plan_snapshot (
            plan_snapshot_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            reference_trade_date DATE NOT NULL,
            position_action_decision TEXT NOT NULL,
            requested_weight DOUBLE NOT NULL DEFAULT 0,
            admitted_weight DOUBLE NOT NULL DEFAULT 0,
            trimmed_weight DOUBLE NOT NULL DEFAULT 0,
            plan_status TEXT NOT NULL,
            decision_reason_code TEXT NOT NULL DEFAULT 'unknown',
            blocking_reason_code TEXT,
            decision_rank BIGINT NOT NULL DEFAULT 0,
            decision_order_code TEXT NOT NULL DEFAULT 'requested_weight_desc_then_instrument',
            trade_readiness_status TEXT NOT NULL DEFAULT 'not_trade_ready',
            schedule_stage TEXT,
            schedule_lag_days INTEGER NOT NULL DEFAULT 1,
            source_binding_cap_code TEXT NOT NULL DEFAULT 'no_binding_cap',
            portfolio_gross_cap_weight DOUBLE NOT NULL DEFAULT 0,
            portfolio_gross_used_weight DOUBLE NOT NULL DEFAULT 0,
            portfolio_gross_remaining_weight DOUBLE NOT NULL DEFAULT 0,
            candidate_decision_nk TEXT NOT NULL,
            capacity_snapshot_nk TEXT NOT NULL,
            allocation_snapshot_nk TEXT NOT NULL,
            portfolio_plan_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS portfolio_plan_run_snapshot (
            run_id TEXT NOT NULL,
            plan_snapshot_nk TEXT NOT NULL,
            candidate_decision_nk TEXT NOT NULL,
            capacity_snapshot_nk TEXT NOT NULL,
            allocation_snapshot_nk TEXT NOT NULL,
            candidate_nk TEXT NOT NULL,
            plan_status TEXT NOT NULL,
            decision_reason_code TEXT NOT NULL DEFAULT 'unknown',
            trade_readiness_status TEXT NOT NULL DEFAULT 'not_trade_ready',
            materialization_action TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, plan_snapshot_nk)
        )
    """,
    PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE: """
        CREATE TABLE IF NOT EXISTS portfolio_plan_freshness_audit (
            portfolio_id TEXT PRIMARY KEY,
            latest_reference_trade_date DATE,
            expected_reference_trade_date DATE,
            freshness_status TEXT NOT NULL DEFAULT 'unknown',
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
}


PORTFOLIO_PLAN_SCHEMA_EVOLUTION: Final[
    dict[str, tuple[tuple[str, str], ...]]
] = {
    PORTFOLIO_PLAN_RUN_TABLE: (
        ("deferred_count", "BIGINT NOT NULL DEFAULT 0"),
    ),
    PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE: (
        ("decision_rank", "BIGINT NOT NULL DEFAULT 0"),
        (
            "decision_order_code",
            "TEXT NOT NULL DEFAULT 'requested_weight_desc_then_instrument'",
        ),
        ("source_candidate_status", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("source_blocked_reason_code", "TEXT"),
        ("source_binding_cap_code", "TEXT NOT NULL DEFAULT 'no_binding_cap'"),
        ("source_capacity_source_code", "TEXT"),
        ("source_required_reduction_weight", "DOUBLE NOT NULL DEFAULT 0"),
        (
            "source_remaining_single_name_capacity_weight",
            "DOUBLE NOT NULL DEFAULT 0",
        ),
        (
            "source_remaining_portfolio_capacity_weight",
            "DOUBLE NOT NULL DEFAULT 0",
        ),
        ("capacity_before_weight", "DOUBLE NOT NULL DEFAULT 0"),
        ("capacity_after_weight", "DOUBLE NOT NULL DEFAULT 0"),
        ("trade_readiness_status", "TEXT NOT NULL DEFAULT 'not_trade_ready'"),
        ("schedule_stage", "TEXT"),
        ("schedule_lag_days", "INTEGER NOT NULL DEFAULT 1"),
    ),
    PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE: (
        ("requested_candidate_count", "BIGINT NOT NULL DEFAULT 0"),
        ("requested_total_weight", "DOUBLE NOT NULL DEFAULT 0"),
        ("admitted_total_weight", "DOUBLE NOT NULL DEFAULT 0"),
        ("trimmed_total_weight", "DOUBLE NOT NULL DEFAULT 0"),
        ("blocked_total_weight", "DOUBLE NOT NULL DEFAULT 0"),
        ("deferred_total_weight", "DOUBLE NOT NULL DEFAULT 0"),
        ("binding_constraint_code", "TEXT NOT NULL DEFAULT 'no_binding_cap'"),
        (
            "capacity_decision_reason_code",
            "TEXT NOT NULL DEFAULT 'no_candidates'",
        ),
        ("capacity_reason_summary_json", "TEXT"),
    ),
    PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE: (
        ("decision_reason_code", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("decision_rank", "BIGINT NOT NULL DEFAULT 0"),
        (
            "decision_order_code",
            "TEXT NOT NULL DEFAULT 'requested_weight_desc_then_instrument'",
        ),
        ("trade_readiness_status", "TEXT NOT NULL DEFAULT 'not_trade_ready'"),
        ("schedule_stage", "TEXT"),
        ("schedule_lag_days", "INTEGER NOT NULL DEFAULT 1"),
        ("source_binding_cap_code", "TEXT NOT NULL DEFAULT 'no_binding_cap'"),
    ),
    PORTFOLIO_PLAN_SNAPSHOT_TABLE: (
        ("candidate_decision_nk", "TEXT"),
        ("capacity_snapshot_nk", "TEXT"),
        ("allocation_snapshot_nk", "TEXT"),
        ("decision_reason_code", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("decision_rank", "BIGINT NOT NULL DEFAULT 0"),
        (
            "decision_order_code",
            "TEXT NOT NULL DEFAULT 'requested_weight_desc_then_instrument'",
        ),
        ("trade_readiness_status", "TEXT NOT NULL DEFAULT 'not_trade_ready'"),
        ("schedule_stage", "TEXT"),
        ("schedule_lag_days", "INTEGER NOT NULL DEFAULT 1"),
        ("source_binding_cap_code", "TEXT NOT NULL DEFAULT 'no_binding_cap'"),
    ),
    PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE: (
        ("candidate_decision_nk", "TEXT"),
        ("capacity_snapshot_nk", "TEXT"),
        ("allocation_snapshot_nk", "TEXT"),
        ("decision_reason_code", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("trade_readiness_status", "TEXT NOT NULL DEFAULT 'not_trade_ready'"),
    ),
}


def connect_portfolio_plan_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `portfolio_plan` 账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(workspace.databases.portfolio_plan), read_only=read_only)


def bootstrap_portfolio_plan_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建 `portfolio_plan` v2 官方账本表族并补齐兼容列。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_portfolio_plan_ledger(workspace)
    try:
        for ddl in PORTFOLIO_PLAN_LEDGER_DDL.values():
            conn.execute(ddl)
        apply_portfolio_plan_schema_evolution(conn)
        return PORTFOLIO_PLAN_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def apply_portfolio_plan_schema_evolution(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """对既有 `portfolio_plan` 表补齐 52 卡冻结所需的兼容列。"""

    existing_tables = {
        str(row[0])
        for row in connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
    }
    for table_name, columns in PORTFOLIO_PLAN_SCHEMA_EVOLUTION.items():
        if table_name not in existing_tables:
            continue
        existing_columns = {
            str(row[1])
            for row in connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        }
        for column_name, column_type in columns:
            if column_name in existing_columns:
                continue
            connection.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            )


def portfolio_plan_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `portfolio_plan` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.portfolio_plan
