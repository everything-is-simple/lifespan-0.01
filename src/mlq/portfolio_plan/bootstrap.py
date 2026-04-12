"""冻结最小正式 `portfolio_plan` 账本表族与辅助入口。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


PORTFOLIO_PLAN_RUN_TABLE: Final[str] = "portfolio_plan_run"
PORTFOLIO_PLAN_SNAPSHOT_TABLE: Final[str] = "portfolio_plan_snapshot"
PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE: Final[str] = "portfolio_plan_run_snapshot"


PORTFOLIO_PLAN_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    PORTFOLIO_PLAN_RUN_TABLE,
    PORTFOLIO_PLAN_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE,
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
            source_position_table TEXT NOT NULL,
            portfolio_plan_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
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
            blocking_reason_code TEXT,
            portfolio_gross_cap_weight DOUBLE NOT NULL DEFAULT 0,
            portfolio_gross_used_weight DOUBLE NOT NULL DEFAULT 0,
            portfolio_gross_remaining_weight DOUBLE NOT NULL DEFAULT 0,
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
            candidate_nk TEXT NOT NULL,
            plan_status TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, plan_snapshot_nk)
        )
    """,
}


def connect_portfolio_plan_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Connect to the official `portfolio_plan` ledger."""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(workspace.databases.portfolio_plan), read_only=read_only)


def bootstrap_portfolio_plan_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """Create the minimal three-table `portfolio_plan` ledger family."""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_portfolio_plan_ledger(workspace)
    try:
        for table_name in PORTFOLIO_PLAN_LEDGER_TABLE_NAMES:
            conn.execute(PORTFOLIO_PLAN_LEDGER_DDL[table_name])
        return PORTFOLIO_PLAN_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def portfolio_plan_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """Return the official `portfolio_plan` ledger path."""

    workspace = settings or default_settings()
    return workspace.databases.portfolio_plan
