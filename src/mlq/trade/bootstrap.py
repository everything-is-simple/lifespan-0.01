"""冻结 `trade_runtime` 最小正式账本表族。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


TRADE_RUN_TABLE: Final[str] = "trade_run"
TRADE_EXECUTION_PLAN_TABLE: Final[str] = "trade_execution_plan"
TRADE_POSITION_LEG_TABLE: Final[str] = "trade_position_leg"
TRADE_CARRY_SNAPSHOT_TABLE: Final[str] = "trade_carry_snapshot"
TRADE_RUN_EXECUTION_PLAN_TABLE: Final[str] = "trade_run_execution_plan"


TRADE_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    TRADE_RUN_TABLE,
    TRADE_EXECUTION_PLAN_TABLE,
    TRADE_POSITION_LEG_TABLE,
    TRADE_CARRY_SNAPSHOT_TABLE,
    TRADE_RUN_EXECUTION_PLAN_TABLE,
)


TRADE_LEDGER_DDL: Final[dict[str, str]] = {
    TRADE_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS trade_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_plan_count BIGINT NOT NULL DEFAULT 0,
            planned_entry_count BIGINT NOT NULL DEFAULT 0,
            blocked_upstream_count BIGINT NOT NULL DEFAULT 0,
            carried_open_leg_count BIGINT NOT NULL DEFAULT 0,
            source_portfolio_plan_table TEXT NOT NULL,
            trade_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    TRADE_EXECUTION_PLAN_TABLE: """
        CREATE TABLE IF NOT EXISTS trade_execution_plan (
            execution_plan_nk TEXT PRIMARY KEY,
            plan_snapshot_nk TEXT NOT NULL,
            candidate_nk TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            signal_date DATE NOT NULL,
            planned_entry_trade_date DATE NOT NULL,
            execution_action TEXT NOT NULL,
            execution_status TEXT NOT NULL,
            requested_weight DOUBLE NOT NULL DEFAULT 0,
            planned_entry_weight DOUBLE NOT NULL DEFAULT 0,
            trimmed_weight DOUBLE NOT NULL DEFAULT 0,
            carry_source_status TEXT NOT NULL,
            entry_timing_policy TEXT NOT NULL,
            risk_unit_policy TEXT NOT NULL,
            take_profit_policy TEXT NOT NULL,
            fast_failure_policy TEXT NOT NULL,
            trailing_stop_policy TEXT NOT NULL,
            time_stop_policy TEXT NOT NULL,
            trade_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    TRADE_POSITION_LEG_TABLE: """
        CREATE TABLE IF NOT EXISTS trade_position_leg (
            position_leg_nk TEXT PRIMARY KEY,
            execution_plan_nk TEXT NOT NULL,
            instrument TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            leg_role TEXT NOT NULL,
            entry_trade_date DATE NOT NULL,
            entry_weight DOUBLE NOT NULL DEFAULT 0,
            remaining_weight DOUBLE NOT NULL DEFAULT 0,
            leg_status TEXT NOT NULL,
            carry_eligible BOOLEAN NOT NULL DEFAULT TRUE,
            trade_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    TRADE_CARRY_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS trade_carry_snapshot (
            carry_snapshot_nk TEXT PRIMARY KEY,
            snapshot_date DATE NOT NULL,
            instrument TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            current_position_weight DOUBLE NOT NULL DEFAULT 0,
            open_leg_count BIGINT NOT NULL DEFAULT 0,
            carry_source_leg_nk TEXT,
            carry_source_run_id TEXT,
            carry_source_status TEXT NOT NULL,
            trade_contract_version TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    TRADE_RUN_EXECUTION_PLAN_TABLE: """
        CREATE TABLE IF NOT EXISTS trade_run_execution_plan (
            run_id TEXT NOT NULL,
            execution_plan_nk TEXT NOT NULL,
            execution_status TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, execution_plan_nk)
        )
    """,
}


def connect_trade_runtime_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `trade_runtime` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(workspace.databases.trade_runtime), read_only=read_only)


def bootstrap_trade_runtime_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建 `trade_runtime` 最小五表。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_trade_runtime_ledger(workspace)
    try:
        for table_name in TRADE_LEDGER_TABLE_NAMES:
            conn.execute(TRADE_LEDGER_DDL[table_name])
        return TRADE_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def trade_runtime_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `trade_runtime` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.trade_runtime
