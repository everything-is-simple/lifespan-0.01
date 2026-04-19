"""冻结 `system` 模块最小正式 readout / audit 账本表族。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


SYSTEM_RUN_TABLE: Final[str] = "system_run"
SYSTEM_CHILD_RUN_READOUT_TABLE: Final[str] = "system_child_run_readout"
SYSTEM_MAINLINE_SNAPSHOT_TABLE: Final[str] = "system_mainline_snapshot"
SYSTEM_RUN_SNAPSHOT_TABLE: Final[str] = "system_run_snapshot"


SYSTEM_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    SYSTEM_RUN_TABLE,
    SYSTEM_CHILD_RUN_READOUT_TABLE,
    SYSTEM_MAINLINE_SNAPSHOT_TABLE,
    SYSTEM_RUN_SNAPSHOT_TABLE,
)


SYSTEM_LEDGER_DDL: Final[dict[str, str]] = {
    SYSTEM_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS system_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            system_materialization_action TEXT,
            portfolio_id TEXT NOT NULL,
            snapshot_date DATE NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            system_scene TEXT NOT NULL,
            system_contract_version TEXT NOT NULL,
            bounded_child_run_count BIGINT NOT NULL DEFAULT 0,
            planned_entry_count BIGINT NOT NULL DEFAULT 0,
            blocked_upstream_count BIGINT NOT NULL DEFAULT 0,
            planned_carry_count BIGINT NOT NULL DEFAULT 0,
            carried_open_leg_count BIGINT NOT NULL DEFAULT 0,
            child_readout_inserted_count BIGINT NOT NULL DEFAULT 0,
            child_readout_reused_count BIGINT NOT NULL DEFAULT 0,
            child_readout_rematerialized_count BIGINT NOT NULL DEFAULT 0,
            snapshot_inserted_count BIGINT NOT NULL DEFAULT 0,
            snapshot_reused_count BIGINT NOT NULL DEFAULT 0,
            snapshot_rematerialized_count BIGINT NOT NULL DEFAULT 0,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    SYSTEM_CHILD_RUN_READOUT_TABLE: """
        CREATE TABLE IF NOT EXISTS system_child_run_readout (
            child_run_readout_nk TEXT PRIMARY KEY,
            child_module TEXT NOT NULL,
            child_run_id TEXT NOT NULL,
            child_run_status TEXT NOT NULL,
            child_runner_name TEXT NOT NULL,
            child_runner_version TEXT NOT NULL,
            child_contract_version TEXT,
            child_signal_start_date DATE,
            child_signal_end_date DATE,
            child_started_at TIMESTAMP,
            child_completed_at TIMESTAMP,
            child_summary_json TEXT NOT NULL,
            child_ledger_path TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    SYSTEM_MAINLINE_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS system_mainline_snapshot (
            mainline_snapshot_nk TEXT PRIMARY KEY,
            portfolio_id TEXT NOT NULL,
            snapshot_date DATE NOT NULL,
            system_scene TEXT NOT NULL,
            acceptance_status TEXT NOT NULL,
            acceptance_note TEXT,
            planned_entry_count BIGINT NOT NULL DEFAULT 0,
            blocked_upstream_count BIGINT NOT NULL DEFAULT 0,
            planned_carry_count BIGINT NOT NULL DEFAULT 0,
            carried_open_leg_count BIGINT NOT NULL DEFAULT 0,
            current_carry_weight DOUBLE NOT NULL DEFAULT 0,
            included_child_run_count BIGINT NOT NULL DEFAULT 0,
            source_portfolio_plan_run_id TEXT NOT NULL,
            source_trade_run_id TEXT NOT NULL,
            system_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    SYSTEM_RUN_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS system_run_snapshot (
            run_id TEXT NOT NULL,
            mainline_snapshot_nk TEXT NOT NULL,
            acceptance_status TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, mainline_snapshot_nk)
        )
    """,
}


def connect_system_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `system` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(workspace.databases.system), read_only=read_only)


def bootstrap_system_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建 `system` 最小四表。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_system_ledger(workspace)
    try:
        for table_name in SYSTEM_LEDGER_TABLE_NAMES:
            conn.execute(SYSTEM_LEDGER_DDL[table_name])
        return SYSTEM_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def system_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `system` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.system
