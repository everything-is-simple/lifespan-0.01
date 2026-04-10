"""冻结 `malf` 模块最小正式快照表族。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


MALF_RUN_TABLE: Final[str] = "malf_run"
PAS_CONTEXT_SNAPSHOT_TABLE: Final[str] = "pas_context_snapshot"
STRUCTURE_CANDIDATE_SNAPSHOT_TABLE: Final[str] = "structure_candidate_snapshot"
MALF_RUN_CONTEXT_SNAPSHOT_TABLE: Final[str] = "malf_run_context_snapshot"
MALF_RUN_STRUCTURE_SNAPSHOT_TABLE: Final[str] = "malf_run_structure_snapshot"


MALF_LEDGER_TABLES: Final[dict[str, str]] = {
    MALF_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_run (
            run_id TEXT,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            source_price_table TEXT NOT NULL,
            adjust_method TEXT NOT NULL,
            malf_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    PAS_CONTEXT_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS pas_context_snapshot (
            context_nk TEXT,
            entity_code TEXT NOT NULL,
            entity_name TEXT,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            source_context_nk TEXT,
            malf_context_4 TEXT NOT NULL,
            lifecycle_rank_high BIGINT NOT NULL,
            lifecycle_rank_total BIGINT NOT NULL,
            calc_date DATE,
            adjust_method TEXT,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    STRUCTURE_CANDIDATE_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS structure_candidate_snapshot (
            candidate_nk TEXT,
            instrument TEXT NOT NULL,
            instrument_name TEXT,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            new_high_count BIGINT NOT NULL,
            new_low_count BIGINT NOT NULL,
            refresh_density DOUBLE NOT NULL,
            advancement_density DOUBLE NOT NULL,
            is_failed_extreme BOOLEAN NOT NULL,
            failure_type TEXT,
            adjust_method TEXT,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_RUN_CONTEXT_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_run_context_snapshot (
            run_id TEXT NOT NULL,
            context_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_RUN_STRUCTURE_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_run_structure_snapshot (
            run_id TEXT NOT NULL,
            candidate_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
}


MALF_REQUIRED_COLUMNS: Final[dict[str, dict[str, str]]] = {
    MALF_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "run_status": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_instrument_count": "BIGINT",
        "source_price_table": "TEXT",
        "adjust_method": "TEXT",
        "malf_contract_version": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    PAS_CONTEXT_SNAPSHOT_TABLE: {
        "context_nk": "TEXT",
        "entity_code": "TEXT",
        "entity_name": "TEXT",
        "signal_date": "DATE",
        "asof_date": "DATE",
        "source_context_nk": "TEXT",
        "malf_context_4": "TEXT",
        "lifecycle_rank_high": "BIGINT",
        "lifecycle_rank_total": "BIGINT",
        "calc_date": "DATE",
        "adjust_method": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    STRUCTURE_CANDIDATE_SNAPSHOT_TABLE: {
        "candidate_nk": "TEXT",
        "instrument": "TEXT",
        "instrument_name": "TEXT",
        "signal_date": "DATE",
        "asof_date": "DATE",
        "new_high_count": "BIGINT",
        "new_low_count": "BIGINT",
        "refresh_density": "DOUBLE",
        "advancement_density": "DOUBLE",
        "is_failed_extreme": "BOOLEAN",
        "failure_type": "TEXT",
        "adjust_method": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    MALF_RUN_CONTEXT_SNAPSHOT_TABLE: {
        "run_id": "TEXT",
        "context_nk": "TEXT",
        "materialization_action": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
    MALF_RUN_STRUCTURE_SNAPSHOT_TABLE: {
        "run_id": "TEXT",
        "candidate_nk": "TEXT",
        "materialization_action": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
}


def malf_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `malf` 历史账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.malf


def connect_malf_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `malf` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(malf_ledger_path(workspace)), read_only=read_only)


def bootstrap_malf_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> Path:
    """创建或补齐 `malf` 最小正式表族。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    owns_connection = connection is None
    conn = connection or connect_malf_ledger(workspace)
    try:
        for ddl in MALF_LEDGER_TABLES.values():
            conn.execute(ddl)
        for table_name, required_columns in MALF_REQUIRED_COLUMNS.items():
            _ensure_columns(conn, table_name=table_name, required_columns=required_columns)
        return malf_ledger_path(workspace)
    finally:
        if owns_connection:
            conn.close()


def _ensure_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    required_columns: dict[str, str],
) -> None:
    existing_rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    existing_columns = {str(row[1]) for row in existing_rows}
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
