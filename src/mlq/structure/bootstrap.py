"""冻结 `structure snapshot` 最小正式账本三表的 bootstrap。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


STRUCTURE_RUN_TABLE: Final[str] = "structure_run"
STRUCTURE_SNAPSHOT_TABLE: Final[str] = "structure_snapshot"
STRUCTURE_RUN_SNAPSHOT_TABLE: Final[str] = "structure_run_snapshot"


STRUCTURE_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    STRUCTURE_RUN_TABLE,
    STRUCTURE_SNAPSHOT_TABLE,
    STRUCTURE_RUN_SNAPSHOT_TABLE,
)


STRUCTURE_LEDGER_DDL: Final[dict[str, str]] = {
    STRUCTURE_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS structure_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            source_context_table TEXT NOT NULL,
            source_structure_input_table TEXT NOT NULL,
            structure_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    STRUCTURE_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS structure_snapshot (
            structure_snapshot_nk TEXT PRIMARY KEY,
            instrument TEXT NOT NULL,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            malf_context_4 TEXT NOT NULL,
            lifecycle_rank_high BIGINT NOT NULL,
            lifecycle_rank_total BIGINT NOT NULL,
            new_high_count BIGINT NOT NULL,
            new_low_count BIGINT NOT NULL,
            refresh_density DOUBLE NOT NULL,
            advancement_density DOUBLE NOT NULL,
            is_failed_extreme BOOLEAN NOT NULL,
            failure_type TEXT,
            structure_progress_state TEXT NOT NULL,
            break_confirmation_status TEXT,
            break_confirmation_ref TEXT,
            stats_snapshot_nk TEXT,
            exhaustion_risk_bucket TEXT,
            reversal_probability_bucket TEXT,
            source_context_nk TEXT NOT NULL,
            structure_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    STRUCTURE_RUN_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS structure_run_snapshot (
            run_id TEXT NOT NULL,
            structure_snapshot_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            structure_progress_state TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, structure_snapshot_nk)
        )
    """,
}


STRUCTURE_REQUIRED_COLUMNS: Final[dict[str, dict[str, str]]] = {
    STRUCTURE_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "run_status": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_instrument_count": "BIGINT",
        "source_context_table": "TEXT",
        "source_structure_input_table": "TEXT",
        "structure_contract_version": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    STRUCTURE_SNAPSHOT_TABLE: {
        "structure_snapshot_nk": "TEXT",
        "instrument": "TEXT",
        "signal_date": "DATE",
        "asof_date": "DATE",
        "malf_context_4": "TEXT",
        "lifecycle_rank_high": "BIGINT",
        "lifecycle_rank_total": "BIGINT",
        "new_high_count": "BIGINT",
        "new_low_count": "BIGINT",
        "refresh_density": "DOUBLE",
        "advancement_density": "DOUBLE",
        "is_failed_extreme": "BOOLEAN",
        "failure_type": "TEXT",
        "structure_progress_state": "TEXT",
        "break_confirmation_status": "TEXT",
        "break_confirmation_ref": "TEXT",
        "stats_snapshot_nk": "TEXT",
        "exhaustion_risk_bucket": "TEXT",
        "reversal_probability_bucket": "TEXT",
        "source_context_nk": "TEXT",
        "structure_contract_version": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    STRUCTURE_RUN_SNAPSHOT_TABLE: {
        "run_id": "TEXT",
        "structure_snapshot_nk": "TEXT",
        "materialization_action": "TEXT",
        "structure_progress_state": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
}


def connect_structure_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `structure` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(workspace.databases.structure), read_only=read_only)


def bootstrap_structure_snapshot_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建并迁移 `structure snapshot` 最小三表。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_structure_ledger(workspace)
    try:
        for ddl in STRUCTURE_LEDGER_DDL.values():
            conn.execute(ddl)
        for table_name, column_map in STRUCTURE_REQUIRED_COLUMNS.items():
            _ensure_columns(conn, table_name=table_name, required_columns=column_map)
        return STRUCTURE_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def structure_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `structure` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.structure


def _ensure_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    required_columns: dict[str, str],
) -> None:
    existing_rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchall()
    existing_columns = {str(row[0]) for row in existing_rows}
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
