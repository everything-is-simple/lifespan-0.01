"""冻结 `structure snapshot` 最小正式账本三表的 bootstrap。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


STRUCTURE_RUN_TABLE: Final[str] = "structure_run"
STRUCTURE_WORK_QUEUE_TABLE: Final[str] = "structure_work_queue"
STRUCTURE_CHECKPOINT_TABLE: Final[str] = "structure_checkpoint"
STRUCTURE_SNAPSHOT_TABLE: Final[str] = "structure_snapshot"
STRUCTURE_RUN_SNAPSHOT_TABLE: Final[str] = "structure_run_snapshot"


STRUCTURE_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    STRUCTURE_RUN_TABLE,
    STRUCTURE_WORK_QUEUE_TABLE,
    STRUCTURE_CHECKPOINT_TABLE,
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
    STRUCTURE_WORK_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS structure_work_queue (
            queue_nk TEXT PRIMARY KEY,
            scope_nk TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            dirty_reason TEXT NOT NULL,
            replay_start_bar_dt DATE,
            replay_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            queue_status TEXT NOT NULL,
            enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            claimed_at TIMESTAMP,
            completed_at TIMESTAMP,
            first_seen_run_id TEXT,
            last_claimed_run_id TEXT,
            last_materialized_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    STRUCTURE_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS structure_checkpoint (
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            last_completed_bar_dt DATE,
            tail_start_bar_dt DATE,
            tail_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (asset_type, code, timeframe)
        )
    """,
    STRUCTURE_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS structure_snapshot (
            structure_snapshot_nk TEXT PRIMARY KEY,
            instrument TEXT NOT NULL,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            major_state TEXT NOT NULL,
            trend_direction TEXT NOT NULL,
            reversal_stage TEXT NOT NULL,
            wave_id BIGINT NOT NULL,
            current_hh_count BIGINT NOT NULL,
            current_ll_count BIGINT NOT NULL,
            daily_major_state TEXT,
            daily_trend_direction TEXT,
            daily_reversal_stage TEXT,
            daily_wave_id BIGINT,
            daily_current_hh_count BIGINT,
            daily_current_ll_count BIGINT,
            daily_source_context_nk TEXT,
            weekly_major_state TEXT,
            weekly_trend_direction TEXT,
            weekly_reversal_stage TEXT,
            weekly_wave_id BIGINT,
            weekly_current_hh_count BIGINT,
            weekly_current_ll_count BIGINT,
            weekly_source_context_nk TEXT,
            monthly_major_state TEXT,
            monthly_trend_direction TEXT,
            monthly_reversal_stage TEXT,
            monthly_wave_id BIGINT,
            monthly_current_hh_count BIGINT,
            monthly_current_ll_count BIGINT,
            monthly_source_context_nk TEXT,
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
    STRUCTURE_WORK_QUEUE_TABLE: {
        "queue_nk": "TEXT",
        "scope_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "dirty_reason": "TEXT",
        "replay_start_bar_dt": "DATE",
        "replay_confirm_until_dt": "DATE",
        "source_fingerprint": "TEXT",
        "queue_status": "TEXT",
        "enqueued_at": "TIMESTAMP",
        "claimed_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "first_seen_run_id": "TEXT",
        "last_claimed_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    STRUCTURE_CHECKPOINT_TABLE: {
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "last_completed_bar_dt": "DATE",
        "tail_start_bar_dt": "DATE",
        "tail_confirm_until_dt": "DATE",
        "source_fingerprint": "TEXT",
        "last_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    STRUCTURE_SNAPSHOT_TABLE: {
        "structure_snapshot_nk": "TEXT",
        "instrument": "TEXT",
        "signal_date": "DATE",
        "asof_date": "DATE",
        "major_state": "TEXT",
        "trend_direction": "TEXT",
        "reversal_stage": "TEXT",
        "wave_id": "BIGINT",
        "current_hh_count": "BIGINT",
        "current_ll_count": "BIGINT",
        "daily_major_state": "TEXT",
        "daily_trend_direction": "TEXT",
        "daily_reversal_stage": "TEXT",
        "daily_wave_id": "BIGINT",
        "daily_current_hh_count": "BIGINT",
        "daily_current_ll_count": "BIGINT",
        "daily_source_context_nk": "TEXT",
        "weekly_major_state": "TEXT",
        "weekly_trend_direction": "TEXT",
        "weekly_reversal_stage": "TEXT",
        "weekly_wave_id": "BIGINT",
        "weekly_current_hh_count": "BIGINT",
        "weekly_current_ll_count": "BIGINT",
        "weekly_source_context_nk": "TEXT",
        "monthly_major_state": "TEXT",
        "monthly_trend_direction": "TEXT",
        "monthly_reversal_stage": "TEXT",
        "monthly_wave_id": "BIGINT",
        "monthly_current_hh_count": "BIGINT",
        "monthly_current_ll_count": "BIGINT",
        "monthly_source_context_nk": "TEXT",
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

STRUCTURE_SNAPSHOT_REQUIRED_VALUE_COLUMNS: Final[tuple[str, ...]] = (
    "structure_snapshot_nk",
    "instrument",
    "signal_date",
    "asof_date",
    "major_state",
    "trend_direction",
    "reversal_stage",
    "wave_id",
    "current_hh_count",
    "current_ll_count",
    "structure_progress_state",
    "source_context_nk",
    "structure_contract_version",
    "first_seen_run_id",
    "last_materialized_run_id",
)

STRUCTURE_RUN_SNAPSHOT_REQUIRED_VALUE_COLUMNS: Final[tuple[str, ...]] = (
    "run_id",
    "structure_snapshot_nk",
    "materialization_action",
    "structure_progress_state",
)


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
        if _standardize_structure_snapshot_table(conn):
            _standardize_structure_run_snapshot_table(conn, force_rebuild=True)
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


def _standardize_structure_snapshot_table(connection: duckdb.DuckDBPyConnection) -> bool:
    if not _table_exists(connection, STRUCTURE_SNAPSHOT_TABLE):
        return False
    existing_columns = _list_columns(connection, STRUCTURE_SNAPSHOT_TABLE)
    unexpected_columns = {
        column_name
        for column_name in existing_columns
        if column_name not in STRUCTURE_REQUIRED_COLUMNS[STRUCTURE_SNAPSHOT_TABLE]
    }
    missing_required_value_columns = {
        column_name
        for column_name in STRUCTURE_SNAPSHOT_REQUIRED_VALUE_COLUMNS
        if column_name not in existing_columns
    }
    if not unexpected_columns and not missing_required_value_columns:
        return False

    transferable_columns = [
        column_name
        for column_name in STRUCTURE_REQUIRED_COLUMNS[STRUCTURE_SNAPSHOT_TABLE]
        if column_name in existing_columns
    ]
    temp_table_name = f"{STRUCTURE_SNAPSHOT_TABLE}__canonicalized"
    connection.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
    connection.execute(_build_table_ddl(STRUCTURE_SNAPSHOT_TABLE, temp_table_name))
    if not missing_required_value_columns and transferable_columns:
        transfer_predicate = " AND ".join(
            f"{column_name} IS NOT NULL" for column_name in STRUCTURE_SNAPSHOT_REQUIRED_VALUE_COLUMNS
        )
        transferable_sql = ", ".join(transferable_columns)
        connection.execute(
            f"""
            INSERT INTO {temp_table_name} ({transferable_sql})
            SELECT {transferable_sql}
            FROM {STRUCTURE_SNAPSHOT_TABLE}
            WHERE {transfer_predicate}
            """
        )
    connection.execute(f"DROP TABLE {STRUCTURE_SNAPSHOT_TABLE}")
    connection.execute(f"ALTER TABLE {temp_table_name} RENAME TO {STRUCTURE_SNAPSHOT_TABLE}")
    return True


def _standardize_structure_run_snapshot_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    force_rebuild: bool = False,
) -> None:
    if not _table_exists(connection, STRUCTURE_RUN_SNAPSHOT_TABLE):
        return
    existing_columns = _list_columns(connection, STRUCTURE_RUN_SNAPSHOT_TABLE)
    unexpected_columns = {
        column_name
        for column_name in existing_columns
        if column_name not in STRUCTURE_REQUIRED_COLUMNS[STRUCTURE_RUN_SNAPSHOT_TABLE]
    }
    missing_required_value_columns = {
        column_name
        for column_name in STRUCTURE_RUN_SNAPSHOT_REQUIRED_VALUE_COLUMNS
        if column_name not in existing_columns
    }
    if not force_rebuild and not unexpected_columns and not missing_required_value_columns:
        return

    transferable_columns = [
        column_name
        for column_name in STRUCTURE_REQUIRED_COLUMNS[STRUCTURE_RUN_SNAPSHOT_TABLE]
        if column_name in existing_columns
    ]
    temp_table_name = f"{STRUCTURE_RUN_SNAPSHOT_TABLE}__canonicalized"
    connection.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
    connection.execute(_build_table_ddl(STRUCTURE_RUN_SNAPSHOT_TABLE, temp_table_name))
    if not missing_required_value_columns and transferable_columns:
        transfer_predicate = " AND ".join(
            f"legacy.{column_name} IS NOT NULL"
            for column_name in STRUCTURE_RUN_SNAPSHOT_REQUIRED_VALUE_COLUMNS
        )
        transferable_sql = ", ".join(f"legacy.{column_name}" for column_name in transferable_columns)
        connection.execute(
            f"""
            INSERT INTO {temp_table_name} ({", ".join(transferable_columns)})
            SELECT {transferable_sql}
            FROM {STRUCTURE_RUN_SNAPSHOT_TABLE} AS legacy
            INNER JOIN {STRUCTURE_SNAPSHOT_TABLE} AS snapshot
                ON snapshot.structure_snapshot_nk = legacy.structure_snapshot_nk
            WHERE {transfer_predicate}
            """
        )
    connection.execute(f"DROP TABLE {STRUCTURE_RUN_SNAPSHOT_TABLE}")
    connection.execute(f"ALTER TABLE {temp_table_name} RENAME TO {STRUCTURE_RUN_SNAPSHOT_TABLE}")


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return row is not None


def _list_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> tuple[str, ...]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _build_table_ddl(template_table_name: str, actual_table_name: str) -> str:
    return STRUCTURE_LEDGER_DDL[template_table_name].replace(
        f"CREATE TABLE IF NOT EXISTS {template_table_name}",
        f"CREATE TABLE {actual_table_name}",
        1,
    )
