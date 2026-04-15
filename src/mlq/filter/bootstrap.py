"""冻结 `filter snapshot` 最小正式账本三表的 bootstrap。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


FILTER_RUN_TABLE: Final[str] = "filter_run"
FILTER_WORK_QUEUE_TABLE: Final[str] = "filter_work_queue"
FILTER_CHECKPOINT_TABLE: Final[str] = "filter_checkpoint"
FILTER_SNAPSHOT_TABLE: Final[str] = "filter_snapshot"
FILTER_RUN_SNAPSHOT_TABLE: Final[str] = "filter_run_snapshot"


FILTER_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    FILTER_RUN_TABLE,
    FILTER_WORK_QUEUE_TABLE,
    FILTER_CHECKPOINT_TABLE,
    FILTER_SNAPSHOT_TABLE,
    FILTER_RUN_SNAPSHOT_TABLE,
)


FILTER_LEDGER_DDL: Final[dict[str, str]] = {
    FILTER_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS filter_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            source_structure_table TEXT NOT NULL,
            source_context_table TEXT NOT NULL,
            filter_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    FILTER_WORK_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS filter_work_queue (
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
    FILTER_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS filter_checkpoint (
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
    FILTER_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS filter_snapshot (
            filter_snapshot_nk TEXT PRIMARY KEY,
            structure_snapshot_nk TEXT NOT NULL,
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
            trigger_admissible BOOLEAN NOT NULL,
            filter_gate_code TEXT NOT NULL,
            filter_reject_reason_code TEXT,
            primary_blocking_condition TEXT,
            blocking_conditions_json TEXT NOT NULL,
            admission_notes TEXT,
            break_confirmation_status TEXT,
            break_confirmation_ref TEXT,
            stats_snapshot_nk TEXT,
            exhaustion_risk_bucket TEXT,
            reversal_probability_bucket TEXT,
            source_context_nk TEXT NOT NULL,
            filter_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    FILTER_RUN_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS filter_run_snapshot (
            run_id TEXT NOT NULL,
            filter_snapshot_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            trigger_admissible BOOLEAN NOT NULL,
            filter_gate_code TEXT NOT NULL,
            filter_reject_reason_code TEXT,
            primary_blocking_condition TEXT,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, filter_snapshot_nk)
        )
    """,
}


FILTER_REQUIRED_COLUMNS: Final[dict[str, dict[str, str]]] = {
    FILTER_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "run_status": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_instrument_count": "BIGINT",
        "source_structure_table": "TEXT",
        "source_context_table": "TEXT",
        "filter_contract_version": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    FILTER_WORK_QUEUE_TABLE: {
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
    FILTER_CHECKPOINT_TABLE: {
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
    FILTER_SNAPSHOT_TABLE: {
        "filter_snapshot_nk": "TEXT",
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
        "trigger_admissible": "BOOLEAN",
        "filter_gate_code": "TEXT",
        "filter_reject_reason_code": "TEXT",
        "primary_blocking_condition": "TEXT",
        "blocking_conditions_json": "TEXT",
        "admission_notes": "TEXT",
        "break_confirmation_status": "TEXT",
        "break_confirmation_ref": "TEXT",
        "stats_snapshot_nk": "TEXT",
        "exhaustion_risk_bucket": "TEXT",
        "reversal_probability_bucket": "TEXT",
        "source_context_nk": "TEXT",
        "filter_contract_version": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    FILTER_RUN_SNAPSHOT_TABLE: {
        "run_id": "TEXT",
        "filter_snapshot_nk": "TEXT",
        "materialization_action": "TEXT",
        "trigger_admissible": "BOOLEAN",
        "filter_gate_code": "TEXT",
        "filter_reject_reason_code": "TEXT",
        "primary_blocking_condition": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
}


def connect_filter_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `filter` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(workspace.databases.filter), read_only=read_only)


def bootstrap_filter_snapshot_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建并迁移 `filter snapshot` 最小三表。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_filter_ledger(workspace)
    try:
        for ddl in FILTER_LEDGER_DDL.values():
            conn.execute(ddl)
        for table_name, column_map in FILTER_REQUIRED_COLUMNS.items():
            _ensure_columns(conn, table_name=table_name, required_columns=column_map)
        return FILTER_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def filter_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `filter` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.filter


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
