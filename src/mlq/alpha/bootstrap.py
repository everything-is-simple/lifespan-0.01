"""冻结 `alpha` 模块正式账本表族的 bootstrap。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


ALPHA_TRIGGER_RUN_TABLE: Final[str] = "alpha_trigger_run"
ALPHA_TRIGGER_EVENT_TABLE: Final[str] = "alpha_trigger_event"
ALPHA_TRIGGER_RUN_EVENT_TABLE: Final[str] = "alpha_trigger_run_event"

ALPHA_FORMAL_SIGNAL_RUN_TABLE: Final[str] = "alpha_formal_signal_run"
ALPHA_FORMAL_SIGNAL_EVENT_TABLE: Final[str] = "alpha_formal_signal_event"
ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE: Final[str] = "alpha_formal_signal_run_event"


ALPHA_TRIGGER_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    ALPHA_TRIGGER_RUN_TABLE,
    ALPHA_TRIGGER_EVENT_TABLE,
    ALPHA_TRIGGER_RUN_EVENT_TABLE,
)


ALPHA_FORMAL_SIGNAL_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    ALPHA_FORMAL_SIGNAL_RUN_TABLE,
    ALPHA_FORMAL_SIGNAL_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE,
)


ALPHA_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    *ALPHA_TRIGGER_LEDGER_TABLE_NAMES,
    *ALPHA_FORMAL_SIGNAL_LEDGER_TABLE_NAMES,
)


ALPHA_LEDGER_DDL: Final[dict[str, str]] = {
    ALPHA_TRIGGER_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_trigger_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            candidate_trigger_count BIGINT NOT NULL DEFAULT 0,
            source_trigger_input_table TEXT NOT NULL,
            source_filter_table TEXT NOT NULL,
            source_structure_table TEXT NOT NULL,
            trigger_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT,
            notes TEXT
        )
    """,
    ALPHA_TRIGGER_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_trigger_event (
            trigger_event_nk TEXT PRIMARY KEY,
            instrument TEXT NOT NULL,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            trigger_family TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            pattern_code TEXT NOT NULL,
            source_filter_snapshot_nk TEXT NOT NULL,
            source_structure_snapshot_nk TEXT NOT NULL,
            upstream_context_fingerprint TEXT NOT NULL,
            trigger_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    ALPHA_TRIGGER_RUN_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_trigger_run_event (
            run_id TEXT NOT NULL,
            trigger_event_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, trigger_event_nk)
        )
    """,
    ALPHA_FORMAL_SIGNAL_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_formal_signal_run (
            run_id TEXT PRIMARY KEY,
            producer_name TEXT NOT NULL,
            producer_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            source_trigger_table TEXT NOT NULL,
            source_context_table TEXT NOT NULL,
            signal_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT,
            notes TEXT
        )
    """,
    ALPHA_FORMAL_SIGNAL_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_formal_signal_event (
            signal_nk TEXT PRIMARY KEY,
            instrument TEXT NOT NULL,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            trigger_family TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            pattern_code TEXT NOT NULL,
            formal_signal_status TEXT NOT NULL,
            trigger_admissible BOOLEAN NOT NULL,
            malf_context_4 TEXT NOT NULL,
            lifecycle_rank_high BIGINT NOT NULL,
            lifecycle_rank_total BIGINT NOT NULL,
            source_trigger_event_nk TEXT NOT NULL,
            signal_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_formal_signal_run_event (
            run_id TEXT NOT NULL,
            signal_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            formal_signal_status TEXT NOT NULL,
            source_trigger_event_nk TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, signal_nk)
        )
    """,
}


def connect_alpha_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `alpha` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(workspace.databases.alpha), read_only=read_only)


def bootstrap_alpha_trigger_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建 `alpha trigger ledger` 最小三表。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_alpha_ledger(workspace)
    try:
        for table_name in ALPHA_TRIGGER_LEDGER_TABLE_NAMES:
            conn.execute(ALPHA_LEDGER_DDL[table_name])
        return ALPHA_TRIGGER_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def bootstrap_alpha_formal_signal_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建 `alpha formal signal` 三表。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_alpha_ledger(workspace)
    try:
        for table_name in ALPHA_FORMAL_SIGNAL_LEDGER_TABLE_NAMES:
            conn.execute(ALPHA_LEDGER_DDL[table_name])
        return ALPHA_FORMAL_SIGNAL_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def alpha_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `alpha` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.alpha
