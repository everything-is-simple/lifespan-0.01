"""冻结 `alpha formal signal` 最小正式账本三表的 bootstrap。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


ALPHA_FORMAL_SIGNAL_RUN_TABLE: Final[str] = "alpha_formal_signal_run"
ALPHA_FORMAL_SIGNAL_EVENT_TABLE: Final[str] = "alpha_formal_signal_event"
ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE: Final[str] = "alpha_formal_signal_run_event"


ALPHA_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    ALPHA_FORMAL_SIGNAL_RUN_TABLE,
    ALPHA_FORMAL_SIGNAL_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE,
)


ALPHA_LEDGER_DDL: Final[dict[str, str]] = {
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
        for ddl in ALPHA_LEDGER_DDL.values():
            conn.execute(ddl)
        return ALPHA_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def alpha_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `alpha` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.alpha
