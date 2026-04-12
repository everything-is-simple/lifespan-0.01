"""冻结 `position` 模块最小公共账本层与方法分表 bootstrap。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings

from .position_bootstrap_schema import (
    DEFAULT_POSITION_POLICY_SEEDS,
    POSITION_LEDGER_DDL,
    POSITION_LEDGER_TABLE_NAMES,
    seed_default_policies,
)
from .position_materialization import (
    fetch_policy_contract,
    materialize_position_rows,
    register_position_run,
)
from .position_shared import (
    PositionFormalSignalInput,
    PositionMaterializationSummary,
    build_position_run_id,
    resolve_signal_contract_version,
    resolve_signal_run_id,
)


def connect_position_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `position` 账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(workspace.databases.position), read_only=read_only)


def bootstrap_position_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建最小 `position` 账本表族并写入默认 policy seed。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_position_ledger(workspace)
    try:
        for ddl in POSITION_LEDGER_DDL.values():
            conn.execute(ddl)
        seed_default_policies(conn)
        return POSITION_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def materialize_position_from_formal_signals(
    formal_signals: list[PositionFormalSignalInput] | tuple[PositionFormalSignalInput, ...],
    *,
    policy_id: str,
    settings: WorkspaceRoots | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
    run_id: str | None = None,
    default_single_name_cap_weight: float = 0.25,
    default_portfolio_cap_weight: float = 0.50,
    share_lot_size: int = 100,
) -> PositionMaterializationSummary:
    """把最小 `alpha formal signal` 样本落成 `position` 候选/容量/仓位事实。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_position_ledger(workspace)
    try:
        bootstrap_position_ledger(workspace, connection=conn)
        policy_family, policy_version, entry_leg_role_default = fetch_policy_contract(conn, policy_id)
        materialization_run_id = run_id or build_position_run_id()
        register_position_run(
            conn,
            run_id=materialization_run_id,
            source_signal_contract_version=resolve_signal_contract_version(formal_signals),
            source_signal_run_id=resolve_signal_run_id(formal_signals),
        )
        counts = materialize_position_rows(
            conn,
            formal_signals,
            policy_id=policy_id,
            policy_family=policy_family,
            policy_version=policy_version,
            entry_leg_role_default=entry_leg_role_default,
            default_single_name_cap_weight=default_single_name_cap_weight,
            default_portfolio_cap_weight=default_portfolio_cap_weight,
            share_lot_size=share_lot_size,
        )
        return PositionMaterializationSummary(
            run_id=materialization_run_id,
            policy_id=policy_id,
            candidate_count=len(formal_signals),
            admitted_count=counts.admitted_count,
            blocked_count=counts.blocked_count,
            sizing_count=len(formal_signals),
            family_snapshot_count=counts.family_snapshot_count,
        )
    finally:
        if owns_connection:
            conn.close()


def position_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `position` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.position
