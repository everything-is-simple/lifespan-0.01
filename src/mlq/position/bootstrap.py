"""冻结 `position` 模块最小公共账本层与方法分表 bootstrap。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


POSITION_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    "position_run",
    "position_policy_registry",
    "position_candidate_audit",
    "position_capacity_snapshot",
    "position_sizing_snapshot",
    "position_funding_fixed_notional_snapshot",
    "position_funding_single_lot_snapshot",
    "position_exit_plan",
    "position_exit_leg",
)


POSITION_LEDGER_DDL: Final[dict[str, str]] = {
    "position_run": """
        CREATE TABLE IF NOT EXISTS position_run (
            run_id TEXT PRIMARY KEY,
            run_status TEXT NOT NULL,
            source_signal_contract_version TEXT,
            source_signal_run_id TEXT,
            run_started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            run_completed_at TIMESTAMP,
            notes TEXT
        )
    """,
    "position_policy_registry": """
        CREATE TABLE IF NOT EXISTS position_policy_registry (
            policy_id TEXT PRIMARY KEY,
            policy_family TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            entry_leg_role_default TEXT NOT NULL,
            exit_family TEXT NOT NULL,
            is_active BOOLEAN NOT NULL,
            effective_from DATE NOT NULL,
            effective_to DATE,
            notes TEXT
        )
    """,
    "position_candidate_audit": """
        CREATE TABLE IF NOT EXISTS position_candidate_audit (
            candidate_nk TEXT PRIMARY KEY,
            signal_nk TEXT NOT NULL,
            instrument TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            reference_trade_date DATE NOT NULL,
            candidate_status TEXT NOT NULL,
            blocked_reason_code TEXT,
            context_code TEXT,
            audit_note TEXT,
            source_signal_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_capacity_snapshot": """
        CREATE TABLE IF NOT EXISTS position_capacity_snapshot (
            capacity_snapshot_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            capacity_snapshot_role TEXT NOT NULL,
            current_position_weight DOUBLE NOT NULL DEFAULT 0,
            context_max_position_weight DOUBLE NOT NULL DEFAULT 0,
            remaining_single_name_capacity_weight DOUBLE NOT NULL DEFAULT 0,
            remaining_portfolio_capacity_weight DOUBLE NOT NULL DEFAULT 0,
            final_allowed_position_weight DOUBLE NOT NULL DEFAULT 0,
            required_reduction_weight DOUBLE NOT NULL DEFAULT 0,
            capacity_source_code TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_sizing_snapshot": """
        CREATE TABLE IF NOT EXISTS position_sizing_snapshot (
            sizing_snapshot_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            entry_leg_role TEXT NOT NULL,
            position_action_decision TEXT NOT NULL,
            target_weight DOUBLE NOT NULL DEFAULT 0,
            target_notional DOUBLE NOT NULL DEFAULT 0,
            target_shares BIGINT NOT NULL DEFAULT 0,
            final_allowed_position_weight DOUBLE NOT NULL DEFAULT 0,
            required_reduction_weight DOUBLE NOT NULL DEFAULT 0,
            reference_price DOUBLE,
            reference_trade_date DATE NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_funding_fixed_notional_snapshot": """
        CREATE TABLE IF NOT EXISTS position_funding_fixed_notional_snapshot (
            family_snapshot_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            target_notional_before_cap DOUBLE NOT NULL DEFAULT 0,
            target_shares_before_cap BIGINT NOT NULL DEFAULT 0,
            cap_trim_applied BOOLEAN NOT NULL DEFAULT FALSE,
            final_target_shares BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_funding_single_lot_snapshot": """
        CREATE TABLE IF NOT EXISTS position_funding_single_lot_snapshot (
            family_snapshot_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            min_lot_size BIGINT NOT NULL DEFAULT 100,
            lot_floor_applied BOOLEAN NOT NULL DEFAULT FALSE,
            final_target_shares BIGINT NOT NULL DEFAULT 0,
            fallback_reason_code TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_exit_plan": """
        CREATE TABLE IF NOT EXISTS position_exit_plan (
            exit_plan_nk TEXT PRIMARY KEY,
            position_nk TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            exit_family TEXT NOT NULL,
            exit_status TEXT NOT NULL,
            planned_leg_count INTEGER NOT NULL DEFAULT 1,
            hard_close_guard_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_exit_leg": """
        CREATE TABLE IF NOT EXISTS position_exit_leg (
            exit_leg_nk TEXT PRIMARY KEY,
            exit_plan_nk TEXT NOT NULL,
            exit_leg_seq INTEGER NOT NULL,
            exit_reason_code TEXT NOT NULL,
            target_qty_after BIGINT NOT NULL DEFAULT 0,
            is_partial_exit BOOLEAN NOT NULL DEFAULT FALSE,
            fallback_to_full_exit BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
}


@dataclass(frozen=True)
class PositionPolicySeed:
    """描述 bootstrap 时写入的默认 policy 组合。"""

    policy_id: str
    policy_family: str
    policy_version: str
    entry_leg_role_default: str
    exit_family: str
    is_active: bool
    effective_from: str
    effective_to: str | None
    notes: str


DEFAULT_POSITION_POLICY_SEEDS: Final[tuple[PositionPolicySeed, ...]] = (
    PositionPolicySeed(
        policy_id="fixed_notional_full_exit_v1",
        policy_family="FIXED_NOTIONAL_CONTROL",
        policy_version="v1",
        entry_leg_role_default="base_entry",
        exit_family="FULL_EXIT_CONTROL",
        is_active=True,
        effective_from="2026-04-09",
        effective_to=None,
        notes="position v1 operating control baseline",
    ),
    PositionPolicySeed(
        policy_id="single_lot_full_exit_v1",
        policy_family="SINGLE_LOT_CONTROL",
        policy_version="v1",
        entry_leg_role_default="base_entry",
        exit_family="FULL_EXIT_CONTROL",
        is_active=True,
        effective_from="2026-04-09",
        effective_to=None,
        notes="position v1 floor sanity baseline",
    ),
    PositionPolicySeed(
        policy_id="fixed_notional_naive_trail_scale_out_50_50_v1",
        policy_family="FIXED_NOTIONAL_CONTROL",
        policy_version="v1",
        entry_leg_role_default="base_entry",
        exit_family="NAIVE_TRAIL_SCALE_OUT_50_50_CONTROL",
        is_active=True,
        effective_from="2026-04-09",
        effective_to=None,
        notes="position v1 operating partial-exit control",
    ),
    PositionPolicySeed(
        policy_id="single_lot_naive_trail_scale_out_50_50_v1",
        policy_family="SINGLE_LOT_CONTROL",
        policy_version="v1",
        entry_leg_role_default="base_entry",
        exit_family="NAIVE_TRAIL_SCALE_OUT_50_50_CONTROL",
        is_active=True,
        effective_from="2026-04-09",
        effective_to=None,
        notes="position v1 floor sanity partial-exit control",
    ),
)


@dataclass(frozen=True)
class PositionFormalSignalInput:
    """描述 `alpha formal signal` 回接 `position` 的最小输入行。"""

    signal_nk: str
    instrument: str
    signal_date: str
    asof_date: str
    trigger_family: str
    trigger_type: str
    pattern_code: str
    formal_signal_status: str
    trigger_admissible: bool
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int
    source_trigger_event_nk: str
    signal_contract_version: str
    reference_trade_date: str
    reference_price: float | None = None
    capital_base_value: float | None = None
    current_position_weight: float = 0.0
    remaining_single_name_capacity_weight: float | None = None
    remaining_portfolio_capacity_weight: float | None = None
    blocked_reason_code: str | None = None
    audit_note: str | None = None
    source_signal_run_id: str | None = None


@dataclass(frozen=True)
class PositionMaterializationSummary:
    """总结一次最小 materialization 的落表结果。"""

    run_id: str
    policy_id: str
    candidate_count: int
    admitted_count: int
    blocked_count: int
    sizing_count: int
    family_snapshot_count: int


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
        _seed_default_policies(conn)
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
        policy_family, policy_version, entry_leg_role_default = _fetch_policy_contract(conn, policy_id)
        materialization_run_id = run_id or _build_position_run_id()
        _register_position_run(
            conn,
            run_id=materialization_run_id,
            source_signal_contract_version=_resolve_signal_contract_version(formal_signals),
            source_signal_run_id=_resolve_signal_run_id(formal_signals),
        )

        admitted_count = 0
        blocked_count = 0
        family_snapshot_count = 0

        for signal in formal_signals:
            candidate_status = _resolve_candidate_status(signal)
            blocked_reason_code = _resolve_blocked_reason_code(signal, candidate_status)
            candidate_nk = _build_candidate_nk(signal, policy_id)
            context_max_position_weight = _context_max_position_weight(signal)
            remaining_single_name_capacity_weight = _resolve_single_name_capacity_weight(
                signal,
                default_single_name_cap_weight=default_single_name_cap_weight,
            )
            remaining_portfolio_capacity_weight = _resolve_portfolio_capacity_weight(
                signal,
                default_portfolio_cap_weight=default_portfolio_cap_weight,
            )
            final_allowed_position_weight = _resolve_final_allowed_position_weight(
                candidate_status=candidate_status,
                context_max_position_weight=context_max_position_weight,
                remaining_single_name_capacity_weight=remaining_single_name_capacity_weight,
                remaining_portfolio_capacity_weight=remaining_portfolio_capacity_weight,
            )
            required_reduction_weight = max(
                signal.current_position_weight - final_allowed_position_weight,
                0.0,
            )
            position_action_decision = _resolve_position_action_decision(
                candidate_status=candidate_status,
                current_position_weight=signal.current_position_weight,
                final_allowed_position_weight=final_allowed_position_weight,
                required_reduction_weight=required_reduction_weight,
            )

            target_weight = 0.0 if position_action_decision == "reject_open" else final_allowed_position_weight
            target_notional = _resolve_target_notional(signal, target_weight=target_weight)
            target_shares_before_lot = _resolve_target_shares_before_lot(
                signal,
                target_notional=target_notional,
            )
            target_shares = _apply_share_lot_floor(
                target_shares_before_lot,
                share_lot_size=share_lot_size,
            )

            _insert_position_candidate_audit(
                conn,
                candidate_nk=candidate_nk,
                signal=signal,
                policy_id=policy_id,
                candidate_status=candidate_status,
                blocked_reason_code=blocked_reason_code,
            )
            _insert_position_capacity_snapshot(
                conn,
                candidate_nk=candidate_nk,
                signal=signal,
                context_max_position_weight=context_max_position_weight,
                remaining_single_name_capacity_weight=remaining_single_name_capacity_weight,
                remaining_portfolio_capacity_weight=remaining_portfolio_capacity_weight,
                final_allowed_position_weight=final_allowed_position_weight,
                required_reduction_weight=required_reduction_weight,
            )
            _insert_position_sizing_snapshot(
                conn,
                candidate_nk=candidate_nk,
                policy_id=policy_id,
                policy_version=policy_version,
                entry_leg_role_default=entry_leg_role_default,
                signal=signal,
                position_action_decision=position_action_decision,
                target_weight=target_weight,
                target_notional=target_notional,
                target_shares=target_shares,
                final_allowed_position_weight=final_allowed_position_weight,
                required_reduction_weight=required_reduction_weight,
            )
            family_snapshot_count += _insert_policy_family_snapshot(
                conn,
                policy_family=policy_family,
                policy_version=policy_version,
                policy_id=policy_id,
                candidate_nk=candidate_nk,
                signal=signal,
                share_lot_size=share_lot_size,
                context_max_position_weight=context_max_position_weight,
                final_allowed_position_weight=final_allowed_position_weight,
                target_shares_before_lot=target_shares_before_lot,
                final_target_shares=target_shares,
            )

            if candidate_status == "admitted":
                admitted_count += 1
            else:
                blocked_count += 1

        return PositionMaterializationSummary(
            run_id=materialization_run_id,
            policy_id=policy_id,
            candidate_count=len(formal_signals),
            admitted_count=admitted_count,
            blocked_count=blocked_count,
            sizing_count=len(formal_signals),
            family_snapshot_count=family_snapshot_count,
        )
    finally:
        if owns_connection:
            conn.close()


def position_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `position` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.position


def _seed_default_policies(connection: duckdb.DuckDBPyConnection) -> None:
    """以幂等方式写入当前激活的默认 policy 组合。"""

    insert_sql = """
        INSERT INTO position_policy_registry (
            policy_id,
            policy_family,
            policy_version,
            entry_leg_role_default,
            exit_family,
            is_active,
            effective_from,
            effective_to,
            notes
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM position_policy_registry
            WHERE policy_id = ?
        )
    """
    for seed in DEFAULT_POSITION_POLICY_SEEDS:
        connection.execute(
            insert_sql,
            [
                seed.policy_id,
                seed.policy_family,
                seed.policy_version,
                seed.entry_leg_role_default,
                seed.exit_family,
                seed.is_active,
                seed.effective_from,
                seed.effective_to,
                seed.notes,
                seed.policy_id,
            ],
        )


def _build_position_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"position-bootstrap-{timestamp}"


def _resolve_signal_contract_version(
    formal_signals: list[PositionFormalSignalInput] | tuple[PositionFormalSignalInput, ...]
) -> str | None:
    versions = {signal.signal_contract_version for signal in formal_signals if signal.signal_contract_version}
    if not versions:
        return None
    return ",".join(sorted(versions))


def _resolve_signal_run_id(
    formal_signals: list[PositionFormalSignalInput] | tuple[PositionFormalSignalInput, ...]
) -> str | None:
    run_ids = {signal.source_signal_run_id for signal in formal_signals if signal.source_signal_run_id}
    if not run_ids:
        return None
    return ",".join(sorted(run_ids))


def _register_position_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    source_signal_contract_version: str | None,
    source_signal_run_id: str | None,
) -> None:
    connection.execute(
        """
        INSERT INTO position_run (
            run_id,
            run_status,
            source_signal_contract_version,
            source_signal_run_id,
            run_completed_at,
            notes
        )
        SELECT ?, 'completed', ?, ?, CURRENT_TIMESTAMP, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM position_run
            WHERE run_id = ?
        )
        """,
        [
            run_id,
            source_signal_contract_version,
            source_signal_run_id,
            "minimal bootstrap materialization from alpha formal signal",
            run_id,
        ],
    )


def _fetch_policy_contract(
    connection: duckdb.DuckDBPyConnection,
    policy_id: str,
) -> tuple[str, str, str]:
    row = connection.execute(
        """
        SELECT policy_family, policy_version, entry_leg_role_default
        FROM position_policy_registry
        WHERE policy_id = ?
        """,
        [policy_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Unknown position policy: {policy_id}")
    return row[0], row[1], row[2]


def _resolve_candidate_status(signal: PositionFormalSignalInput) -> str:
    if signal.trigger_admissible and signal.formal_signal_status == "admitted":
        return "admitted"
    if signal.formal_signal_status in {"blocked", "deferred"}:
        return signal.formal_signal_status
    return "blocked"


def _resolve_blocked_reason_code(signal: PositionFormalSignalInput, candidate_status: str) -> str | None:
    if candidate_status == "admitted":
        return None
    if signal.blocked_reason_code:
        return signal.blocked_reason_code
    if not signal.trigger_admissible:
        return "alpha_not_admitted"
    return f"alpha_status_{candidate_status}"


def _build_candidate_nk(signal: PositionFormalSignalInput, policy_id: str) -> str:
    return "|".join((signal.signal_nk, policy_id, signal.reference_trade_date))


def _build_capacity_snapshot_nk(candidate_nk: str) -> str:
    return f"{candidate_nk}|default"


def _build_sizing_snapshot_nk(
    candidate_nk: str,
    *,
    entry_leg_role_default: str,
    policy_version: str,
) -> str:
    return f"{candidate_nk}|{entry_leg_role_default}|{policy_version}"


def _build_family_snapshot_nk(candidate_nk: str, *, policy_family: str, policy_version: str) -> str:
    return f"{candidate_nk}|{policy_family}|{policy_version}"


def _context_max_position_weight(signal: PositionFormalSignalInput) -> float:
    if signal.lifecycle_rank_total <= 0:
        conservative_rank_ratio = 0.0
    else:
        conservative_rank_ratio = signal.lifecycle_rank_high / signal.lifecycle_rank_total
    context_code = signal.malf_context_4
    if context_code == "BULL_MAINSTREAM":
        return 0.25 * (1 - conservative_rank_ratio)
    if context_code == "BULL_COUNTERTREND":
        return 0.25 * conservative_rank_ratio
    if context_code == "BEAR_COUNTERTREND":
        return 0.25 * (1 - conservative_rank_ratio) * 0.5
    if context_code == "BEAR_MAINSTREAM":
        return 0.0
    return 0.0


def _resolve_single_name_capacity_weight(
    signal: PositionFormalSignalInput,
    *,
    default_single_name_cap_weight: float,
) -> float:
    if signal.remaining_single_name_capacity_weight is not None:
        return max(signal.remaining_single_name_capacity_weight, 0.0)
    return max(default_single_name_cap_weight - signal.current_position_weight, 0.0)


def _resolve_portfolio_capacity_weight(
    signal: PositionFormalSignalInput,
    *,
    default_portfolio_cap_weight: float,
) -> float:
    if signal.remaining_portfolio_capacity_weight is not None:
        return max(signal.remaining_portfolio_capacity_weight, 0.0)
    return max(default_portfolio_cap_weight, 0.0)


def _resolve_final_allowed_position_weight(
    *,
    candidate_status: str,
    context_max_position_weight: float,
    remaining_single_name_capacity_weight: float,
    remaining_portfolio_capacity_weight: float,
) -> float:
    if candidate_status != "admitted":
        return 0.0
    return max(
        min(
            context_max_position_weight,
            remaining_single_name_capacity_weight,
            remaining_portfolio_capacity_weight,
        ),
        0.0,
    )


def _resolve_position_action_decision(
    *,
    candidate_status: str,
    current_position_weight: float,
    final_allowed_position_weight: float,
    required_reduction_weight: float,
) -> str:
    if candidate_status != "admitted" or final_allowed_position_weight <= 0:
        return "reject_open"
    if required_reduction_weight > 0:
        return "trim_to_context_cap"
    if current_position_weight > 0 and abs(current_position_weight - final_allowed_position_weight) < 1e-12:
        return "hold_at_cap"
    return "open_up_to_context_cap"


def _resolve_target_notional(signal: PositionFormalSignalInput, *, target_weight: float) -> float:
    if signal.capital_base_value is None:
        return 0.0
    return max(target_weight, 0.0) * signal.capital_base_value


def _resolve_target_shares_before_lot(
    signal: PositionFormalSignalInput,
    *,
    target_notional: float,
) -> int:
    if signal.reference_price is None or signal.reference_price <= 0:
        return 0
    return int(target_notional / signal.reference_price)


def _apply_share_lot_floor(target_shares_before_lot: int, *, share_lot_size: int) -> int:
    if target_shares_before_lot <= 0:
        return 0
    return (target_shares_before_lot // share_lot_size) * share_lot_size


def _insert_position_candidate_audit(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    signal: PositionFormalSignalInput,
    policy_id: str,
    candidate_status: str,
    blocked_reason_code: str | None,
) -> None:
    connection.execute(
        """
        INSERT INTO position_candidate_audit (
            candidate_nk,
            signal_nk,
            instrument,
            policy_id,
            reference_trade_date,
            candidate_status,
            blocked_reason_code,
            context_code,
            audit_note,
            source_signal_run_id
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM position_candidate_audit
            WHERE candidate_nk = ?
        )
        """,
        [
            candidate_nk,
            signal.signal_nk,
            signal.instrument,
            policy_id,
            signal.reference_trade_date,
            candidate_status,
            blocked_reason_code,
            signal.malf_context_4,
            signal.audit_note,
            signal.source_signal_run_id,
            candidate_nk,
        ],
    )


def _insert_position_capacity_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    signal: PositionFormalSignalInput,
    context_max_position_weight: float,
    remaining_single_name_capacity_weight: float,
    remaining_portfolio_capacity_weight: float,
    final_allowed_position_weight: float,
    required_reduction_weight: float,
) -> None:
    capacity_snapshot_nk = _build_capacity_snapshot_nk(candidate_nk)
    capacity_source_code = (
        "formal_position_capacity"
        if signal.remaining_single_name_capacity_weight is not None
        or signal.remaining_portfolio_capacity_weight is not None
        else "bootstrap_default_capacity"
    )
    connection.execute(
        """
        INSERT INTO position_capacity_snapshot (
            capacity_snapshot_nk,
            candidate_nk,
            capacity_snapshot_role,
            current_position_weight,
            context_max_position_weight,
            remaining_single_name_capacity_weight,
            remaining_portfolio_capacity_weight,
            final_allowed_position_weight,
            required_reduction_weight,
            capacity_source_code
        )
        SELECT ?, ?, 'default', ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM position_capacity_snapshot
            WHERE capacity_snapshot_nk = ?
        )
        """,
        [
            capacity_snapshot_nk,
            candidate_nk,
            signal.current_position_weight,
            context_max_position_weight,
            remaining_single_name_capacity_weight,
            remaining_portfolio_capacity_weight,
            final_allowed_position_weight,
            required_reduction_weight,
            capacity_source_code,
            capacity_snapshot_nk,
        ],
    )


def _insert_position_sizing_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    policy_id: str,
    policy_version: str,
    entry_leg_role_default: str,
    signal: PositionFormalSignalInput,
    position_action_decision: str,
    target_weight: float,
    target_notional: float,
    target_shares: int,
    final_allowed_position_weight: float,
    required_reduction_weight: float,
) -> None:
    sizing_snapshot_nk = _build_sizing_snapshot_nk(
        candidate_nk,
        entry_leg_role_default=entry_leg_role_default,
        policy_version=policy_version,
    )
    connection.execute(
        """
        INSERT INTO position_sizing_snapshot (
            sizing_snapshot_nk,
            candidate_nk,
            policy_id,
            entry_leg_role,
            position_action_decision,
            target_weight,
            target_notional,
            target_shares,
            final_allowed_position_weight,
            required_reduction_weight,
            reference_price,
            reference_trade_date
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM position_sizing_snapshot
            WHERE sizing_snapshot_nk = ?
        )
        """,
        [
            sizing_snapshot_nk,
            candidate_nk,
            policy_id,
            entry_leg_role_default,
            position_action_decision,
            target_weight,
            target_notional,
            target_shares,
            final_allowed_position_weight,
            required_reduction_weight,
            signal.reference_price,
            signal.reference_trade_date,
            sizing_snapshot_nk,
        ],
    )


def _insert_policy_family_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    policy_family: str,
    policy_version: str,
    policy_id: str,
    candidate_nk: str,
    signal: PositionFormalSignalInput,
    share_lot_size: int,
    context_max_position_weight: float,
    final_allowed_position_weight: float,
    target_shares_before_lot: int,
    final_target_shares: int,
) -> int:
    family_snapshot_nk = _build_family_snapshot_nk(
        candidate_nk,
        policy_family=policy_family,
        policy_version=policy_version,
    )
    if policy_family == "FIXED_NOTIONAL_CONTROL":
        target_notional_before_cap = _resolve_target_notional(
            signal,
            target_weight=context_max_position_weight,
        )
        target_shares_before_cap = _resolve_target_shares_before_lot(
            signal,
            target_notional=target_notional_before_cap,
        )
        connection.execute(
            """
            INSERT INTO position_funding_fixed_notional_snapshot (
                family_snapshot_nk,
                candidate_nk,
                policy_id,
                target_notional_before_cap,
                target_shares_before_cap,
                cap_trim_applied,
                final_target_shares
            )
            SELECT ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM position_funding_fixed_notional_snapshot
                WHERE family_snapshot_nk = ?
            )
            """,
            [
                family_snapshot_nk,
                candidate_nk,
                policy_id,
                target_notional_before_cap,
                target_shares_before_cap,
                final_allowed_position_weight < context_max_position_weight,
                final_target_shares,
                family_snapshot_nk,
            ],
        )
        return 1
    if policy_family == "SINGLE_LOT_CONTROL":
        lot_floor_applied = target_shares_before_lot != final_target_shares
        fallback_reason_code = None
        if final_allowed_position_weight > 0 and final_target_shares == 0:
            fallback_reason_code = "insufficient_notional_for_single_lot"
        connection.execute(
            """
            INSERT INTO position_funding_single_lot_snapshot (
                family_snapshot_nk,
                candidate_nk,
                policy_id,
                min_lot_size,
                lot_floor_applied,
                final_target_shares,
                fallback_reason_code
            )
            SELECT ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM position_funding_single_lot_snapshot
                WHERE family_snapshot_nk = ?
            )
            """,
            [
                family_snapshot_nk,
                candidate_nk,
                policy_id,
                share_lot_size,
                lot_floor_applied,
                final_target_shares,
                fallback_reason_code,
                family_snapshot_nk,
            ],
        )
        return 1
    return 0
