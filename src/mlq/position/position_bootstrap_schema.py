"""承载 `position bootstrap` 的正式表族 DDL 与默认 policy seed。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import duckdb


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


def seed_default_policies(connection: duckdb.DuckDBPyConnection) -> None:
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
