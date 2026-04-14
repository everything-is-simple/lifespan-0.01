"""承载 `position bootstrap` 的正式表族 DDL、补列升级与默认 policy seed。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import duckdb


DEFAULT_POSITION_CONTRACT_VERSION: Final[str] = "position-malf-batched-entry-exit-v2"

POSITION_RUN_TABLE: Final[str] = "position_run"
POSITION_WORK_QUEUE_TABLE: Final[str] = "position_work_queue"
POSITION_CHECKPOINT_TABLE: Final[str] = "position_checkpoint"
POSITION_RUN_SNAPSHOT_TABLE: Final[str] = "position_run_snapshot"


POSITION_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    POSITION_RUN_TABLE,
    POSITION_WORK_QUEUE_TABLE,
    POSITION_CHECKPOINT_TABLE,
    POSITION_RUN_SNAPSHOT_TABLE,
    "position_policy_registry",
    "position_candidate_audit",
    "position_risk_budget_snapshot",
    "position_capacity_snapshot",
    "position_sizing_snapshot",
    "position_entry_leg_plan",
    "position_funding_fixed_notional_snapshot",
    "position_funding_single_lot_snapshot",
    "position_exit_plan",
    "position_exit_leg",
)


POSITION_LEDGER_DDL: Final[dict[str, str]] = {
    POSITION_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS position_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT,
            runner_version TEXT,
            run_status TEXT NOT NULL,
            execution_mode TEXT,
            policy_id TEXT,
            bounded_signal_count BIGINT NOT NULL DEFAULT 0,
            source_signal_contract_version TEXT,
            source_signal_run_id TEXT,
            run_started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            run_completed_at TIMESTAMP,
            inserted_count BIGINT NOT NULL DEFAULT 0,
            reused_count BIGINT NOT NULL DEFAULT 0,
            rematerialized_count BIGINT NOT NULL DEFAULT 0,
            queue_enqueued_count BIGINT NOT NULL DEFAULT 0,
            queue_claimed_count BIGINT NOT NULL DEFAULT 0,
            checkpoint_upserted_count BIGINT NOT NULL DEFAULT 0,
            notes TEXT,
            summary_json TEXT
        )
    """,
    POSITION_WORK_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS position_work_queue (
            queue_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            checkpoint_nk TEXT NOT NULL,
            signal_nk TEXT NOT NULL,
            instrument TEXT NOT NULL,
            reference_trade_date DATE NOT NULL,
            source_signal_fingerprint TEXT NOT NULL,
            queue_reason TEXT NOT NULL,
            queue_status TEXT NOT NULL,
            queued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            claimed_at TIMESTAMP,
            completed_at TIMESTAMP,
            first_seen_run_id TEXT,
            last_claimed_run_id TEXT,
            last_materialized_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    POSITION_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS position_checkpoint (
            checkpoint_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            instrument TEXT NOT NULL,
            checkpoint_scope TEXT NOT NULL,
            last_signal_nk TEXT NOT NULL,
            last_reference_trade_date DATE NOT NULL,
            last_source_signal_fingerprint TEXT NOT NULL,
            last_completed_at TIMESTAMP,
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    POSITION_RUN_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS position_run_snapshot (
            run_id TEXT NOT NULL,
            candidate_nk TEXT NOT NULL,
            signal_nk TEXT NOT NULL,
            reference_trade_date DATE NOT NULL,
            materialization_action TEXT NOT NULL,
            queue_nk TEXT,
            queue_reason TEXT,
            candidate_status TEXT,
            position_action_decision TEXT,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, candidate_nk)
        )
    """,
    "position_policy_registry": """
        CREATE TABLE IF NOT EXISTS position_policy_registry (
            policy_id TEXT PRIMARY KEY,
            policy_family TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            position_contract_version TEXT NOT NULL DEFAULT 'position-malf-batched-entry-exit-v2',
            entry_leg_role_default TEXT NOT NULL,
            entry_schedule_stage_default TEXT NOT NULL DEFAULT 't+1',
            entry_schedule_lag_days_default INTEGER NOT NULL DEFAULT 1,
            trim_schedule_stage_default TEXT NOT NULL DEFAULT 't+1',
            trim_schedule_lag_days_default INTEGER NOT NULL DEFAULT 1,
            exit_schedule_stage_default TEXT NOT NULL DEFAULT 't+1',
            exit_schedule_lag_days_default INTEGER NOT NULL DEFAULT 1,
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
            asset_type TEXT NOT NULL DEFAULT 'stock',
            instrument TEXT NOT NULL,
            code TEXT,
            policy_id TEXT NOT NULL,
            reference_trade_date DATE NOT NULL,
            candidate_status TEXT NOT NULL,
            blocked_reason_code TEXT,
            context_code TEXT,
            context_behavior_profile TEXT,
            deployment_stage TEXT,
            candidate_contract_version TEXT NOT NULL DEFAULT 'position-malf-batched-entry-exit-v2',
            audit_note TEXT,
            source_signal_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_risk_budget_snapshot": """
        CREATE TABLE IF NOT EXISTS position_risk_budget_snapshot (
            risk_budget_snapshot_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            risk_budget_snapshot_role TEXT NOT NULL DEFAULT 'default',
            current_position_weight DOUBLE NOT NULL DEFAULT 0,
            context_behavior_profile TEXT,
            deployment_stage TEXT,
            risk_budget_weight DOUBLE NOT NULL DEFAULT 0,
            risk_budget_reason_code TEXT NOT NULL,
            context_cap_weight DOUBLE NOT NULL DEFAULT 0,
            context_cap_reason_code TEXT NOT NULL,
            single_name_cap_weight DOUBLE NOT NULL DEFAULT 0,
            single_name_cap_reason_code TEXT NOT NULL,
            portfolio_cap_weight DOUBLE NOT NULL DEFAULT 0,
            portfolio_cap_reason_code TEXT NOT NULL,
            remaining_single_name_capacity_weight DOUBLE NOT NULL DEFAULT 0,
            remaining_portfolio_capacity_weight DOUBLE NOT NULL DEFAULT 0,
            final_allowed_position_weight DOUBLE NOT NULL DEFAULT 0,
            required_reduction_weight DOUBLE NOT NULL DEFAULT 0,
            binding_cap_code TEXT NOT NULL,
            capacity_source_code TEXT NOT NULL,
            context_weight_rule_code TEXT,
            source_policy_family TEXT NOT NULL,
            source_policy_version TEXT NOT NULL,
            source_signal_contract_version TEXT,
            source_context_fingerprint TEXT,
            risk_budget_contract_version TEXT NOT NULL DEFAULT 'position-malf-batched-entry-exit-v2',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_capacity_snapshot": """
        CREATE TABLE IF NOT EXISTS position_capacity_snapshot (
            capacity_snapshot_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            risk_budget_snapshot_nk TEXT,
            capacity_snapshot_role TEXT NOT NULL,
            current_position_weight DOUBLE NOT NULL DEFAULT 0,
            context_behavior_profile TEXT,
            deployment_stage TEXT,
            risk_budget_weight DOUBLE NOT NULL DEFAULT 0,
            context_max_position_weight DOUBLE NOT NULL DEFAULT 0,
            single_name_cap_weight DOUBLE NOT NULL DEFAULT 0,
            portfolio_cap_weight DOUBLE NOT NULL DEFAULT 0,
            remaining_single_name_capacity_weight DOUBLE NOT NULL DEFAULT 0,
            remaining_portfolio_capacity_weight DOUBLE NOT NULL DEFAULT 0,
            final_allowed_position_weight DOUBLE NOT NULL DEFAULT 0,
            required_reduction_weight DOUBLE NOT NULL DEFAULT 0,
            binding_cap_code TEXT NOT NULL DEFAULT 'no_binding_cap',
            context_weight_rule_code TEXT,
            capacity_source_code TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_sizing_snapshot": """
        CREATE TABLE IF NOT EXISTS position_sizing_snapshot (
            sizing_snapshot_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            risk_budget_snapshot_nk TEXT,
            policy_id TEXT NOT NULL,
            entry_leg_role TEXT NOT NULL,
            context_behavior_profile TEXT,
            deployment_stage TEXT,
            schedule_stage TEXT NOT NULL DEFAULT 't+1',
            schedule_lag_days INTEGER NOT NULL DEFAULT 1,
            sizing_contract_version TEXT NOT NULL DEFAULT 'position-malf-batched-entry-exit-v2',
            entry_leg_count INTEGER NOT NULL DEFAULT 1,
            exit_plan_required BOOLEAN NOT NULL DEFAULT FALSE,
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
    "position_entry_leg_plan": """
        CREATE TABLE IF NOT EXISTS position_entry_leg_plan (
            entry_leg_nk TEXT PRIMARY KEY,
            candidate_nk TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            leg_role TEXT NOT NULL,
            leg_status TEXT NOT NULL,
            schedule_stage TEXT NOT NULL,
            schedule_lag_days INTEGER NOT NULL DEFAULT 1,
            leg_gate_reason TEXT,
            target_weight_after_leg DOUBLE NOT NULL DEFAULT 0,
            target_notional_after_leg DOUBLE NOT NULL DEFAULT 0,
            target_shares_after_leg BIGINT NOT NULL DEFAULT 0,
            context_behavior_profile TEXT,
            deployment_stage TEXT,
            plan_contract_version TEXT NOT NULL DEFAULT 'position-malf-batched-entry-exit-v2',
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
            candidate_nk TEXT,
            policy_id TEXT NOT NULL,
            exit_family TEXT NOT NULL,
            plan_role TEXT NOT NULL DEFAULT 'hold',
            exit_status TEXT NOT NULL,
            schedule_stage TEXT NOT NULL DEFAULT 't+1',
            schedule_lag_days INTEGER NOT NULL DEFAULT 1,
            planned_leg_count INTEGER NOT NULL DEFAULT 1,
            required_reduction_weight DOUBLE NOT NULL DEFAULT 0,
            target_weight_after_exit DOUBLE NOT NULL DEFAULT 0,
            plan_contract_version TEXT NOT NULL DEFAULT 'position-malf-batched-entry-exit-v2',
            hard_close_guard_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "position_exit_leg": """
        CREATE TABLE IF NOT EXISTS position_exit_leg (
            exit_leg_nk TEXT PRIMARY KEY,
            exit_plan_nk TEXT NOT NULL,
            exit_leg_seq INTEGER NOT NULL,
            leg_role TEXT NOT NULL DEFAULT 'closeout',
            schedule_stage TEXT NOT NULL DEFAULT 't+1',
            schedule_lag_days INTEGER NOT NULL DEFAULT 1,
            leg_gate_reason TEXT,
            exit_reason_code TEXT NOT NULL,
            target_weight_after_leg DOUBLE NOT NULL DEFAULT 0,
            target_qty_after BIGINT NOT NULL DEFAULT 0,
            is_partial_exit BOOLEAN NOT NULL DEFAULT FALSE,
            fallback_to_full_exit BOOLEAN NOT NULL DEFAULT FALSE,
            plan_contract_version TEXT NOT NULL DEFAULT 'position-malf-batched-entry-exit-v2',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
}


POSITION_LEDGER_EVOLUTION_DDL: Final[dict[str, tuple[str, ...]]] = {
    POSITION_RUN_TABLE: (
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS runner_name TEXT",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS runner_version TEXT",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS execution_mode TEXT",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS policy_id TEXT",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS bounded_signal_count BIGINT DEFAULT 0",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS inserted_count BIGINT DEFAULT 0",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS reused_count BIGINT DEFAULT 0",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS rematerialized_count BIGINT DEFAULT 0",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS queue_enqueued_count BIGINT DEFAULT 0",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS queue_claimed_count BIGINT DEFAULT 0",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS checkpoint_upserted_count BIGINT DEFAULT 0",
        "ALTER TABLE position_run ADD COLUMN IF NOT EXISTS summary_json TEXT",
    ),
    "position_policy_registry": (
        f"ALTER TABLE position_policy_registry ADD COLUMN IF NOT EXISTS position_contract_version TEXT DEFAULT '{DEFAULT_POSITION_CONTRACT_VERSION}'",
        "ALTER TABLE position_policy_registry ADD COLUMN IF NOT EXISTS entry_schedule_stage_default TEXT DEFAULT 't+1'",
        "ALTER TABLE position_policy_registry ADD COLUMN IF NOT EXISTS entry_schedule_lag_days_default INTEGER DEFAULT 1",
        "ALTER TABLE position_policy_registry ADD COLUMN IF NOT EXISTS trim_schedule_stage_default TEXT DEFAULT 't+1'",
        "ALTER TABLE position_policy_registry ADD COLUMN IF NOT EXISTS trim_schedule_lag_days_default INTEGER DEFAULT 1",
        "ALTER TABLE position_policy_registry ADD COLUMN IF NOT EXISTS exit_schedule_stage_default TEXT DEFAULT 't+1'",
        "ALTER TABLE position_policy_registry ADD COLUMN IF NOT EXISTS exit_schedule_lag_days_default INTEGER DEFAULT 1",
    ),
    "position_candidate_audit": (
        "ALTER TABLE position_candidate_audit ADD COLUMN IF NOT EXISTS asset_type TEXT DEFAULT 'stock'",
        "ALTER TABLE position_candidate_audit ADD COLUMN IF NOT EXISTS code TEXT",
        "ALTER TABLE position_candidate_audit ADD COLUMN IF NOT EXISTS context_behavior_profile TEXT",
        "ALTER TABLE position_candidate_audit ADD COLUMN IF NOT EXISTS deployment_stage TEXT",
        f"ALTER TABLE position_candidate_audit ADD COLUMN IF NOT EXISTS candidate_contract_version TEXT DEFAULT '{DEFAULT_POSITION_CONTRACT_VERSION}'",
    ),
    "position_capacity_snapshot": (
        "ALTER TABLE position_capacity_snapshot ADD COLUMN IF NOT EXISTS risk_budget_snapshot_nk TEXT",
        "ALTER TABLE position_capacity_snapshot ADD COLUMN IF NOT EXISTS context_behavior_profile TEXT",
        "ALTER TABLE position_capacity_snapshot ADD COLUMN IF NOT EXISTS deployment_stage TEXT",
        "ALTER TABLE position_capacity_snapshot ADD COLUMN IF NOT EXISTS risk_budget_weight DOUBLE DEFAULT 0",
        "ALTER TABLE position_capacity_snapshot ADD COLUMN IF NOT EXISTS single_name_cap_weight DOUBLE DEFAULT 0",
        "ALTER TABLE position_capacity_snapshot ADD COLUMN IF NOT EXISTS portfolio_cap_weight DOUBLE DEFAULT 0",
        "ALTER TABLE position_capacity_snapshot ADD COLUMN IF NOT EXISTS binding_cap_code TEXT DEFAULT 'no_binding_cap'",
        "ALTER TABLE position_capacity_snapshot ADD COLUMN IF NOT EXISTS context_weight_rule_code TEXT",
    ),
    "position_sizing_snapshot": (
        "ALTER TABLE position_sizing_snapshot ADD COLUMN IF NOT EXISTS risk_budget_snapshot_nk TEXT",
        "ALTER TABLE position_sizing_snapshot ADD COLUMN IF NOT EXISTS context_behavior_profile TEXT",
        "ALTER TABLE position_sizing_snapshot ADD COLUMN IF NOT EXISTS deployment_stage TEXT",
        "ALTER TABLE position_sizing_snapshot ADD COLUMN IF NOT EXISTS schedule_stage TEXT DEFAULT 't+1'",
        "ALTER TABLE position_sizing_snapshot ADD COLUMN IF NOT EXISTS schedule_lag_days INTEGER DEFAULT 1",
        f"ALTER TABLE position_sizing_snapshot ADD COLUMN IF NOT EXISTS sizing_contract_version TEXT DEFAULT '{DEFAULT_POSITION_CONTRACT_VERSION}'",
        "ALTER TABLE position_sizing_snapshot ADD COLUMN IF NOT EXISTS entry_leg_count INTEGER DEFAULT 1",
        "ALTER TABLE position_sizing_snapshot ADD COLUMN IF NOT EXISTS exit_plan_required BOOLEAN DEFAULT FALSE",
    ),
    "position_exit_plan": (
        "ALTER TABLE position_exit_plan ADD COLUMN IF NOT EXISTS candidate_nk TEXT",
        "ALTER TABLE position_exit_plan ADD COLUMN IF NOT EXISTS plan_role TEXT DEFAULT 'hold'",
        "ALTER TABLE position_exit_plan ADD COLUMN IF NOT EXISTS schedule_stage TEXT DEFAULT 't+1'",
        "ALTER TABLE position_exit_plan ADD COLUMN IF NOT EXISTS schedule_lag_days INTEGER DEFAULT 1",
        "ALTER TABLE position_exit_plan ADD COLUMN IF NOT EXISTS required_reduction_weight DOUBLE DEFAULT 0",
        "ALTER TABLE position_exit_plan ADD COLUMN IF NOT EXISTS target_weight_after_exit DOUBLE DEFAULT 0",
        f"ALTER TABLE position_exit_plan ADD COLUMN IF NOT EXISTS plan_contract_version TEXT DEFAULT '{DEFAULT_POSITION_CONTRACT_VERSION}'",
    ),
    "position_exit_leg": (
        "ALTER TABLE position_exit_leg ADD COLUMN IF NOT EXISTS leg_role TEXT DEFAULT 'closeout'",
        "ALTER TABLE position_exit_leg ADD COLUMN IF NOT EXISTS schedule_stage TEXT DEFAULT 't+1'",
        "ALTER TABLE position_exit_leg ADD COLUMN IF NOT EXISTS schedule_lag_days INTEGER DEFAULT 1",
        "ALTER TABLE position_exit_leg ADD COLUMN IF NOT EXISTS leg_gate_reason TEXT",
        "ALTER TABLE position_exit_leg ADD COLUMN IF NOT EXISTS target_weight_after_leg DOUBLE DEFAULT 0",
        f"ALTER TABLE position_exit_leg ADD COLUMN IF NOT EXISTS plan_contract_version TEXT DEFAULT '{DEFAULT_POSITION_CONTRACT_VERSION}'",
    ),
}


@dataclass(frozen=True)
class PositionPolicySeed:
    """描述 bootstrap 时写入的默认 policy 组合。"""

    policy_id: str
    policy_family: str
    policy_version: str
    position_contract_version: str
    entry_leg_role_default: str
    entry_schedule_stage_default: str
    entry_schedule_lag_days_default: int
    trim_schedule_stage_default: str
    trim_schedule_lag_days_default: int
    exit_schedule_stage_default: str
    exit_schedule_lag_days_default: int
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
        position_contract_version=DEFAULT_POSITION_CONTRACT_VERSION,
        entry_leg_role_default="initial_entry",
        entry_schedule_stage_default="t+1",
        entry_schedule_lag_days_default=1,
        trim_schedule_stage_default="t+1",
        trim_schedule_lag_days_default=1,
        exit_schedule_stage_default="t+1",
        exit_schedule_lag_days_default=1,
        exit_family="FULL_EXIT_CONTROL",
        is_active=True,
        effective_from="2026-04-09",
        effective_to=None,
        notes="position MALF sizing operating baseline",
    ),
    PositionPolicySeed(
        policy_id="single_lot_full_exit_v1",
        policy_family="SINGLE_LOT_CONTROL",
        policy_version="v1",
        position_contract_version=DEFAULT_POSITION_CONTRACT_VERSION,
        entry_leg_role_default="initial_entry",
        entry_schedule_stage_default="t+1",
        entry_schedule_lag_days_default=1,
        trim_schedule_stage_default="t+1",
        trim_schedule_lag_days_default=1,
        exit_schedule_stage_default="t+1",
        exit_schedule_lag_days_default=1,
        exit_family="FULL_EXIT_CONTROL",
        is_active=True,
        effective_from="2026-04-09",
        effective_to=None,
        notes="position single-lot floor sanity baseline",
    ),
    PositionPolicySeed(
        policy_id="fixed_notional_naive_trail_scale_out_50_50_v1",
        policy_family="FIXED_NOTIONAL_CONTROL",
        policy_version="v1",
        position_contract_version=DEFAULT_POSITION_CONTRACT_VERSION,
        entry_leg_role_default="initial_entry",
        entry_schedule_stage_default="t+1",
        entry_schedule_lag_days_default=1,
        trim_schedule_stage_default="t+1",
        trim_schedule_lag_days_default=1,
        exit_schedule_stage_default="t+1",
        exit_schedule_lag_days_default=1,
        exit_family="NAIVE_TRAIL_SCALE_OUT_50_50_CONTROL",
        is_active=True,
        effective_from="2026-04-09",
        effective_to=None,
        notes="position MALF sizing partial-exit baseline",
    ),
    PositionPolicySeed(
        policy_id="single_lot_naive_trail_scale_out_50_50_v1",
        policy_family="SINGLE_LOT_CONTROL",
        policy_version="v1",
        position_contract_version=DEFAULT_POSITION_CONTRACT_VERSION,
        entry_leg_role_default="initial_entry",
        entry_schedule_stage_default="t+1",
        entry_schedule_lag_days_default=1,
        trim_schedule_stage_default="t+1",
        trim_schedule_lag_days_default=1,
        exit_schedule_stage_default="t+1",
        exit_schedule_lag_days_default=1,
        exit_family="NAIVE_TRAIL_SCALE_OUT_50_50_CONTROL",
        is_active=True,
        effective_from="2026-04-09",
        effective_to=None,
        notes="position single-lot partial-exit floor sanity baseline",
    ),
)


def apply_position_schema_evolution(connection: duckdb.DuckDBPyConnection) -> None:
    """对已有 `position` 账本执行幂等补列，确保当前卡组合同可被旧库消费。"""

    existing_tables = {
        str(row[0])
        for row in connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
    }
    for table_name, statements in POSITION_LEDGER_EVOLUTION_DDL.items():
        if table_name not in existing_tables:
            continue
        for statement in statements:
            connection.execute(statement)


def seed_default_policies(connection: duckdb.DuckDBPyConnection) -> None:
    """以幂等方式写入当前激活的默认 policy 组合。"""

    insert_sql = """
        INSERT INTO position_policy_registry (
            policy_id,
            policy_family,
            policy_version,
            position_contract_version,
            entry_leg_role_default,
            entry_schedule_stage_default,
            entry_schedule_lag_days_default,
            trim_schedule_stage_default,
            trim_schedule_lag_days_default,
            exit_schedule_stage_default,
            exit_schedule_lag_days_default,
            exit_family,
            is_active,
            effective_from,
            effective_to,
            notes
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
                seed.position_contract_version,
                seed.entry_leg_role_default,
                seed.entry_schedule_stage_default,
                seed.entry_schedule_lag_days_default,
                seed.trim_schedule_stage_default,
                seed.trim_schedule_lag_days_default,
                seed.exit_schedule_stage_default,
                seed.exit_schedule_lag_days_default,
                seed.exit_family,
                seed.is_active,
                seed.effective_from,
                seed.effective_to,
                seed.notes,
                seed.policy_id,
            ],
        )
        connection.execute(
            """
            UPDATE position_policy_registry
            SET
                policy_family = ?,
                policy_version = ?,
                position_contract_version = ?,
                entry_leg_role_default = ?,
                entry_schedule_stage_default = ?,
                entry_schedule_lag_days_default = ?,
                trim_schedule_stage_default = ?,
                trim_schedule_lag_days_default = ?,
                exit_schedule_stage_default = ?,
                exit_schedule_lag_days_default = ?,
                exit_family = ?,
                is_active = ?,
                effective_from = ?,
                effective_to = ?,
                notes = ?
            WHERE policy_id = ?
            """,
            [
                seed.policy_family,
                seed.policy_version,
                seed.position_contract_version,
                seed.entry_leg_role_default,
                seed.entry_schedule_stage_default,
                seed.entry_schedule_lag_days_default,
                seed.trim_schedule_stage_default,
                seed.trim_schedule_lag_days_default,
                seed.exit_schedule_stage_default,
                seed.exit_schedule_lag_days_default,
                seed.exit_family,
                seed.is_active,
                seed.effective_from,
                seed.effective_to,
                seed.notes,
                seed.policy_id,
            ],
        )
