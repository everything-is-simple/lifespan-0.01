"""承载 `position bootstrap` 的账本物化与落表逻辑。"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .position_contract_logic import (
    PositionContextContract,
    PositionEntryLegPlan,
    PositionExitLeg,
    PositionExitPlan,
    PositionPolicyContract,
    PositionRiskBudgetSnapshot,
    apply_share_lot_floor,
    build_candidate_nk,
    build_capacity_snapshot_nk,
    build_entry_leg_plans,
    build_exit_plan_bundles,
    build_family_snapshot_nk,
    build_risk_budget_snapshot_nk,
    build_sizing_snapshot_nk,
    resolve_blocked_reason_code,
    resolve_capacity_source_code,
    resolve_candidate_status,
    resolve_binding_cap_code,
    resolve_context_contract,
    resolve_final_allowed_position_weight,
    resolve_portfolio_capacity_weight,
    resolve_position_action_decision,
    resolve_risk_budget_weight,
    resolve_single_name_capacity_weight,
    resolve_sizing_leg_role,
    resolve_sizing_schedule,
    resolve_target_notional,
    resolve_target_shares_before_lot,
    resolve_target_weight,
)
from .position_shared import PositionFormalSignalInput


@dataclass(frozen=True)
class PositionMaterializationCounts:
    """汇总一次 bounded position 物化产生的内部计数。"""

    admitted_count: int
    blocked_count: int
    risk_budget_count: int
    family_snapshot_count: int
    entry_leg_count: int
    exit_plan_count: int
    exit_leg_count: int


def register_position_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    source_signal_contract_version: str | None,
    source_signal_run_id: str | None,
) -> None:
    """登记一次 `position` materialization run 审计行。"""

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
            "MALF context driven position materialization from alpha formal signal",
            run_id,
        ],
    )


def fetch_policy_contract(
    connection: duckdb.DuckDBPyConnection,
    policy_id: str,
) -> PositionPolicyContract:
    """读取一次物化所绑定的 policy 契约。"""

    row = connection.execute(
        """
        SELECT
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
            exit_family
        FROM position_policy_registry
        WHERE policy_id = ?
        """,
        [policy_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Unknown position policy: {policy_id}")
    return PositionPolicyContract(
        policy_family=str(row[0]),
        policy_version=str(row[1]),
        position_contract_version=str(row[2]),
        entry_leg_role_default=str(row[3]),
        entry_schedule_stage_default=str(row[4]),
        entry_schedule_lag_days_default=int(row[5]),
        trim_schedule_stage_default=str(row[6]),
        trim_schedule_lag_days_default=int(row[7]),
        exit_schedule_stage_default=str(row[8]),
        exit_schedule_lag_days_default=int(row[9]),
        exit_family=str(row[10]),
    )


def materialize_position_rows(
    connection: duckdb.DuckDBPyConnection,
    formal_signals: list[PositionFormalSignalInput] | tuple[PositionFormalSignalInput, ...],
    *,
    policy_id: str,
    policy_contract: PositionPolicyContract,
    default_single_name_cap_weight: float,
    default_portfolio_cap_weight: float,
    share_lot_size: int,
) -> PositionMaterializationCounts:
    """把 bounded `alpha formal signal` 样本写成 `position` 账本事实。"""

    admitted_count = 0
    blocked_count = 0
    risk_budget_count = 0
    family_snapshot_count = 0
    entry_leg_count = 0
    exit_plan_count = 0
    exit_leg_count = 0

    for signal in formal_signals:
        candidate_status = resolve_candidate_status(signal)
        blocked_reason_code = resolve_blocked_reason_code(signal, candidate_status)
        candidate_nk = build_candidate_nk(signal, policy_id)
        context_contract = resolve_context_contract(signal)
        risk_budget_weight = resolve_risk_budget_weight(
            default_single_name_cap_weight=default_single_name_cap_weight,
        )
        single_name_cap_weight = resolve_single_name_capacity_weight(
            signal,
            default_single_name_cap_weight=default_single_name_cap_weight,
        )
        portfolio_cap_weight = resolve_portfolio_capacity_weight(
            signal,
            default_portfolio_cap_weight=default_portfolio_cap_weight,
        )
        final_allowed_position_weight = resolve_final_allowed_position_weight(
            candidate_status=candidate_status,
            risk_budget_weight=risk_budget_weight,
            context_max_position_weight=context_contract.context_max_position_weight,
            single_name_cap_weight=single_name_cap_weight,
            portfolio_cap_weight=portfolio_cap_weight,
        )
        required_reduction_weight = max(
            signal.current_position_weight - final_allowed_position_weight,
            0.0,
        )
        capacity_source_code = resolve_capacity_source_code(signal)
        binding_cap_code = resolve_binding_cap_code(
            candidate_status=candidate_status,
            blocked_reason_code=blocked_reason_code,
            risk_budget_weight=risk_budget_weight,
            context_max_position_weight=context_contract.context_max_position_weight,
            single_name_cap_weight=single_name_cap_weight,
            portfolio_cap_weight=portfolio_cap_weight,
            final_allowed_position_weight=final_allowed_position_weight,
        )
        risk_budget_snapshot = PositionRiskBudgetSnapshot(
            risk_budget_snapshot_nk=build_risk_budget_snapshot_nk(candidate_nk),
            risk_budget_weight=risk_budget_weight,
            risk_budget_reason_code="operating_baseline_fixed_notional",
            context_cap_weight=context_contract.context_max_position_weight,
            context_cap_reason_code=context_contract.context_weight_rule_code,
            single_name_cap_weight=single_name_cap_weight,
            single_name_cap_reason_code=(
                "formal_single_name_capacity"
                if signal.remaining_single_name_capacity_weight is not None
                else "default_single_name_cap"
            ),
            portfolio_cap_weight=portfolio_cap_weight,
            portfolio_cap_reason_code=(
                "formal_portfolio_capacity"
                if signal.remaining_portfolio_capacity_weight is not None
                else "default_portfolio_cap"
            ),
            remaining_single_name_capacity_weight=single_name_cap_weight,
            remaining_portfolio_capacity_weight=portfolio_cap_weight,
            final_allowed_position_weight=final_allowed_position_weight,
            required_reduction_weight=required_reduction_weight,
            binding_cap_code=binding_cap_code,
            capacity_source_code=capacity_source_code,
        )
        position_action_decision = resolve_position_action_decision(
            signal=signal,
            candidate_status=candidate_status,
            final_allowed_position_weight=final_allowed_position_weight,
            required_reduction_weight=required_reduction_weight,
        )
        sizing_leg_role = resolve_sizing_leg_role(
            position_action_decision,
            entry_leg_role_default=policy_contract.entry_leg_role_default,
        )
        sizing_schedule_stage, sizing_schedule_lag_days = resolve_sizing_schedule(
            context_contract=context_contract,
            policy_contract=policy_contract,
            position_action_decision=position_action_decision,
        )
        target_weight = resolve_target_weight(
            position_action_decision=position_action_decision,
            final_allowed_position_weight=final_allowed_position_weight,
        )
        target_notional = resolve_target_notional(signal, target_weight=target_weight)
        target_shares_before_lot = resolve_target_shares_before_lot(
            signal,
            target_notional=target_notional,
        )
        target_shares = apply_share_lot_floor(
            target_shares_before_lot,
            share_lot_size=share_lot_size,
        )
        entry_leg_plans = build_entry_leg_plans(
            signal=signal,
            candidate_nk=candidate_nk,
            policy_contract=policy_contract,
            context_contract=context_contract,
            candidate_status=candidate_status,
            position_action_decision=position_action_decision,
            final_allowed_position_weight=final_allowed_position_weight,
            share_lot_size=share_lot_size,
        )
        exit_plan_bundles = build_exit_plan_bundles(
            signal=signal,
            candidate_nk=candidate_nk,
            policy_contract=policy_contract,
            position_action_decision=position_action_decision,
            blocked_reason_code=blocked_reason_code,
            final_allowed_position_weight=final_allowed_position_weight,
            required_reduction_weight=required_reduction_weight,
            target_shares=target_shares,
            share_lot_size=share_lot_size,
        )

        _insert_position_candidate_audit(
            connection,
            candidate_nk=candidate_nk,
            signal=signal,
            policy_id=policy_id,
            candidate_status=candidate_status,
            blocked_reason_code=blocked_reason_code,
            context_contract=context_contract,
            candidate_contract_version=policy_contract.position_contract_version,
        )
        _insert_position_risk_budget_snapshot(
            connection,
            candidate_nk=candidate_nk,
            policy_id=policy_id,
            policy_contract=policy_contract,
            signal=signal,
            context_contract=context_contract,
            risk_budget_snapshot=risk_budget_snapshot,
        )
        _insert_position_capacity_snapshot(
            connection,
            candidate_nk=candidate_nk,
            current_position_weight=signal.current_position_weight,
            context_contract=context_contract,
            risk_budget_snapshot=risk_budget_snapshot,
        )
        _insert_position_sizing_snapshot(
            connection,
            candidate_nk=candidate_nk,
            risk_budget_snapshot_nk=risk_budget_snapshot.risk_budget_snapshot_nk,
            policy_id=policy_id,
            policy_contract=policy_contract,
            sizing_leg_role=sizing_leg_role,
            signal=signal,
            context_contract=context_contract,
            position_action_decision=position_action_decision,
            sizing_schedule_stage=sizing_schedule_stage,
            sizing_schedule_lag_days=sizing_schedule_lag_days,
            entry_leg_count=len(entry_leg_plans),
            exit_plan_required=bool(exit_plan_bundles),
            target_weight=target_weight,
            target_notional=target_notional,
            target_shares=target_shares,
            final_allowed_position_weight=final_allowed_position_weight,
            required_reduction_weight=required_reduction_weight,
        )
        for entry_leg_plan in entry_leg_plans:
            _insert_position_entry_leg_plan(
                connection,
                candidate_nk=candidate_nk,
                policy_id=policy_id,
                policy_contract=policy_contract,
                context_contract=context_contract,
                entry_leg_plan=entry_leg_plan,
            )
        family_snapshot_count += _insert_policy_family_snapshot(
            connection,
            policy_contract=policy_contract,
            policy_id=policy_id,
            candidate_nk=candidate_nk,
            signal=signal,
            share_lot_size=share_lot_size,
            risk_budget_weight=risk_budget_weight,
            final_allowed_position_weight=final_allowed_position_weight,
            target_shares_before_lot=target_shares_before_lot,
            final_target_shares=target_shares,
        )
        for exit_plan_bundle in exit_plan_bundles:
            _insert_position_exit_plan(
                connection,
                candidate_nk=candidate_nk,
                policy_id=policy_id,
                policy_contract=policy_contract,
                exit_plan=exit_plan_bundle.exit_plan,
            )
            exit_plan_count += 1
            for exit_leg in exit_plan_bundle.exit_legs:
                _insert_position_exit_leg(
                    connection,
                    policy_contract=policy_contract,
                    exit_plan_nk=exit_plan_bundle.exit_plan.exit_plan_nk,
                    exit_leg=exit_leg,
                )
                exit_leg_count += 1

        entry_leg_count += len(entry_leg_plans)
        risk_budget_count += 1
        if candidate_status == "admitted":
            admitted_count += 1
        else:
            blocked_count += 1

    return PositionMaterializationCounts(
        admitted_count=admitted_count,
        blocked_count=blocked_count,
        risk_budget_count=risk_budget_count,
        family_snapshot_count=family_snapshot_count,
        entry_leg_count=entry_leg_count,
        exit_plan_count=exit_plan_count,
        exit_leg_count=exit_leg_count,
    )


def _insert_position_candidate_audit(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    signal: PositionFormalSignalInput,
    policy_id: str,
    candidate_status: str,
    blocked_reason_code: str | None,
    context_contract: PositionContextContract,
    candidate_contract_version: str,
) -> None:
    _insert_once(
        connection,
        table_name="position_candidate_audit",
        key_column="candidate_nk",
        key_value=candidate_nk,
        columns=(
            "candidate_nk", "signal_nk", "asset_type", "instrument", "code",
            "policy_id", "reference_trade_date", "candidate_status", "blocked_reason_code",
            "context_code", "context_behavior_profile", "deployment_stage",
            "candidate_contract_version", "audit_note", "source_signal_run_id",
        ),
        values=(
            candidate_nk, signal.signal_nk, "stock", signal.instrument, signal.instrument,
            policy_id, signal.reference_trade_date, candidate_status, blocked_reason_code,
            signal.malf_context_4, context_contract.context_behavior_profile,
            context_contract.deployment_stage, candidate_contract_version,
            signal.audit_note, signal.source_signal_run_id,
        ),
    )


def _insert_position_risk_budget_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    policy_id: str,
    policy_contract: PositionPolicyContract,
    signal: PositionFormalSignalInput,
    context_contract: PositionContextContract,
    risk_budget_snapshot: PositionRiskBudgetSnapshot,
) -> None:
    _insert_once(
        connection,
        table_name="position_risk_budget_snapshot",
        key_column="risk_budget_snapshot_nk",
        key_value=risk_budget_snapshot.risk_budget_snapshot_nk,
        columns=(
            "risk_budget_snapshot_nk", "candidate_nk", "policy_id", "risk_budget_snapshot_role",
            "current_position_weight", "context_behavior_profile", "deployment_stage",
            "risk_budget_weight", "risk_budget_reason_code", "context_cap_weight",
            "context_cap_reason_code", "single_name_cap_weight", "single_name_cap_reason_code",
            "portfolio_cap_weight", "portfolio_cap_reason_code",
            "remaining_single_name_capacity_weight", "remaining_portfolio_capacity_weight",
            "final_allowed_position_weight", "required_reduction_weight", "binding_cap_code",
            "capacity_source_code", "context_weight_rule_code", "source_policy_family",
            "source_policy_version", "source_signal_contract_version",
            "source_context_fingerprint", "risk_budget_contract_version",
        ),
        values=(
            risk_budget_snapshot.risk_budget_snapshot_nk, candidate_nk, policy_id, "default",
            signal.current_position_weight, context_contract.context_behavior_profile,
            context_contract.deployment_stage, risk_budget_snapshot.risk_budget_weight,
            risk_budget_snapshot.risk_budget_reason_code, risk_budget_snapshot.context_cap_weight,
            risk_budget_snapshot.context_cap_reason_code, risk_budget_snapshot.single_name_cap_weight,
            risk_budget_snapshot.single_name_cap_reason_code, risk_budget_snapshot.portfolio_cap_weight,
            risk_budget_snapshot.portfolio_cap_reason_code,
            risk_budget_snapshot.remaining_single_name_capacity_weight,
            risk_budget_snapshot.remaining_portfolio_capacity_weight,
            risk_budget_snapshot.final_allowed_position_weight,
            risk_budget_snapshot.required_reduction_weight, risk_budget_snapshot.binding_cap_code,
            risk_budget_snapshot.capacity_source_code, context_contract.context_weight_rule_code,
            policy_contract.policy_family, policy_contract.policy_version,
            signal.signal_contract_version, signal.family_source_context_fingerprint,
            policy_contract.position_contract_version,
        ),
    )


def _insert_position_capacity_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    current_position_weight: float,
    context_contract: PositionContextContract,
    risk_budget_snapshot: PositionRiskBudgetSnapshot,
) -> None:
    capacity_snapshot_nk = build_capacity_snapshot_nk(candidate_nk)
    _insert_once(
        connection,
        table_name="position_capacity_snapshot",
        key_column="capacity_snapshot_nk",
        key_value=capacity_snapshot_nk,
        columns=(
            "capacity_snapshot_nk", "candidate_nk", "risk_budget_snapshot_nk", "capacity_snapshot_role",
            "current_position_weight", "context_behavior_profile", "deployment_stage",
            "risk_budget_weight", "context_max_position_weight", "single_name_cap_weight",
            "portfolio_cap_weight", "remaining_single_name_capacity_weight",
            "remaining_portfolio_capacity_weight", "final_allowed_position_weight",
            "required_reduction_weight", "binding_cap_code", "context_weight_rule_code",
            "capacity_source_code",
        ),
        values=(
            capacity_snapshot_nk, candidate_nk, risk_budget_snapshot.risk_budget_snapshot_nk, "default",
            current_position_weight, context_contract.context_behavior_profile,
            context_contract.deployment_stage, risk_budget_snapshot.risk_budget_weight,
            risk_budget_snapshot.context_cap_weight, risk_budget_snapshot.single_name_cap_weight,
            risk_budget_snapshot.portfolio_cap_weight,
            risk_budget_snapshot.remaining_single_name_capacity_weight,
            risk_budget_snapshot.remaining_portfolio_capacity_weight,
            risk_budget_snapshot.final_allowed_position_weight,
            risk_budget_snapshot.required_reduction_weight, risk_budget_snapshot.binding_cap_code,
            context_contract.context_weight_rule_code, risk_budget_snapshot.capacity_source_code,
        ),
    )


def _insert_position_sizing_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    risk_budget_snapshot_nk: str,
    policy_id: str,
    policy_contract: PositionPolicyContract,
    sizing_leg_role: str,
    signal: PositionFormalSignalInput,
    context_contract: PositionContextContract,
    position_action_decision: str,
    sizing_schedule_stage: str,
    sizing_schedule_lag_days: int,
    entry_leg_count: int,
    exit_plan_required: bool,
    target_weight: float,
    target_notional: float,
    target_shares: int,
    final_allowed_position_weight: float,
    required_reduction_weight: float,
) -> None:
    sizing_snapshot_nk = build_sizing_snapshot_nk(
        candidate_nk,
        sizing_leg_role=sizing_leg_role,
        contract_version=policy_contract.position_contract_version,
    )
    _insert_once(
        connection,
        table_name="position_sizing_snapshot",
        key_column="sizing_snapshot_nk",
        key_value=sizing_snapshot_nk,
        columns=(
            "sizing_snapshot_nk", "candidate_nk", "risk_budget_snapshot_nk", "policy_id",
            "entry_leg_role", "context_behavior_profile", "deployment_stage",
            "schedule_stage", "schedule_lag_days", "sizing_contract_version",
            "entry_leg_count", "exit_plan_required", "position_action_decision",
            "target_weight", "target_notional", "target_shares",
            "final_allowed_position_weight", "required_reduction_weight",
            "reference_price", "reference_trade_date",
        ),
        values=(
            sizing_snapshot_nk, candidate_nk, risk_budget_snapshot_nk, policy_id,
            sizing_leg_role, context_contract.context_behavior_profile,
            context_contract.deployment_stage, sizing_schedule_stage, sizing_schedule_lag_days,
            policy_contract.position_contract_version, entry_leg_count, exit_plan_required,
            position_action_decision, target_weight, target_notional, target_shares,
            final_allowed_position_weight, required_reduction_weight,
            signal.reference_price, signal.reference_trade_date,
        ),
    )


def _insert_position_entry_leg_plan(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    policy_id: str,
    policy_contract: PositionPolicyContract,
    context_contract: PositionContextContract,
    entry_leg_plan: PositionEntryLegPlan,
) -> None:
    _insert_once(
        connection,
        table_name="position_entry_leg_plan",
        key_column="entry_leg_nk",
        key_value=entry_leg_plan.entry_leg_nk,
        columns=(
            "entry_leg_nk", "candidate_nk", "policy_id", "leg_role", "leg_status",
            "schedule_stage", "schedule_lag_days", "leg_gate_reason",
            "target_weight_after_leg", "target_notional_after_leg", "target_shares_after_leg",
            "context_behavior_profile", "deployment_stage", "plan_contract_version",
        ),
        values=(
            entry_leg_plan.entry_leg_nk, candidate_nk, policy_id, entry_leg_plan.leg_role,
            entry_leg_plan.leg_status, entry_leg_plan.schedule_stage,
            entry_leg_plan.schedule_lag_days, entry_leg_plan.leg_gate_reason,
            entry_leg_plan.target_weight_after_leg, entry_leg_plan.target_notional_after_leg,
            entry_leg_plan.target_shares_after_leg, context_contract.context_behavior_profile,
            context_contract.deployment_stage, policy_contract.position_contract_version,
        ),
    )


def _insert_policy_family_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    policy_contract: PositionPolicyContract,
    policy_id: str,
    candidate_nk: str,
    signal: PositionFormalSignalInput,
    share_lot_size: int,
    risk_budget_weight: float,
    final_allowed_position_weight: float,
    target_shares_before_lot: int,
    final_target_shares: int,
) -> int:
    family_snapshot_nk = build_family_snapshot_nk(
        candidate_nk,
        policy_family=policy_contract.policy_family,
        policy_version=policy_contract.policy_version,
    )
    if policy_contract.policy_family == "FIXED_NOTIONAL_CONTROL":
        target_notional_before_cap = resolve_target_notional(
            signal,
            target_weight=risk_budget_weight,
        )
        target_shares_before_cap = resolve_target_shares_before_lot(
            signal,
            target_notional=target_notional_before_cap,
        )
        _insert_once(
            connection,
            table_name="position_funding_fixed_notional_snapshot",
            key_column="family_snapshot_nk",
            key_value=family_snapshot_nk,
            columns=(
                "family_snapshot_nk", "candidate_nk", "policy_id", "target_notional_before_cap",
                "target_shares_before_cap", "cap_trim_applied", "final_target_shares",
            ),
            values=(
                family_snapshot_nk, candidate_nk, policy_id, target_notional_before_cap,
                target_shares_before_cap, final_allowed_position_weight < risk_budget_weight,
                final_target_shares,
            ),
        )
        return 1
    if policy_contract.policy_family == "SINGLE_LOT_CONTROL":
        lot_floor_applied = target_shares_before_lot != final_target_shares
        fallback_reason_code = None
        if final_allowed_position_weight > 0 and final_target_shares == 0:
            fallback_reason_code = "insufficient_notional_for_single_lot"
        _insert_once(
            connection,
            table_name="position_funding_single_lot_snapshot",
            key_column="family_snapshot_nk",
            key_value=family_snapshot_nk,
            columns=(
                "family_snapshot_nk", "candidate_nk", "policy_id", "min_lot_size",
                "lot_floor_applied", "final_target_shares", "fallback_reason_code",
            ),
            values=(
                family_snapshot_nk, candidate_nk, policy_id, share_lot_size,
                lot_floor_applied, final_target_shares, fallback_reason_code,
            ),
        )
        return 1
    return 0


def _insert_position_exit_plan(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    policy_id: str,
    policy_contract: PositionPolicyContract,
    exit_plan: PositionExitPlan,
) -> None:
    _insert_once(
        connection,
        table_name="position_exit_plan",
        key_column="exit_plan_nk",
        key_value=exit_plan.exit_plan_nk,
        columns=(
            "exit_plan_nk", "position_nk", "candidate_nk", "policy_id", "exit_family",
            "plan_role", "exit_status", "schedule_stage", "schedule_lag_days",
            "planned_leg_count", "required_reduction_weight", "target_weight_after_exit",
            "plan_contract_version", "hard_close_guard_active",
        ),
        values=(
            exit_plan.exit_plan_nk, candidate_nk, candidate_nk, policy_id,
            policy_contract.exit_family, exit_plan.plan_role, exit_plan.exit_status,
            exit_plan.schedule_stage, exit_plan.schedule_lag_days, exit_plan.planned_leg_count,
            exit_plan.required_reduction_weight, exit_plan.target_weight_after_exit,
            policy_contract.position_contract_version, exit_plan.hard_close_guard_active,
        ),
    )


def _insert_position_exit_leg(
    connection: duckdb.DuckDBPyConnection,
    *,
    policy_contract: PositionPolicyContract,
    exit_plan_nk: str,
    exit_leg: PositionExitLeg,
) -> None:
    _insert_once(
        connection,
        table_name="position_exit_leg",
        key_column="exit_leg_nk",
        key_value=exit_leg.exit_leg_nk,
        columns=(
            "exit_leg_nk", "exit_plan_nk", "exit_leg_seq", "leg_role", "schedule_stage",
            "schedule_lag_days", "leg_gate_reason", "exit_reason_code", "target_weight_after_leg",
            "target_qty_after", "is_partial_exit", "fallback_to_full_exit",
            "plan_contract_version",
        ),
        values=(
            exit_leg.exit_leg_nk, exit_plan_nk, exit_leg.exit_leg_seq, exit_leg.leg_role,
            exit_leg.schedule_stage, exit_leg.schedule_lag_days, exit_leg.leg_gate_reason,
            exit_leg.exit_reason_code,
            exit_leg.target_weight_after_leg, exit_leg.target_qty_after,
            exit_leg.is_partial_exit, exit_leg.fallback_to_full_exit,
            policy_contract.position_contract_version,
        ),
    )


def _insert_once(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    key_column: str,
    key_value: str,
    columns: tuple[str, ...],
    values: tuple[object, ...],
) -> None:
    placeholders = ", ".join("?" for _ in columns)
    column_sql = ",\n            ".join(columns)
    connection.execute(
        f"""
        INSERT INTO {table_name} (
            {column_sql}
        )
        SELECT {placeholders}
        WHERE NOT EXISTS (
            SELECT 1
            FROM {table_name}
            WHERE {key_column} = ?
        )
        """,
        [*values, key_value],
    )
