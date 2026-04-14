"""承载 `position` 的 MALF context 合同映射、分批计划与退出计划纯逻辑。"""

from __future__ import annotations

from dataclasses import dataclass

from .position_shared import PositionFormalSignalInput


_PROFILE_BY_CONTEXT: dict[str, str] = {
    "BULL_MAINSTREAM": "trend_following_expansion",
    "BULL_COUNTERTREND": "pullback_probe_then_confirm",
    "BEAR_COUNTERTREND": "countertrend_probe_only",
    "BEAR_MAINSTREAM": "defensive_exit_only",
}

_DEPLOYMENT_STAGE_ORDER: dict[str, int] = {
    "blocked_no_lifecycle": 0,
    "initial_entry_window": 1,
    "confirmation_window": 2,
    "continuation_window": 3,
    "late_cycle_window": 4,
    "exit_only_window": 5,
}

_PROFILE_STAGE_WEIGHT: dict[tuple[str, str], float] = {
    ("trend_following_expansion", "initial_entry_window"): 0.1875,
    ("trend_following_expansion", "confirmation_window"): 0.1250,
    ("trend_following_expansion", "continuation_window"): 0.0625,
    ("trend_following_expansion", "late_cycle_window"): 0.0,
    ("pullback_probe_then_confirm", "initial_entry_window"): 0.0625,
    ("pullback_probe_then_confirm", "confirmation_window"): 0.1250,
    ("pullback_probe_then_confirm", "continuation_window"): 0.1875,
    ("pullback_probe_then_confirm", "late_cycle_window"): 0.25,
    ("countertrend_probe_only", "initial_entry_window"): 0.09375,
    ("countertrend_probe_only", "confirmation_window"): 0.0625,
    ("countertrend_probe_only", "continuation_window"): 0.03125,
    ("countertrend_probe_only", "late_cycle_window"): 0.0,
    ("defensive_exit_only", "exit_only_window"): 0.0,
}

_ENTRY_LEG_BLUEPRINTS: dict[str, tuple[tuple[str, str, int, float], ...]] = {
    "trend_following_expansion": (
        ("initial_entry", "t+1", 1, 0.50),
        ("add_on_confirmation", "t+2", 2, 0.80),
        ("add_on_continuation", "t+3", 3, 1.00),
    ),
    "pullback_probe_then_confirm": (
        ("initial_entry", "t+1", 1, 0.35),
        ("add_on_confirmation", "t+2", 2, 0.70),
        ("add_on_continuation", "t+3", 3, 1.00),
    ),
    "countertrend_probe_only": (
        ("initial_entry", "t+1", 1, 1.00),
        ("add_on_confirmation", "t+2", 2, 1.00),
        ("add_on_continuation", "t+3", 3, 1.00),
    ),
    "defensive_exit_only": (
        ("initial_entry", "t+1", 1, 0.0),
        ("add_on_confirmation", "t+2", 2, 0.0),
        ("add_on_continuation", "t+3", 3, 0.0),
    ),
}

_ENTRY_LEG_STAGE_REQUIREMENT: dict[str, int] = {
    "initial_entry": _DEPLOYMENT_STAGE_ORDER["initial_entry_window"],
    "add_on_confirmation": _DEPLOYMENT_STAGE_ORDER["confirmation_window"],
    "add_on_continuation": _DEPLOYMENT_STAGE_ORDER["continuation_window"],
}


@dataclass(frozen=True)
class PositionPolicyContract:
    """描述 `position_policy_registry` 中一条被激活的正式契约。"""

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


@dataclass(frozen=True)
class PositionContextContract:
    """描述 `malf context + lifecycle` 冻结后的正式中间语义。"""

    context_behavior_profile: str
    deployment_stage: str
    context_weight_rule_code: str
    context_max_position_weight: float


@dataclass(frozen=True)
class PositionRiskBudgetSnapshot:
    """描述 `48` 冻结后的 risk budget / capacity 分层快照。"""

    risk_budget_snapshot_nk: str
    risk_budget_weight: float
    risk_budget_reason_code: str
    context_cap_weight: float
    context_cap_reason_code: str
    single_name_cap_weight: float
    single_name_cap_reason_code: str
    portfolio_cap_weight: float
    portfolio_cap_reason_code: str
    remaining_single_name_capacity_weight: float
    remaining_portfolio_capacity_weight: float
    final_allowed_position_weight: float
    required_reduction_weight: float
    binding_cap_code: str
    capacity_source_code: str


@dataclass(frozen=True)
class PositionEntryLegPlan:
    """描述一条正式入场计划腿。"""

    entry_leg_nk: str
    leg_role: str
    leg_status: str
    schedule_stage: str
    schedule_lag_days: int
    leg_gate_reason: str | None
    target_weight_after_leg: float
    target_notional_after_leg: float
    target_shares_after_leg: int


@dataclass(frozen=True)
class PositionExitPlan:
    """描述一条正式退出计划头。"""

    exit_plan_nk: str
    plan_role: str
    exit_status: str
    schedule_stage: str
    schedule_lag_days: int
    planned_leg_count: int
    required_reduction_weight: float
    target_weight_after_exit: float
    hard_close_guard_active: bool


@dataclass(frozen=True)
class PositionExitLeg:
    """描述一条正式退出计划腿。"""

    exit_leg_nk: str
    exit_leg_seq: int
    leg_role: str
    schedule_stage: str
    schedule_lag_days: int
    exit_reason_code: str
    target_weight_after_leg: float
    target_qty_after: int
    is_partial_exit: bool
    fallback_to_full_exit: bool


def resolve_candidate_status(signal: PositionFormalSignalInput) -> str:
    if signal.trigger_admissible and signal.formal_signal_status == "admitted":
        return "admitted"
    if signal.formal_signal_status in {"blocked", "deferred"}:
        return signal.formal_signal_status
    return "blocked"


def resolve_blocked_reason_code(signal: PositionFormalSignalInput, candidate_status: str) -> str | None:
    if candidate_status == "admitted":
        return None
    if signal.blocked_reason_code:
        return signal.blocked_reason_code
    if signal.filter_reject_reason_code:
        return signal.filter_reject_reason_code
    if not signal.trigger_admissible:
        return "alpha_not_admitted"
    return f"alpha_status_{candidate_status}"


def build_candidate_nk(signal: PositionFormalSignalInput, policy_id: str) -> str:
    return "|".join((signal.signal_nk, policy_id, signal.reference_trade_date))


def build_capacity_snapshot_nk(candidate_nk: str) -> str:
    return f"{candidate_nk}|default"


def build_risk_budget_snapshot_nk(candidate_nk: str) -> str:
    return f"{candidate_nk}|default"


def build_sizing_snapshot_nk(
    candidate_nk: str,
    *,
    sizing_leg_role: str,
    policy_version: str,
) -> str:
    return f"{candidate_nk}|{sizing_leg_role}|{policy_version}"


def build_entry_leg_nk(
    candidate_nk: str,
    *,
    leg_role: str,
    schedule_stage: str,
    contract_version: str,
) -> str:
    return f"{candidate_nk}|{leg_role}|{schedule_stage}|{contract_version}"


def build_family_snapshot_nk(candidate_nk: str, *, policy_family: str, policy_version: str) -> str:
    return f"{candidate_nk}|{policy_family}|{policy_version}"


def build_exit_plan_nk(
    candidate_nk: str,
    *,
    plan_role: str,
    schedule_stage: str,
    contract_version: str,
) -> str:
    return f"{candidate_nk}|{plan_role}|{schedule_stage}|{contract_version}"


def build_exit_leg_nk(exit_plan_nk: str, *, exit_leg_seq: int) -> str:
    return f"{exit_plan_nk}|{exit_leg_seq}"


def resolve_context_contract(signal: PositionFormalSignalInput) -> PositionContextContract:
    context_behavior_profile = _PROFILE_BY_CONTEXT.get(
        signal.malf_context_4,
        "defensive_exit_only",
    )
    deployment_stage = resolve_deployment_stage(
        context_behavior_profile=context_behavior_profile,
        lifecycle_rank_high=signal.lifecycle_rank_high,
        lifecycle_rank_total=signal.lifecycle_rank_total,
    )
    context_weight_rule_code = f"{signal.malf_context_4.lower()}::{deployment_stage}::v1"
    return PositionContextContract(
        context_behavior_profile=context_behavior_profile,
        deployment_stage=deployment_stage,
        context_weight_rule_code=context_weight_rule_code,
        context_max_position_weight=resolve_context_max_position_weight(
            context_behavior_profile=context_behavior_profile,
            deployment_stage=deployment_stage,
        ),
    )


def resolve_deployment_stage(
    *,
    context_behavior_profile: str,
    lifecycle_rank_high: int,
    lifecycle_rank_total: int,
) -> str:
    if context_behavior_profile == "defensive_exit_only":
        return "exit_only_window"
    if lifecycle_rank_total <= 0:
        return "blocked_no_lifecycle"
    normalized_ratio = max(min(lifecycle_rank_high / lifecycle_rank_total, 1.0), 0.0)
    if normalized_ratio <= 0.25:
        return "initial_entry_window"
    if normalized_ratio <= 0.50:
        return "confirmation_window"
    if normalized_ratio <= 0.75:
        return "continuation_window"
    return "late_cycle_window"


def resolve_context_max_position_weight(
    *,
    context_behavior_profile: str,
    deployment_stage: str,
) -> float:
    return _PROFILE_STAGE_WEIGHT.get((context_behavior_profile, deployment_stage), 0.0)


def resolve_risk_budget_weight(*, default_single_name_cap_weight: float) -> float:
    return max(default_single_name_cap_weight, 0.0)


def resolve_single_name_capacity_weight(
    signal: PositionFormalSignalInput,
    *,
    default_single_name_cap_weight: float,
) -> float:
    if signal.remaining_single_name_capacity_weight is not None:
        return max(signal.remaining_single_name_capacity_weight, 0.0)
    return max(default_single_name_cap_weight - signal.current_position_weight, 0.0)


def resolve_portfolio_capacity_weight(
    signal: PositionFormalSignalInput,
    *,
    default_portfolio_cap_weight: float,
) -> float:
    if signal.remaining_portfolio_capacity_weight is not None:
        return max(signal.remaining_portfolio_capacity_weight, 0.0)
    return max(default_portfolio_cap_weight, 0.0)


def resolve_final_allowed_position_weight(
    *,
    candidate_status: str,
    risk_budget_weight: float,
    context_max_position_weight: float,
    single_name_cap_weight: float,
    portfolio_cap_weight: float,
) -> float:
    if candidate_status != "admitted":
        return 0.0
    return max(
        min(
            risk_budget_weight,
            context_max_position_weight,
            single_name_cap_weight,
            portfolio_cap_weight,
        ),
        0.0,
    )


def resolve_binding_cap_code(
    *,
    candidate_status: str,
    blocked_reason_code: str | None,
    risk_budget_weight: float,
    context_max_position_weight: float,
    single_name_cap_weight: float,
    portfolio_cap_weight: float,
    final_allowed_position_weight: float,
) -> str:
    if candidate_status != "admitted":
        return blocked_reason_code or f"candidate_{candidate_status}"
    cap_candidates = (
        ("risk_budget_cap", risk_budget_weight),
        ("context_cap", context_max_position_weight),
        ("single_name_cap", single_name_cap_weight),
        ("portfolio_cap", portfolio_cap_weight),
    )
    for binding_cap_code, cap_weight in cap_candidates:
        if abs(cap_weight - final_allowed_position_weight) < 1e-12:
            return binding_cap_code
    return "no_binding_cap"


def resolve_capacity_source_code(signal: PositionFormalSignalInput) -> str:
    if (
        signal.remaining_single_name_capacity_weight is not None
        or signal.remaining_portfolio_capacity_weight is not None
    ):
        return "formal_position_capacity"
    return "bootstrap_default_capacity"


def resolve_position_action_decision(
    *,
    signal: PositionFormalSignalInput,
    candidate_status: str,
    final_allowed_position_weight: float,
    required_reduction_weight: float,
) -> str:
    if candidate_status != "admitted" or final_allowed_position_weight <= 0:
        if signal.current_position_weight > 0:
            return "closeout_by_exit_plan"
        return "reject_open"
    if required_reduction_weight > 0:
        return "trim_to_context_cap"
    if signal.current_position_weight > 0 and abs(signal.current_position_weight - final_allowed_position_weight) < 1e-12:
        return "hold_at_cap"
    return "open_up_to_context_cap"


def resolve_sizing_leg_role(position_action_decision: str, *, entry_leg_role_default: str) -> str:
    if position_action_decision == "trim_to_context_cap":
        return "protective_trim"
    if position_action_decision == "closeout_by_exit_plan":
        return "closeout"
    return entry_leg_role_default


def resolve_sizing_schedule(
    *,
    context_contract: PositionContextContract,
    policy_contract: PositionPolicyContract,
    position_action_decision: str,
) -> tuple[str, int]:
    if position_action_decision == "trim_to_context_cap":
        return (
            policy_contract.trim_schedule_stage_default,
            policy_contract.trim_schedule_lag_days_default,
        )
    if position_action_decision == "closeout_by_exit_plan":
        return (
            policy_contract.exit_schedule_stage_default,
            policy_contract.exit_schedule_lag_days_default,
        )
    if context_contract.deployment_stage == "confirmation_window":
        return ("t+2", 2)
    if context_contract.deployment_stage == "continuation_window":
        return ("t+3", 3)
    if context_contract.deployment_stage == "late_cycle_window":
        return ("t+3", 3)
    return (
        policy_contract.entry_schedule_stage_default,
        policy_contract.entry_schedule_lag_days_default,
    )


def resolve_target_weight(
    *,
    position_action_decision: str,
    final_allowed_position_weight: float,
) -> float:
    if position_action_decision in {"reject_open", "closeout_by_exit_plan"}:
        return 0.0
    return max(final_allowed_position_weight, 0.0)


def resolve_target_notional(signal: PositionFormalSignalInput, *, target_weight: float) -> float:
    if signal.capital_base_value is None:
        return 0.0
    return max(target_weight, 0.0) * signal.capital_base_value


def resolve_target_shares_before_lot(
    signal: PositionFormalSignalInput,
    *,
    target_notional: float,
) -> int:
    if signal.reference_price is None or signal.reference_price <= 0:
        return 0
    return int(target_notional / signal.reference_price)


def apply_share_lot_floor(target_shares_before_lot: int, *, share_lot_size: int) -> int:
    if target_shares_before_lot <= 0:
        return 0
    return (target_shares_before_lot // share_lot_size) * share_lot_size


def build_entry_leg_plans(
    *,
    signal: PositionFormalSignalInput,
    candidate_nk: str,
    policy_contract: PositionPolicyContract,
    context_contract: PositionContextContract,
    candidate_status: str,
    position_action_decision: str,
    final_allowed_position_weight: float,
    share_lot_size: int,
) -> tuple[PositionEntryLegPlan, ...]:
    blueprints = _ENTRY_LEG_BLUEPRINTS[context_contract.context_behavior_profile]
    active_stage_order = _DEPLOYMENT_STAGE_ORDER[context_contract.deployment_stage]
    plans: list[PositionEntryLegPlan] = []
    for leg_role, schedule_stage, schedule_lag_days, cumulative_ratio in blueprints:
        required_stage_order = _ENTRY_LEG_STAGE_REQUIREMENT[leg_role]
        leg_status, leg_gate_reason = _resolve_entry_leg_status(
            context_behavior_profile=context_contract.context_behavior_profile,
            candidate_status=candidate_status,
            position_action_decision=position_action_decision,
            active_stage_order=active_stage_order,
            required_stage_order=required_stage_order,
            leg_role=leg_role,
        )
        target_weight_after_leg = max(final_allowed_position_weight * cumulative_ratio, 0.0)
        if leg_status == "blocked":
            target_weight_after_leg = 0.0
        target_notional_after_leg = resolve_target_notional(
            signal,
            target_weight=target_weight_after_leg,
        )
        target_shares_after_leg = apply_share_lot_floor(
            resolve_target_shares_before_lot(
                signal,
                target_notional=target_notional_after_leg,
            ),
            share_lot_size=share_lot_size,
        )
        plans.append(
            PositionEntryLegPlan(
                entry_leg_nk=build_entry_leg_nk(
                    candidate_nk,
                    leg_role=leg_role,
                    schedule_stage=schedule_stage,
                    contract_version=policy_contract.position_contract_version,
                ),
                leg_role=leg_role,
                leg_status=leg_status,
                schedule_stage=schedule_stage,
                schedule_lag_days=schedule_lag_days,
                leg_gate_reason=leg_gate_reason,
                target_weight_after_leg=target_weight_after_leg,
                target_notional_after_leg=target_notional_after_leg,
                target_shares_after_leg=target_shares_after_leg,
            )
        )
    return tuple(plans)


def build_exit_plan(
    *,
    signal: PositionFormalSignalInput,
    candidate_nk: str,
    policy_contract: PositionPolicyContract,
    position_action_decision: str,
    blocked_reason_code: str | None,
    final_allowed_position_weight: float,
    required_reduction_weight: float,
    target_shares: int,
) -> tuple[PositionExitPlan | None, tuple[PositionExitLeg, ...]]:
    if position_action_decision == "trim_to_context_cap":
        exit_plan = PositionExitPlan(
            exit_plan_nk=build_exit_plan_nk(
                candidate_nk,
                plan_role="trim",
                schedule_stage=policy_contract.trim_schedule_stage_default,
                contract_version=policy_contract.position_contract_version,
            ),
            plan_role="trim",
            exit_status="planned",
            schedule_stage=policy_contract.trim_schedule_stage_default,
            schedule_lag_days=policy_contract.trim_schedule_lag_days_default,
            planned_leg_count=1,
            required_reduction_weight=required_reduction_weight,
            target_weight_after_exit=final_allowed_position_weight,
            hard_close_guard_active=False,
        )
        exit_leg = PositionExitLeg(
            exit_leg_nk=build_exit_leg_nk(exit_plan.exit_plan_nk, exit_leg_seq=1),
            exit_leg_seq=1,
            leg_role="protective_trim",
            schedule_stage=policy_contract.trim_schedule_stage_default,
            schedule_lag_days=policy_contract.trim_schedule_lag_days_default,
            exit_reason_code="required_reduction_weight_positive",
            target_weight_after_leg=final_allowed_position_weight,
            target_qty_after=target_shares,
            is_partial_exit=True,
            fallback_to_full_exit=False,
        )
        return exit_plan, (exit_leg,)
    if position_action_decision == "closeout_by_exit_plan":
        exit_plan = PositionExitPlan(
            exit_plan_nk=build_exit_plan_nk(
                candidate_nk,
                plan_role="terminal_exit",
                schedule_stage=policy_contract.exit_schedule_stage_default,
                contract_version=policy_contract.position_contract_version,
            ),
            plan_role="terminal_exit",
            exit_status="planned",
            schedule_stage=policy_contract.exit_schedule_stage_default,
            schedule_lag_days=policy_contract.exit_schedule_lag_days_default,
            planned_leg_count=1,
            required_reduction_weight=max(signal.current_position_weight, 0.0),
            target_weight_after_exit=0.0,
            hard_close_guard_active=True,
        )
        exit_leg = PositionExitLeg(
            exit_leg_nk=build_exit_leg_nk(exit_plan.exit_plan_nk, exit_leg_seq=1),
            exit_leg_seq=1,
            leg_role="terminal_exit",
            schedule_stage=policy_contract.exit_schedule_stage_default,
            schedule_lag_days=policy_contract.exit_schedule_lag_days_default,
            exit_reason_code=blocked_reason_code or "context_disallows_new_long",
            target_weight_after_leg=0.0,
            target_qty_after=0,
            is_partial_exit=False,
            fallback_to_full_exit=True,
        )
        return exit_plan, (exit_leg,)
    return None, ()


def _resolve_entry_leg_status(
    *,
    context_behavior_profile: str,
    candidate_status: str,
    position_action_decision: str,
    active_stage_order: int,
    required_stage_order: int,
    leg_role: str,
) -> tuple[str, str | None]:
    if candidate_status != "admitted":
        return "blocked", f"candidate_{candidate_status}"
    if position_action_decision != "open_up_to_context_cap":
        return "blocked", f"position_action_{position_action_decision}"
    if context_behavior_profile == "countertrend_probe_only" and leg_role != "initial_entry":
        return "blocked", "profile_blocks_add_on"
    if active_stage_order >= required_stage_order:
        return "planned", None
    return "deferred", "await_future_lifecycle_window"
