"""`portfolio_plan` v2 ledger row、自然键、裁决与 upsert helper。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime

import duckdb


DEFAULT_PORTFOLIO_CAPACITY_SCOPE = "portfolio_gross"
DEFAULT_ALLOCATION_SCENE = "trade_ready"
DEFAULT_DECISION_ORDER_CODE = "requested_weight_desc_then_instrument"
DEFAULT_TRADE_READINESS_STATUS = "not_trade_ready"
_TRADE_READY_SCHEDULE_STAGES = frozenset({"", "ready", "same_day", "t+0", "t+1"})


@dataclass(frozen=True)
class _PositionBridgeRow:
    candidate_nk: str
    instrument: str
    policy_id: str
    reference_trade_date: date
    candidate_status: str
    blocked_reason_code: str | None
    position_action_decision: str
    schedule_stage: str
    schedule_lag_days: int
    final_allowed_position_weight: float
    required_reduction_weight: float
    remaining_single_name_capacity_weight: float
    remaining_portfolio_capacity_weight: float
    binding_cap_code: str
    capacity_source_code: str


@dataclass(frozen=True)
class _PortfolioCandidateDecisionRow:
    candidate_decision_nk: str
    candidate_nk: str
    portfolio_id: str
    instrument: str
    policy_id: str
    reference_trade_date: date
    position_action_decision: str
    decision_status: str
    decision_reason_code: str
    blocking_reason_code: str | None
    decision_rank: int
    decision_order_code: str
    source_candidate_status: str
    source_blocked_reason_code: str | None
    source_binding_cap_code: str
    source_capacity_source_code: str
    source_required_reduction_weight: float
    source_remaining_single_name_capacity_weight: float
    source_remaining_portfolio_capacity_weight: float
    capacity_before_weight: float
    capacity_after_weight: float
    trade_readiness_status: str
    schedule_stage: str
    schedule_lag_days: int
    requested_weight: float
    admitted_weight: float
    trimmed_weight: float
    capacity_snapshot_nk: str
    portfolio_plan_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


@dataclass(frozen=True)
class _PortfolioCapacitySnapshotRow:
    capacity_snapshot_nk: str
    portfolio_id: str
    capacity_scope: str
    reference_trade_date: date
    portfolio_gross_cap_weight: float
    portfolio_gross_used_weight: float
    portfolio_gross_remaining_weight: float
    requested_candidate_count: int
    admitted_candidate_count: int
    blocked_candidate_count: int
    trimmed_candidate_count: int
    deferred_candidate_count: int
    requested_total_weight: float
    admitted_total_weight: float
    trimmed_total_weight: float
    blocked_total_weight: float
    deferred_total_weight: float
    binding_constraint_code: str
    capacity_decision_reason_code: str
    capacity_reason_summary_json: str | None
    portfolio_plan_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


@dataclass(frozen=True)
class _PortfolioAllocationSnapshotRow:
    allocation_snapshot_nk: str
    candidate_nk: str
    portfolio_id: str
    instrument: str
    allocation_scene: str
    reference_trade_date: date
    requested_weight: float
    admitted_weight: float
    trimmed_weight: float
    final_allocated_weight: float
    plan_status: str
    decision_reason_code: str
    blocking_reason_code: str | None
    decision_rank: int
    decision_order_code: str
    trade_readiness_status: str
    schedule_stage: str
    schedule_lag_days: int
    source_binding_cap_code: str
    candidate_decision_nk: str
    capacity_snapshot_nk: str
    portfolio_plan_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


@dataclass(frozen=True)
class _PortfolioPlanSnapshotRow:
    plan_snapshot_nk: str
    candidate_nk: str
    portfolio_id: str
    instrument: str
    reference_trade_date: date
    position_action_decision: str
    requested_weight: float
    admitted_weight: float
    trimmed_weight: float
    plan_status: str
    decision_reason_code: str
    blocking_reason_code: str | None
    decision_rank: int
    decision_order_code: str
    trade_readiness_status: str
    schedule_stage: str
    schedule_lag_days: int
    source_binding_cap_code: str
    portfolio_gross_cap_weight: float
    portfolio_gross_used_weight: float
    portfolio_gross_remaining_weight: float
    candidate_decision_nk: str
    capacity_snapshot_nk: str
    allocation_snapshot_nk: str
    portfolio_plan_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


@dataclass(frozen=True)
class _DecisionEvaluation:
    decision_status: str
    decision_reason_code: str
    blocking_reason_code: str | None
    requested_weight: float
    admitted_weight: float
    trimmed_weight: float
    capacity_before_weight: float
    capacity_after_weight: float
    next_used_weight: float
    next_remaining_weight: float
    trade_readiness_status: str
    decision_order_code: str


@dataclass(frozen=True)
class _CandidateLedgerBundle:
    decision_row: _PortfolioCandidateDecisionRow
    allocation_row: _PortfolioAllocationSnapshotRow
    snapshot_row: _PortfolioPlanSnapshotRow


def _normalize_schedule_stage(schedule_stage: str) -> str:
    return schedule_stage.strip().lower()


def _is_trade_ready_schedule(*, schedule_stage: str, schedule_lag_days: int) -> bool:
    normalized_stage = _normalize_schedule_stage(schedule_stage)
    return normalized_stage in _TRADE_READY_SCHEDULE_STAGES and schedule_lag_days <= 1


def _evaluate_candidate_decision(
    *,
    bridge_row: _PositionBridgeRow,
    portfolio_gross_cap_weight: float,
    portfolio_gross_used_weight: float,
    portfolio_gross_remaining_weight: float,
) -> _DecisionEvaluation:
    requested_weight = max(float(bridge_row.final_allowed_position_weight), 0.0)
    capacity_before_weight = max(float(portfolio_gross_remaining_weight), 0.0)
    blocking_reason_code: str | None = None
    admitted_weight = 0.0
    trimmed_weight = 0.0
    trade_readiness_status = DEFAULT_TRADE_READINESS_STATUS
    source_status = bridge_row.candidate_status or "blocked"
    schedule_trade_ready = _is_trade_ready_schedule(
        schedule_stage=bridge_row.schedule_stage,
        schedule_lag_days=bridge_row.schedule_lag_days,
    )

    if source_status == "deferred":
        decision_status = "deferred"
        decision_reason_code = (
            bridge_row.blocked_reason_code or "position_candidate_deferred"
        )
        trade_readiness_status = "await_schedule"
    elif source_status != "admitted":
        decision_status = "blocked"
        decision_reason_code = (
            bridge_row.blocked_reason_code
            or f"position_candidate_{source_status or 'blocked'}"
        )
        blocking_reason_code = decision_reason_code
        trade_readiness_status = "blocked"
    elif requested_weight <= 0:
        decision_status = "blocked"
        decision_reason_code = "position_weight_not_positive"
        blocking_reason_code = decision_reason_code
        trade_readiness_status = "blocked"
    elif bridge_row.position_action_decision != "open_up_to_context_cap":
        decision_status = "blocked"
        decision_reason_code = (
            f"position_action_{bridge_row.position_action_decision or 'unknown'}"
        )
        blocking_reason_code = decision_reason_code
        trade_readiness_status = "blocked"
    elif not schedule_trade_ready:
        decision_status = "deferred"
        decision_reason_code = "await_future_schedule_stage"
        trade_readiness_status = "await_schedule"
    elif portfolio_gross_remaining_weight >= requested_weight:
        decision_status = "admitted"
        decision_reason_code = "admitted_without_trim"
        admitted_weight = requested_weight
        trade_readiness_status = "trade_ready"
    elif portfolio_gross_remaining_weight > 0:
        decision_status = "trimmed"
        decision_reason_code = "trimmed_by_portfolio_capacity"
        admitted_weight = portfolio_gross_remaining_weight
        trimmed_weight = max(requested_weight - admitted_weight, 0.0)
        trade_readiness_status = "trade_ready"
    else:
        decision_status = "blocked"
        decision_reason_code = "portfolio_capacity_exhausted"
        blocking_reason_code = decision_reason_code
        trade_readiness_status = "blocked"

    next_used_weight = min(
        portfolio_gross_used_weight + admitted_weight,
        portfolio_gross_cap_weight,
    )
    next_remaining_weight = max(portfolio_gross_cap_weight - next_used_weight, 0.0)
    return _DecisionEvaluation(
        decision_status=decision_status,
        decision_reason_code=decision_reason_code,
        blocking_reason_code=blocking_reason_code,
        requested_weight=requested_weight,
        admitted_weight=admitted_weight,
        trimmed_weight=trimmed_weight,
        capacity_before_weight=capacity_before_weight,
        capacity_after_weight=next_remaining_weight,
        next_used_weight=next_used_weight,
        next_remaining_weight=next_remaining_weight,
        trade_readiness_status=trade_readiness_status,
        decision_order_code=DEFAULT_DECISION_ORDER_CODE,
    )


def _build_candidate_bundle(
    *,
    run_id: str,
    bridge_row: _PositionBridgeRow,
    evaluation: _DecisionEvaluation,
    portfolio_id: str,
    portfolio_plan_contract_version: str,
    capacity_snapshot_nk: str,
    portfolio_gross_cap_weight: float,
    decision_rank: int,
    allocation_scene: str = DEFAULT_ALLOCATION_SCENE,
) -> _CandidateLedgerBundle:
    candidate_decision_nk = _build_candidate_decision_nk(
        candidate_nk=bridge_row.candidate_nk,
        portfolio_id=portfolio_id,
        reference_trade_date=bridge_row.reference_trade_date,
        portfolio_plan_contract_version=portfolio_plan_contract_version,
    )
    allocation_snapshot_nk = _build_allocation_snapshot_nk(
        candidate_nk=bridge_row.candidate_nk,
        portfolio_id=portfolio_id,
        allocation_scene=allocation_scene,
        reference_trade_date=bridge_row.reference_trade_date,
        portfolio_plan_contract_version=portfolio_plan_contract_version,
    )
    snapshot_row = _PortfolioPlanSnapshotRow(
        plan_snapshot_nk=_build_plan_snapshot_nk(
            candidate_nk=bridge_row.candidate_nk,
            portfolio_id=portfolio_id,
            reference_trade_date=bridge_row.reference_trade_date,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
        ),
        candidate_nk=bridge_row.candidate_nk,
        portfolio_id=portfolio_id,
        instrument=bridge_row.instrument,
        reference_trade_date=bridge_row.reference_trade_date,
        position_action_decision=bridge_row.position_action_decision,
        requested_weight=evaluation.requested_weight,
        admitted_weight=evaluation.admitted_weight,
        trimmed_weight=evaluation.trimmed_weight,
        plan_status=evaluation.decision_status,
        decision_reason_code=evaluation.decision_reason_code,
        blocking_reason_code=evaluation.blocking_reason_code,
        decision_rank=decision_rank,
        decision_order_code=evaluation.decision_order_code,
        trade_readiness_status=evaluation.trade_readiness_status,
        schedule_stage=bridge_row.schedule_stage,
        schedule_lag_days=bridge_row.schedule_lag_days,
        source_binding_cap_code=bridge_row.binding_cap_code,
        portfolio_gross_cap_weight=portfolio_gross_cap_weight,
        portfolio_gross_used_weight=evaluation.next_used_weight,
        portfolio_gross_remaining_weight=evaluation.next_remaining_weight,
        candidate_decision_nk=candidate_decision_nk,
        capacity_snapshot_nk=capacity_snapshot_nk,
        allocation_snapshot_nk=allocation_snapshot_nk,
        portfolio_plan_contract_version=portfolio_plan_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )
    decision_row = _PortfolioCandidateDecisionRow(
        candidate_decision_nk=candidate_decision_nk,
        candidate_nk=bridge_row.candidate_nk,
        portfolio_id=portfolio_id,
        instrument=bridge_row.instrument,
        policy_id=bridge_row.policy_id,
        reference_trade_date=bridge_row.reference_trade_date,
        position_action_decision=bridge_row.position_action_decision,
        decision_status=evaluation.decision_status,
        decision_reason_code=evaluation.decision_reason_code,
        blocking_reason_code=evaluation.blocking_reason_code,
        decision_rank=decision_rank,
        decision_order_code=evaluation.decision_order_code,
        source_candidate_status=bridge_row.candidate_status,
        source_blocked_reason_code=bridge_row.blocked_reason_code,
        source_binding_cap_code=bridge_row.binding_cap_code,
        source_capacity_source_code=bridge_row.capacity_source_code,
        source_required_reduction_weight=bridge_row.required_reduction_weight,
        source_remaining_single_name_capacity_weight=(
            bridge_row.remaining_single_name_capacity_weight
        ),
        source_remaining_portfolio_capacity_weight=(
            bridge_row.remaining_portfolio_capacity_weight
        ),
        capacity_before_weight=evaluation.capacity_before_weight,
        capacity_after_weight=evaluation.capacity_after_weight,
        trade_readiness_status=evaluation.trade_readiness_status,
        schedule_stage=bridge_row.schedule_stage,
        schedule_lag_days=bridge_row.schedule_lag_days,
        requested_weight=evaluation.requested_weight,
        admitted_weight=evaluation.admitted_weight,
        trimmed_weight=evaluation.trimmed_weight,
        capacity_snapshot_nk=capacity_snapshot_nk,
        portfolio_plan_contract_version=portfolio_plan_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )
    allocation_row = _PortfolioAllocationSnapshotRow(
        allocation_snapshot_nk=allocation_snapshot_nk,
        candidate_nk=bridge_row.candidate_nk,
        portfolio_id=portfolio_id,
        instrument=bridge_row.instrument,
        allocation_scene=allocation_scene,
        reference_trade_date=bridge_row.reference_trade_date,
        requested_weight=evaluation.requested_weight,
        admitted_weight=evaluation.admitted_weight,
        trimmed_weight=evaluation.trimmed_weight,
        final_allocated_weight=evaluation.admitted_weight,
        plan_status=evaluation.decision_status,
        decision_reason_code=evaluation.decision_reason_code,
        blocking_reason_code=evaluation.blocking_reason_code,
        decision_rank=decision_rank,
        decision_order_code=evaluation.decision_order_code,
        trade_readiness_status=evaluation.trade_readiness_status,
        schedule_stage=bridge_row.schedule_stage,
        schedule_lag_days=bridge_row.schedule_lag_days,
        source_binding_cap_code=bridge_row.binding_cap_code,
        candidate_decision_nk=candidate_decision_nk,
        capacity_snapshot_nk=capacity_snapshot_nk,
        portfolio_plan_contract_version=portfolio_plan_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )
    return _CandidateLedgerBundle(
        decision_row=decision_row,
        allocation_row=allocation_row,
        snapshot_row=snapshot_row,
    )


def _build_candidate_decision_nk(
    *,
    candidate_nk: str,
    portfolio_id: str,
    reference_trade_date: date,
    portfolio_plan_contract_version: str,
) -> str:
    return "|".join(
        [
            candidate_nk,
            portfolio_id,
            reference_trade_date.isoformat(),
            portfolio_plan_contract_version,
        ]
    )


def _build_capacity_snapshot_nk(
    *,
    portfolio_id: str,
    capacity_scope: str,
    reference_trade_date: date,
    portfolio_plan_contract_version: str,
) -> str:
    return "|".join(
        [
            portfolio_id,
            capacity_scope,
            reference_trade_date.isoformat(),
            portfolio_plan_contract_version,
        ]
    )


def _build_allocation_snapshot_nk(
    *,
    candidate_nk: str,
    portfolio_id: str,
    allocation_scene: str,
    reference_trade_date: date,
    portfolio_plan_contract_version: str,
) -> str:
    return "|".join(
        [
            candidate_nk,
            portfolio_id,
            allocation_scene,
            reference_trade_date.isoformat(),
            portfolio_plan_contract_version,
        ]
    )


def _build_plan_snapshot_nk(
    *,
    candidate_nk: str,
    portfolio_id: str,
    reference_trade_date: date,
    portfolio_plan_contract_version: str,
) -> str:
    return "|".join(
        [
            candidate_nk,
            portfolio_id,
            reference_trade_date.isoformat(),
            portfolio_plan_contract_version,
        ]
    )


def _decision_sort_key(bridge_row: _PositionBridgeRow) -> tuple[float, str, str]:
    return (
        -max(float(bridge_row.final_allowed_position_weight), 0.0),
        bridge_row.instrument,
        bridge_row.candidate_nk,
    )


def _resolve_binding_constraint_code(
    candidate_bundles: list[_CandidateLedgerBundle],
) -> str:
    for bundle in candidate_bundles:
        if bundle.decision_row.decision_status == "trimmed":
            return "portfolio_gross_cap"
    for bundle in candidate_bundles:
        if bundle.decision_row.decision_reason_code == "portfolio_capacity_exhausted":
            return "portfolio_gross_cap"
    for bundle in candidate_bundles:
        if bundle.decision_row.source_binding_cap_code != "no_binding_cap":
            return bundle.decision_row.source_binding_cap_code
    return "no_binding_cap"


def _resolve_capacity_decision_reason_code(
    candidate_bundles: list[_CandidateLedgerBundle],
) -> str:
    if not candidate_bundles:
        return "no_candidates"
    if any(
        bundle.decision_row.decision_status == "trimmed" for bundle in candidate_bundles
    ) or any(
        bundle.decision_row.decision_reason_code == "portfolio_capacity_exhausted"
        for bundle in candidate_bundles
    ):
        return "portfolio_capacity_binding"
    if any(
        bundle.decision_row.decision_status == "deferred" for bundle in candidate_bundles
    ):
        return "await_schedule_candidates_present"
    if any(
        bundle.decision_row.decision_status == "blocked" for bundle in candidate_bundles
    ):
        return "upstream_position_blocks_present"
    return "portfolio_capacity_available"


def _build_capacity_reason_summary(
    candidate_bundles: list[_CandidateLedgerBundle],
) -> str:
    status_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    binding_counts: dict[str, int] = {}
    readiness_counts: dict[str, int] = {}
    trimmed_candidates: list[dict[str, object]] = []
    blocked_candidates: list[dict[str, object]] = []
    deferred_candidates: list[dict[str, object]] = []

    for bundle in candidate_bundles:
        decision_row = bundle.decision_row
        status_counts[decision_row.decision_status] = (
            status_counts.get(decision_row.decision_status, 0) + 1
        )
        reason_counts[decision_row.decision_reason_code] = (
            reason_counts.get(decision_row.decision_reason_code, 0) + 1
        )
        binding_counts[decision_row.source_binding_cap_code] = (
            binding_counts.get(decision_row.source_binding_cap_code, 0) + 1
        )
        readiness_counts[decision_row.trade_readiness_status] = (
            readiness_counts.get(decision_row.trade_readiness_status, 0) + 1
        )
        if decision_row.decision_status == "trimmed":
            trimmed_candidates.append(
                {
                    "candidate_nk": decision_row.candidate_nk,
                    "decision_rank": decision_row.decision_rank,
                    "trimmed_weight": decision_row.trimmed_weight,
                    "capacity_after_weight": decision_row.capacity_after_weight,
                }
            )
        elif decision_row.decision_status == "blocked":
            blocked_candidates.append(
                {
                    "candidate_nk": decision_row.candidate_nk,
                    "decision_rank": decision_row.decision_rank,
                    "decision_reason_code": decision_row.decision_reason_code,
                }
            )
        elif decision_row.decision_status == "deferred":
            deferred_candidates.append(
                {
                    "candidate_nk": decision_row.candidate_nk,
                    "decision_rank": decision_row.decision_rank,
                    "decision_reason_code": decision_row.decision_reason_code,
                    "schedule_stage": decision_row.schedule_stage,
                }
            )

    summary: dict[str, object] = {
        "decision_order_code": DEFAULT_DECISION_ORDER_CODE,
        "status_counts": dict(sorted(status_counts.items())),
        "decision_reason_counts": dict(sorted(reason_counts.items())),
        "binding_cap_counts": dict(sorted(binding_counts.items())),
        "trade_readiness_counts": dict(sorted(readiness_counts.items())),
    }
    if trimmed_candidates:
        summary["trimmed_candidates"] = trimmed_candidates
    if blocked_candidates:
        summary["blocked_candidates"] = blocked_candidates
    if deferred_candidates:
        summary["deferred_candidates"] = deferred_candidates
    return json.dumps(summary, ensure_ascii=False, sort_keys=True)


def _upsert_materialized_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    key_column: str,
    key_value: str,
    insert_payload: dict[str, object],
    compare_columns: tuple[str, ...],
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT {", ".join(compare_columns)}, first_seen_run_id
        FROM {table_name}
        WHERE {key_column} = ?
        """,
        [key_value],
    ).fetchone()
    if existing_row is None:
        columns = tuple(insert_payload.keys())
        placeholders = ", ".join("?" for _ in columns)
        connection.execute(
            f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({placeholders})
            """,
            [insert_payload[column] for column in columns],
        )
        return "inserted"

    current_payload = tuple(
        _normalize_comparable_value(existing_row[index])
        for index in range(len(compare_columns))
    )
    next_payload = tuple(
        _normalize_comparable_value(insert_payload[column]) for column in compare_columns
    )
    update_payload = dict(insert_payload)
    update_payload["first_seen_run_id"] = (
        str(existing_row[-1])
        if existing_row[-1] is not None
        else str(insert_payload["first_seen_run_id"])
    )
    columns_to_update = tuple(
        column for column in update_payload.keys() if column != key_column
    )
    set_clause = ", ".join(f"{column} = ?" for column in columns_to_update)
    connection.execute(
        f"""
        UPDATE {table_name}
        SET
            {set_clause},
            updated_at = CURRENT_TIMESTAMP
        WHERE {key_column} = ?
        """,
        [
            *[update_payload[column] for column in columns_to_update],
            key_value,
        ],
    )
    if current_payload == next_payload:
        return "reused"
    return "rematerialized"


def _aggregate_materialization_action(*actions: str) -> str:
    if "inserted" in actions:
        return "inserted"
    if "rematerialized" in actions:
        return "rematerialized"
    return "reused"


def _normalize_comparable_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 12)
    return value
