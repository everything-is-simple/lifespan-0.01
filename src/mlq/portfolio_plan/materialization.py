"""`portfolio_plan` v2 ledger row、自然键与 upsert helper。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import duckdb


DEFAULT_PORTFOLIO_CAPACITY_SCOPE = "portfolio_gross"
DEFAULT_ALLOCATION_SCENE = "trade_ready"


@dataclass(frozen=True)
class _PositionBridgeRow:
    candidate_nk: str
    instrument: str
    policy_id: str
    reference_trade_date: date
    candidate_status: str
    position_action_decision: str
    final_allowed_position_weight: float
    required_reduction_weight: float


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
    admitted_candidate_count: int
    blocked_candidate_count: int
    trimmed_candidate_count: int
    deferred_candidate_count: int
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
    blocking_reason_code: str | None
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
    blocking_reason_code: str | None
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
    next_used_weight: float
    next_remaining_weight: float


@dataclass(frozen=True)
class _CandidateLedgerBundle:
    decision_row: _PortfolioCandidateDecisionRow
    allocation_row: _PortfolioAllocationSnapshotRow
    snapshot_row: _PortfolioPlanSnapshotRow


def _evaluate_candidate_decision(
    *,
    bridge_row: _PositionBridgeRow,
    portfolio_gross_cap_weight: float,
    portfolio_gross_used_weight: float,
    portfolio_gross_remaining_weight: float,
) -> _DecisionEvaluation:
    requested_weight = max(float(bridge_row.final_allowed_position_weight), 0.0)
    blocking_reason_code: str | None = None
    admitted_weight = 0.0
    trimmed_weight = 0.0

    if bridge_row.candidate_status != "admitted":
        decision_status = "blocked"
        decision_reason_code = (
            f"position_candidate_{bridge_row.candidate_status or 'blocked'}"
        )
        blocking_reason_code = decision_reason_code
    elif requested_weight <= 0:
        decision_status = "blocked"
        decision_reason_code = "position_weight_not_positive"
        blocking_reason_code = decision_reason_code
    elif portfolio_gross_remaining_weight >= requested_weight:
        decision_status = "admitted"
        decision_reason_code = "admitted_without_trim"
        admitted_weight = requested_weight
    elif portfolio_gross_remaining_weight > 0:
        decision_status = "trimmed"
        decision_reason_code = "trimmed_by_portfolio_capacity"
        admitted_weight = portfolio_gross_remaining_weight
        trimmed_weight = max(requested_weight - admitted_weight, 0.0)
    else:
        decision_status = "blocked"
        decision_reason_code = "portfolio_capacity_exhausted"
        blocking_reason_code = decision_reason_code

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
        next_used_weight=next_used_weight,
        next_remaining_weight=next_remaining_weight,
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
        blocking_reason_code=evaluation.blocking_reason_code,
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
        blocking_reason_code=evaluation.blocking_reason_code,
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
