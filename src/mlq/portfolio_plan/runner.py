"""执行正式 data-grade 的 `position -> portfolio_plan` v2 物化。"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.portfolio_plan.bootstrap import (
    PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE,
    PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_SNAPSHOT_TABLE,
    bootstrap_portfolio_plan_ledger,
    portfolio_plan_ledger_path,
)
from mlq.portfolio_plan.materialization import (
    DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
    _CandidateLedgerBundle,
    _PositionBridgeRow,
    _PortfolioCapacitySnapshotRow,
    _aggregate_materialization_action,
    _build_capacity_reason_summary,
    _build_candidate_bundle,
    _build_capacity_snapshot_nk,
    _decision_sort_key,
    _evaluate_candidate_decision,
    _resolve_binding_constraint_code,
    _resolve_capacity_decision_reason_code,
    _upsert_materialized_row,
)
from mlq.portfolio_plan.runner_queue import (
    build_candidate_checkpoint_nk,
    build_portfolio_checkpoint_nk,
    build_source_fingerprint,
    claim_portfolio_plan_queue_rows,
    delete_stale_date_scope_rows,
    enqueue_portfolio_plan_dirty_candidates,
    mark_queue_rows_completed,
    mark_queue_rows_failed,
    upsert_portfolio_plan_checkpoint,
)
from mlq.portfolio_plan.runner_reporting import (
    insert_run_row,
    mark_run_completed,
    mark_run_failed,
    upsert_portfolio_plan_freshness_audit,
    write_summary,
)
from mlq.portfolio_plan.runner_shared import (
    DEFAULT_CANDIDATE_CHECKPOINT_SCOPE,
    DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION,
    DEFAULT_PORTFOLIO_PLAN_RUNNER_NAME,
    DEFAULT_PORTFOLIO_PLAN_RUNNER_VERSION,
    DEFAULT_SOURCE_POSITION_TABLE,
)
from mlq.portfolio_plan.runner_source import (
    load_bridge_rows_for_claimed_dates,
    load_position_bridge_rows,
    max_reference_trade_date,
    normalize_date_value,
    normalize_optional_str,
)


@dataclass(frozen=True)
class PortfolioPlanBuildSummary:
    """汇总一次 data-grade `portfolio_plan` 运行。"""

    run_id: str
    runner_name: str
    runner_version: str
    execution_mode: str
    portfolio_id: str
    portfolio_plan_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_candidate_count: int
    processed_candidate_count: int
    admitted_count: int
    blocked_count: int
    trimmed_count: int
    deferred_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    freshness_updated_count: int
    requested_total_weight: float
    admitted_total_weight: float
    trimmed_total_weight: float
    blocked_total_weight: float
    deferred_total_weight: float
    decision_reason_counts: dict[str, int]
    trade_readiness_counts: dict[str, int]
    portfolio_gross_cap_weight: float
    portfolio_gross_used_weight: float
    portfolio_gross_remaining_weight: float
    latest_reference_trade_date: str | None
    expected_reference_trade_date: str | None
    freshness_status: str
    source_position_table: str
    position_ledger_path: str
    portfolio_plan_ledger_path: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 `summary_json` 的稳定字典。"""

        return asdict(self)


def run_portfolio_plan_build(
    *,
    portfolio_id: str,
    portfolio_gross_cap_weight: float,
    settings: WorkspaceRoots | None = None,
    position_path: Path | None = None,
    portfolio_plan_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    candidate_nks: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    source_position_table: str = DEFAULT_SOURCE_POSITION_TABLE,
    portfolio_plan_contract_version: str = DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION,
    runner_name: str = DEFAULT_PORTFOLIO_PLAN_RUNNER_NAME,
    runner_version: str = DEFAULT_PORTFOLIO_PLAN_RUNNER_VERSION,
    bootstrap_mode: bool = False,
    incremental_mode: bool | None = None,
    replay_mode: bool = False,
    summary_path: Path | None = None,
) -> PortfolioPlanBuildSummary:
    """Read official `position` outputs and materialize `portfolio_plan` v2 ledgers."""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_candidate_nks = tuple(
        sorted({item for item in candidate_nks or () if item})
    )
    normalized_limit = max(int(limit), 1)
    normalized_cap_weight = max(float(portfolio_gross_cap_weight), 0.0)
    execution_mode = _resolve_execution_mode(
        bootstrap_mode=bootstrap_mode,
        incremental_mode=incremental_mode,
        replay_mode=replay_mode,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        candidate_nks=normalized_candidate_nks,
    )
    planning_run_id = run_id or _build_portfolio_plan_run_id()
    resolved_position_path = Path(position_path or workspace.databases.position)
    resolved_portfolio_plan_path = Path(
        portfolio_plan_path or portfolio_plan_ledger_path(workspace)
    )
    _ensure_database_exists(resolved_position_path, label="position")
    portfolio_plan_connection = duckdb.connect(str(resolved_portfolio_plan_path))
    claimed_queue_rows: list[dict[str, object]] = []
    try:
        bootstrap_portfolio_plan_ledger(workspace, connection=portfolio_plan_connection)
        source_bridge_rows = load_position_bridge_rows(
            position_path=resolved_position_path,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            reference_trade_dates=(),
            instruments=normalized_instruments,
            candidate_nks=normalized_candidate_nks,
            limit=normalized_limit if execution_mode == "bootstrap" else None,
        )
        insert_run_row(
            portfolio_plan_connection,
            run_id=planning_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode=execution_mode,
            portfolio_id=portfolio_id,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_candidate_count=len(source_bridge_rows),
            source_position_table=source_position_table,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
        )

        queue_enqueued_count = 0
        processed_bridge_rows = source_bridge_rows
        if execution_mode in {"incremental", "replay"}:
            queue_enqueued_count = enqueue_portfolio_plan_dirty_candidates(
                portfolio_plan_connection,
                bridge_rows=source_bridge_rows,
                run_id=planning_run_id,
                portfolio_id=portfolio_id,
                portfolio_plan_contract_version=portfolio_plan_contract_version,
                portfolio_gross_cap_weight=normalized_cap_weight,
                force_replay=execution_mode == "replay",
            )
            claimed_queue_rows = claim_portfolio_plan_queue_rows(
                portfolio_plan_connection,
                run_id=planning_run_id,
                portfolio_id=portfolio_id,
                candidate_nks={row.candidate_nk for row in source_bridge_rows},
                limit=normalized_limit,
            )
            processed_bridge_rows = load_bridge_rows_for_claimed_dates(
                position_path=resolved_position_path,
                claimed_queue_rows=claimed_queue_rows,
                signal_start_date=normalized_start_date,
                signal_end_date=normalized_end_date,
            )

        counts = _materialize_portfolio_plan_rows(
            connection=portfolio_plan_connection,
            bridge_rows=processed_bridge_rows,
            run_id=planning_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode=execution_mode,
            portfolio_id=portfolio_id,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            portfolio_gross_cap_weight=normalized_cap_weight,
            source_position_table=source_position_table,
            position_path=resolved_position_path,
            portfolio_plan_path=resolved_portfolio_plan_path,
            queue_rows=claimed_queue_rows,
        )
        latest_reference_trade_date, expected_reference_trade_date, freshness_status = (
            upsert_portfolio_plan_freshness_audit(
                portfolio_plan_connection,
                portfolio_id=portfolio_id,
                expected_reference_trade_date=max_reference_trade_date(source_bridge_rows),
                last_success_run_id=planning_run_id,
            )
        )
        summary = PortfolioPlanBuildSummary(
            run_id=planning_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode=execution_mode,
            portfolio_id=portfolio_id,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
            signal_start_date=(
                None if normalized_start_date is None else normalized_start_date.isoformat()
            ),
            signal_end_date=(
                None if normalized_end_date is None else normalized_end_date.isoformat()
            ),
            bounded_candidate_count=len(source_bridge_rows),
            processed_candidate_count=counts["candidate_count"],
            admitted_count=counts["admitted_count"],
            blocked_count=counts["blocked_count"],
            trimmed_count=counts["trimmed_count"],
            deferred_count=counts["deferred_count"],
            inserted_count=counts["inserted_count"],
            reused_count=counts["reused_count"],
            rematerialized_count=counts["rematerialized_count"],
            queue_enqueued_count=queue_enqueued_count,
            queue_claimed_count=len(claimed_queue_rows),
            checkpoint_upserted_count=counts["checkpoint_upserted_count"],
            freshness_updated_count=1,
            requested_total_weight=counts["requested_total_weight"],
            admitted_total_weight=counts["admitted_total_weight"],
            trimmed_total_weight=counts["trimmed_total_weight"],
            blocked_total_weight=counts["blocked_total_weight"],
            deferred_total_weight=counts["deferred_total_weight"],
            decision_reason_counts=dict(sorted(counts["decision_reason_counts"].items())),
            trade_readiness_counts=dict(sorted(counts["trade_readiness_counts"].items())),
            portfolio_gross_cap_weight=normalized_cap_weight,
            portfolio_gross_used_weight=counts["latest_used_weight"],
            portfolio_gross_remaining_weight=counts["latest_remaining_weight"],
            latest_reference_trade_date=(
                None
                if latest_reference_trade_date is None
                else latest_reference_trade_date.isoformat()
            ),
            expected_reference_trade_date=(
                None
                if expected_reference_trade_date is None
                else expected_reference_trade_date.isoformat()
            ),
            freshness_status=freshness_status,
            source_position_table=source_position_table,
            position_ledger_path=str(resolved_position_path),
            portfolio_plan_ledger_path=str(resolved_portfolio_plan_path),
        )
        mark_run_completed(portfolio_plan_connection, run_id=planning_run_id, summary=summary)
        write_summary(summary, summary_path)
        return summary
    except Exception:
        if claimed_queue_rows:
            mark_queue_rows_failed(
                portfolio_plan_connection,
                queue_nks=[str(row["queue_nk"]) for row in claimed_queue_rows],
                run_id=planning_run_id,
                error_text="portfolio_plan_build_failed",
            )
        mark_run_failed(portfolio_plan_connection, run_id=planning_run_id)
        raise
    finally:
        portfolio_plan_connection.close()


def _resolve_execution_mode(
    *,
    bootstrap_mode: bool,
    incremental_mode: bool | None,
    replay_mode: bool,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    candidate_nks: tuple[str, ...],
) -> str:
    explicit_mode_count = int(bootstrap_mode) + int(replay_mode)
    if incremental_mode is True:
        explicit_mode_count += 1
    if explicit_mode_count > 1:
        raise ValueError(
            "bootstrap_mode / incremental_mode / replay_mode 只能显式选择一种。"
        )
    if replay_mode:
        return "replay"
    if bootstrap_mode:
        return "bootstrap"
    if incremental_mode is True:
        return "incremental"
    if incremental_mode is False:
        return "bootstrap"
    has_bounded_scope = bool(
        signal_start_date is not None
        or signal_end_date is not None
        or instruments
        or candidate_nks
    )
    return "bootstrap" if has_bounded_scope else "incremental"


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_portfolio_plan_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"portfolio-plan-{timestamp}"


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")

def _materialize_portfolio_plan_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    bridge_rows: list[_PositionBridgeRow],
    run_id: str,
    runner_name: str,
    runner_version: str,
    execution_mode: str,
    portfolio_id: str,
    portfolio_plan_contract_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    portfolio_gross_cap_weight: float,
    source_position_table: str,
    position_path: Path,
    portfolio_plan_path: Path,
    queue_rows: list[dict[str, object]],
) -> dict[str, object]:
    del runner_name, runner_version, execution_mode, signal_start_date, signal_end_date
    del source_position_table, position_path, portfolio_plan_path
    counts: dict[str, object] = {
        "candidate_count": 0,
        "admitted_count": 0,
        "blocked_count": 0,
        "trimmed_count": 0,
        "deferred_count": 0,
        "inserted_count": 0,
        "reused_count": 0,
        "rematerialized_count": 0,
        "checkpoint_upserted_count": 0,
        "requested_total_weight": 0.0,
        "admitted_total_weight": 0.0,
        "trimmed_total_weight": 0.0,
        "blocked_total_weight": 0.0,
        "deferred_total_weight": 0.0,
        "decision_reason_counts": defaultdict(int),
        "trade_readiness_counts": defaultdict(int),
        "latest_used_weight": 0.0,
        "latest_remaining_weight": portfolio_gross_cap_weight,
    }
    if not bridge_rows:
        return counts
    queue_rows_by_key = {
        (
            str(row["candidate_nk"]),
            normalize_date_value(
                row["reference_trade_date"],
                field_name="queue.reference_trade_date",
            ),
        ): row
        for row in queue_rows
    }
    queue_rows_by_date: dict[date, list[dict[str, object]]] = defaultdict(list)
    for row in queue_rows:
        queue_rows_by_date[
            normalize_date_value(
                row["reference_trade_date"],
                field_name="queue.reference_trade_date",
            )
        ].append(row)

    rows_by_trade_date: dict[date, list[_PositionBridgeRow]] = defaultdict(list)
    for bridge_row in bridge_rows:
        rows_by_trade_date[bridge_row.reference_trade_date].append(bridge_row)

    for reference_trade_date in sorted(rows_by_trade_date):
        date_rows = sorted(rows_by_trade_date[reference_trade_date], key=_decision_sort_key)
        date_queue_rows = queue_rows_by_date.get(reference_trade_date, [])
        try:
            delete_stale_date_scope_rows(
                connection,
                portfolio_id=portfolio_id,
                reference_trade_date=reference_trade_date,
                current_candidate_nks={row.candidate_nk for row in date_rows},
                portfolio_plan_contract_version=portfolio_plan_contract_version,
            )
            capacity_snapshot_nk = _build_capacity_snapshot_nk(
                portfolio_id=portfolio_id,
                capacity_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
                reference_trade_date=reference_trade_date,
                portfolio_plan_contract_version=portfolio_plan_contract_version,
            )
            used_weight = 0.0
            remaining_weight = portfolio_gross_cap_weight
            date_requested_candidate_count = len(date_rows)
            date_admitted_count = 0
            date_blocked_count = 0
            date_trimmed_count = 0
            date_deferred_count = 0
            date_requested_total_weight = 0.0
            date_admitted_total_weight = 0.0
            date_trimmed_total_weight = 0.0
            date_blocked_total_weight = 0.0
            date_deferred_total_weight = 0.0
            candidate_bundles: list[_CandidateLedgerBundle] = []
            for decision_rank, bridge_row in enumerate(date_rows, start=1):
                evaluation = _evaluate_candidate_decision(
                    bridge_row=bridge_row,
                    portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                    portfolio_gross_used_weight=used_weight,
                    portfolio_gross_remaining_weight=remaining_weight,
                )
                bundle = _build_candidate_bundle(
                    run_id=run_id,
                    bridge_row=bridge_row,
                    evaluation=evaluation,
                    portfolio_id=portfolio_id,
                    portfolio_plan_contract_version=portfolio_plan_contract_version,
                    capacity_snapshot_nk=capacity_snapshot_nk,
                    portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                    decision_rank=decision_rank,
                )
                candidate_bundles.append(bundle)
                used_weight = evaluation.next_used_weight
                remaining_weight = evaluation.next_remaining_weight
                counts["requested_total_weight"] += evaluation.requested_weight
                counts["admitted_total_weight"] += evaluation.admitted_weight
                counts["trimmed_total_weight"] += evaluation.trimmed_weight
                date_requested_total_weight += evaluation.requested_weight
                date_admitted_total_weight += evaluation.admitted_weight
                date_trimmed_total_weight += evaluation.trimmed_weight
                counts["decision_reason_counts"][evaluation.decision_reason_code] += 1
                counts["trade_readiness_counts"][evaluation.trade_readiness_status] += 1
                if evaluation.decision_status == "admitted":
                    counts["admitted_count"] += 1
                    date_admitted_count += 1
                elif evaluation.decision_status == "trimmed":
                    counts["trimmed_count"] += 1
                    date_trimmed_count += 1
                elif evaluation.decision_status == "deferred":
                    counts["deferred_count"] += 1
                    date_deferred_count += 1
                    counts["deferred_total_weight"] += evaluation.requested_weight
                    date_deferred_total_weight += evaluation.requested_weight
                else:
                    counts["blocked_count"] += 1
                    date_blocked_count += 1
                    counts["blocked_total_weight"] += evaluation.requested_weight
                    date_blocked_total_weight += evaluation.requested_weight

            capacity_row = _PortfolioCapacitySnapshotRow(
                capacity_snapshot_nk=capacity_snapshot_nk,
                portfolio_id=portfolio_id,
                capacity_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
                reference_trade_date=reference_trade_date,
                portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                portfolio_gross_used_weight=used_weight,
                portfolio_gross_remaining_weight=remaining_weight,
                requested_candidate_count=date_requested_candidate_count,
                admitted_candidate_count=date_admitted_count,
                blocked_candidate_count=date_blocked_count,
                trimmed_candidate_count=date_trimmed_count,
                deferred_candidate_count=date_deferred_count,
                requested_total_weight=date_requested_total_weight,
                admitted_total_weight=date_admitted_total_weight,
                trimmed_total_weight=date_trimmed_total_weight,
                blocked_total_weight=date_blocked_total_weight,
                deferred_total_weight=date_deferred_total_weight,
                binding_constraint_code=_resolve_binding_constraint_code(candidate_bundles),
                capacity_decision_reason_code=_resolve_capacity_decision_reason_code(
                    candidate_bundles
                ),
                capacity_reason_summary_json=_build_capacity_reason_summary(candidate_bundles),
                portfolio_plan_contract_version=portfolio_plan_contract_version,
                first_seen_run_id=run_id,
                last_materialized_run_id=run_id,
            )
            capacity_action = _upsert_materialized_row(
                connection,
                table_name=PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE,
                key_column="capacity_snapshot_nk",
                key_value=capacity_row.capacity_snapshot_nk,
                insert_payload=asdict(capacity_row),
                compare_columns=(
                    "portfolio_id",
                    "capacity_scope",
                    "reference_trade_date",
                    "portfolio_gross_cap_weight",
                    "portfolio_gross_used_weight",
                    "portfolio_gross_remaining_weight",
                    "requested_candidate_count",
                    "admitted_candidate_count",
                    "blocked_candidate_count",
                    "trimmed_candidate_count",
                    "deferred_candidate_count",
                    "requested_total_weight",
                    "admitted_total_weight",
                    "trimmed_total_weight",
                    "blocked_total_weight",
                    "deferred_total_weight",
                    "binding_constraint_code",
                    "capacity_decision_reason_code",
                    "capacity_reason_summary_json",
                    "portfolio_plan_contract_version",
                ),
            )

            for bridge_row, bundle in zip(date_rows, candidate_bundles, strict=True):
                decision_action = _upsert_materialized_row(
                    connection,
                    table_name=PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE,
                    key_column="candidate_decision_nk",
                    key_value=bundle.decision_row.candidate_decision_nk,
                    insert_payload=asdict(bundle.decision_row),
                    compare_columns=(
                        "candidate_nk",
                        "portfolio_id",
                        "instrument",
                        "policy_id",
                        "reference_trade_date",
                        "position_action_decision",
                        "decision_status",
                        "decision_reason_code",
                        "blocking_reason_code",
                        "decision_rank",
                        "decision_order_code",
                        "source_candidate_status",
                        "source_blocked_reason_code",
                        "source_binding_cap_code",
                        "source_capacity_source_code",
                        "source_required_reduction_weight",
                        "source_remaining_single_name_capacity_weight",
                        "source_remaining_portfolio_capacity_weight",
                        "capacity_before_weight",
                        "capacity_after_weight",
                        "trade_readiness_status",
                        "schedule_stage",
                        "schedule_lag_days",
                        "requested_weight",
                        "admitted_weight",
                        "trimmed_weight",
                        "capacity_snapshot_nk",
                        "portfolio_plan_contract_version",
                    ),
                )
                allocation_action = _upsert_materialized_row(
                    connection,
                    table_name=PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE,
                    key_column="allocation_snapshot_nk",
                    key_value=bundle.allocation_row.allocation_snapshot_nk,
                    insert_payload=asdict(bundle.allocation_row),
                    compare_columns=(
                        "candidate_nk",
                        "portfolio_id",
                        "instrument",
                        "allocation_scene",
                        "reference_trade_date",
                        "requested_weight",
                        "admitted_weight",
                        "trimmed_weight",
                        "final_allocated_weight",
                        "plan_status",
                        "decision_reason_code",
                        "blocking_reason_code",
                        "decision_rank",
                        "decision_order_code",
                        "trade_readiness_status",
                        "schedule_stage",
                        "schedule_lag_days",
                        "source_binding_cap_code",
                        "candidate_decision_nk",
                        "capacity_snapshot_nk",
                        "portfolio_plan_contract_version",
                    ),
                )
                snapshot_action = _upsert_materialized_row(
                    connection,
                    table_name=PORTFOLIO_PLAN_SNAPSHOT_TABLE,
                    key_column="plan_snapshot_nk",
                    key_value=bundle.snapshot_row.plan_snapshot_nk,
                    insert_payload=asdict(bundle.snapshot_row),
                    compare_columns=(
                        "candidate_nk",
                        "portfolio_id",
                        "instrument",
                        "reference_trade_date",
                        "position_action_decision",
                        "requested_weight",
                        "admitted_weight",
                        "trimmed_weight",
                        "plan_status",
                        "decision_reason_code",
                        "blocking_reason_code",
                        "decision_rank",
                        "decision_order_code",
                        "trade_readiness_status",
                        "schedule_stage",
                        "schedule_lag_days",
                        "source_binding_cap_code",
                        "portfolio_gross_cap_weight",
                        "portfolio_gross_used_weight",
                        "portfolio_gross_remaining_weight",
                        "candidate_decision_nk",
                        "capacity_snapshot_nk",
                        "allocation_snapshot_nk",
                        "portfolio_plan_contract_version",
                    ),
                )
                materialization_action = _aggregate_materialization_action(
                    capacity_action,
                    decision_action,
                    allocation_action,
                    snapshot_action,
                )
                queue_row = queue_rows_by_key.get(
                    (bridge_row.candidate_nk, bridge_row.reference_trade_date)
                )
                connection.execute(
                    f"""
                    INSERT OR REPLACE INTO {PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE} (
                        run_id,
                        plan_snapshot_nk,
                        candidate_decision_nk,
                        capacity_snapshot_nk,
                        allocation_snapshot_nk,
                        candidate_nk,
                        plan_status,
                        decision_reason_code,
                        trade_readiness_status,
                        queue_nk,
                        queue_reason,
                        materialization_action
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        bundle.snapshot_row.plan_snapshot_nk,
                        bundle.decision_row.candidate_decision_nk,
                        bundle.decision_row.capacity_snapshot_nk,
                        bundle.allocation_row.allocation_snapshot_nk,
                        bundle.decision_row.candidate_nk,
                        bundle.decision_row.decision_status,
                        bundle.decision_row.decision_reason_code,
                        bundle.decision_row.trade_readiness_status,
                        None if queue_row is None else str(queue_row["queue_nk"]),
                        None if queue_row is None else str(queue_row["queue_reason"]),
                        materialization_action,
                    ],
                )
                counts["candidate_count"] += 1
                counts[f"{materialization_action}_count"] += 1
                candidate_checkpoint_payload = {
                    "candidate_nk": bridge_row.candidate_nk,
                    "instrument": bridge_row.instrument,
                    "reference_trade_date": bridge_row.reference_trade_date.isoformat(),
                    "source_fingerprint": build_source_fingerprint(
                        bridge_row=bridge_row,
                        portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                        portfolio_plan_contract_version=portfolio_plan_contract_version,
                    ),
                }
                upsert_portfolio_plan_checkpoint(
                    connection,
                    checkpoint_nk=build_candidate_checkpoint_nk(
                        portfolio_id=portfolio_id,
                        candidate_nk=bridge_row.candidate_nk,
                    ),
                    portfolio_id=portfolio_id,
                    checkpoint_scope=DEFAULT_CANDIDATE_CHECKPOINT_SCOPE,
                    last_completed_reference_trade_date=bridge_row.reference_trade_date,
                    last_completed_candidate_nk=bridge_row.candidate_nk,
                    last_run_id=run_id,
                    checkpoint_payload=candidate_checkpoint_payload,
                )
                counts["checkpoint_upserted_count"] += 1

            portfolio_checkpoint_payload = {
                "capacity_snapshot_nk": capacity_snapshot_nk,
                "candidate_count": len(date_rows),
                "last_completed_reference_trade_date": reference_trade_date.isoformat(),
            }
            upsert_portfolio_plan_checkpoint(
                connection,
                checkpoint_nk=build_portfolio_checkpoint_nk(
                    portfolio_id=portfolio_id,
                    checkpoint_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
                ),
                portfolio_id=portfolio_id,
                checkpoint_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
                last_completed_reference_trade_date=reference_trade_date,
                last_completed_candidate_nk=(
                    None if not date_rows else date_rows[-1].candidate_nk
                ),
                last_run_id=run_id,
                checkpoint_payload=portfolio_checkpoint_payload,
            )
            counts["checkpoint_upserted_count"] += 1
            if date_queue_rows:
                mark_queue_rows_completed(
                    connection,
                    queue_nks=[str(row["queue_nk"]) for row in date_queue_rows],
                    run_id=run_id,
                )
            counts["latest_used_weight"] = used_weight
            counts["latest_remaining_weight"] = remaining_weight
        except Exception:
            if date_queue_rows:
                mark_queue_rows_failed(
                    connection,
                    queue_nks=[str(row["queue_nk"]) for row in date_queue_rows],
                    run_id=run_id,
                    error_text=f"date_scope_failed:{reference_trade_date.isoformat()}",
                )
            raise
    return counts
