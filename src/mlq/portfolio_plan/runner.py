"""执行正式 bounded 的 `position -> portfolio_plan` v2 物化。"""
from __future__ import annotations
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final
import duckdb
from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.portfolio_plan.bootstrap import (
    PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE,
    PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_RUN_TABLE,
    PORTFOLIO_PLAN_SNAPSHOT_TABLE,
    bootstrap_portfolio_plan_ledger,
    portfolio_plan_ledger_path,
)
from mlq.portfolio_plan.materialization import (
    DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
    _PositionBridgeRow,
    _PortfolioCapacitySnapshotRow,
    _aggregate_materialization_action,
    _build_candidate_bundle,
    _build_capacity_snapshot_nk,
    _evaluate_candidate_decision,
    _upsert_materialized_row,
)
DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION: Final[str] = "portfolio-plan-v2"
DEFAULT_SOURCE_POSITION_TABLE: Final[str] = (
    "position_candidate_audit+position_capacity_snapshot+position_sizing_snapshot"
)
DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE: Final[str] = "position_candidate_audit"
DEFAULT_POSITION_CAPACITY_TABLE: Final[str] = "position_capacity_snapshot"
DEFAULT_POSITION_SIZING_TABLE: Final[str] = "position_sizing_snapshot"
@dataclass(frozen=True)
class PortfolioPlanBuildSummary:
    """汇总一次 bounded `portfolio_plan` v2 物化运行。"""
    run_id: str
    runner_name: str
    runner_version: str
    portfolio_id: str
    portfolio_plan_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_candidate_count: int
    admitted_count: int
    blocked_count: int
    trimmed_count: int
    deferred_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    requested_total_weight: float
    admitted_total_weight: float
    trimmed_total_weight: float
    portfolio_gross_cap_weight: float
    portfolio_gross_used_weight: float
    portfolio_gross_remaining_weight: float
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
    runner_name: str = "portfolio_plan_builder",
    runner_version: str = "v2",
    summary_path: Path | None = None,
) -> PortfolioPlanBuildSummary:
    """Read official `position` outputs and materialize `portfolio_plan` v2 ledgers."""
    workspace = settings or default_settings()
    workspace.ensure_directories()
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_candidate_nks = tuple(sorted({item for item in candidate_nks or () if item}))
    normalized_limit = max(int(limit), 1)
    normalized_cap_weight = max(float(portfolio_gross_cap_weight), 0.0)
    planning_run_id = run_id or _build_portfolio_plan_run_id()
    resolved_position_path = Path(position_path or workspace.databases.position)
    resolved_portfolio_plan_path = Path(portfolio_plan_path or portfolio_plan_ledger_path(workspace))
    _ensure_database_exists(resolved_position_path, label="position")
    portfolio_plan_connection = duckdb.connect(str(resolved_portfolio_plan_path))
    try:
        bootstrap_portfolio_plan_ledger(workspace, connection=portfolio_plan_connection)
        bridge_rows = _load_position_bridge_rows(
            position_path=resolved_position_path,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=normalized_instruments,
            candidate_nks=normalized_candidate_nks,
            limit=normalized_limit,
        )
        _insert_run_row(
            portfolio_plan_connection,
            run_id=planning_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            portfolio_id=portfolio_id,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_candidate_count=len(bridge_rows),
            source_position_table=source_position_table,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
        )
        summary = _materialize_portfolio_plan_rows(
            connection=portfolio_plan_connection,
            bridge_rows=bridge_rows,
            run_id=planning_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            portfolio_id=portfolio_id,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            portfolio_gross_cap_weight=normalized_cap_weight,
            source_position_table=source_position_table,
            position_path=resolved_position_path,
            portfolio_plan_path=resolved_portfolio_plan_path,
        )
        _mark_run_completed(portfolio_plan_connection, run_id=planning_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            portfolio_plan_connection,
            run_id=planning_run_id,
            run_status="failed",
            admitted_count=0,
            blocked_count=0,
            trimmed_count=0,
            deferred_count=0,
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        portfolio_plan_connection.close()
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
def _load_position_bridge_rows(
    *,
    position_path: Path,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    candidate_nks: tuple[str, ...],
    limit: int,
) -> list[_PositionBridgeRow]:
    connection = duckdb.connect(str(position_path), read_only=True)
    try:
        _ensure_bridge_tables_exist(connection)
        parameters: list[object] = []
        where_clauses: list[str] = []
        if signal_start_date is not None:
            where_clauses.append("s.reference_trade_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("s.reference_trade_date <= ?")
            parameters.append(signal_end_date)
        if instruments:
            placeholders = ", ".join("?" for _ in instruments)
            where_clauses.append(f"a.instrument IN ({placeholders})")
            parameters.extend(instruments)
        if candidate_nks:
            placeholders = ", ".join("?" for _ in candidate_nks)
            where_clauses.append(f"a.candidate_nk IN ({placeholders})")
            parameters.extend(candidate_nks)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        rows = connection.execute(
            f"""
            SELECT
                a.candidate_nk,
                a.instrument,
                a.policy_id,
                s.reference_trade_date,
                a.candidate_status,
                s.position_action_decision,
                c.final_allowed_position_weight,
                COALESCE(s.required_reduction_weight, c.required_reduction_weight, 0)
            FROM {DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE} AS a
            INNER JOIN {DEFAULT_POSITION_CAPACITY_TABLE} AS c
                ON c.candidate_nk = a.candidate_nk
            INNER JOIN {DEFAULT_POSITION_SIZING_TABLE} AS s
                ON s.candidate_nk = a.candidate_nk
            {where_sql}
            ORDER BY s.reference_trade_date, a.instrument, a.candidate_nk
            LIMIT ?
            """,
            [*parameters, limit],
        ).fetchall()
        return [
            _PositionBridgeRow(
                candidate_nk=str(row[0]),
                instrument=str(row[1]),
                policy_id=str(row[2]),
                reference_trade_date=_normalize_date_value(row[3], field_name="reference_trade_date"),
                candidate_status=_normalize_optional_str(row[4]).lower(),
                position_action_decision=_normalize_optional_str(row[5]),
                final_allowed_position_weight=float(row[6] or 0.0),
                required_reduction_weight=float(row[7] or 0.0),
            )
            for row in rows
        ]
    finally:
        connection.close()
def _ensure_bridge_tables_exist(connection: duckdb.DuckDBPyConnection) -> None:
    rows = connection.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        """
    ).fetchall()
    existing_tables = {str(row[0]) for row in rows}
    required_tables = {
        DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE,
        DEFAULT_POSITION_CAPACITY_TABLE,
        DEFAULT_POSITION_SIZING_TABLE,
    }
    missing_tables = sorted(required_tables - existing_tables)
    if missing_tables:
        raise ValueError(
            "Missing required position bridge tables: " + ", ".join(missing_tables)
        )
def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    portfolio_id: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_candidate_count: int,
    source_position_table: str,
    portfolio_plan_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {PORTFOLIO_PLAN_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            portfolio_id,
            signal_start_date,
            signal_end_date,
            bounded_candidate_count,
            source_position_table,
            portfolio_plan_contract_version
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            portfolio_id,
            signal_start_date,
            signal_end_date,
            bounded_candidate_count,
            source_position_table,
            portfolio_plan_contract_version,
        ],
    )
def _materialize_portfolio_plan_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    bridge_rows: list[_PositionBridgeRow],
    run_id: str,
    runner_name: str,
    runner_version: str,
    portfolio_id: str,
    portfolio_plan_contract_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    portfolio_gross_cap_weight: float,
    source_position_table: str,
    position_path: Path,
    portfolio_plan_path: Path,
) -> PortfolioPlanBuildSummary:
    admitted_count = 0
    blocked_count = 0
    trimmed_count = 0
    deferred_count = 0
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    requested_total_weight = 0.0
    admitted_total_weight = 0.0
    trimmed_total_weight = 0.0
    latest_used_weight = 0.0
    latest_remaining_weight = portfolio_gross_cap_weight
    rows_by_trade_date: dict[date, list[_PositionBridgeRow]] = defaultdict(list)
    for bridge_row in bridge_rows:
        rows_by_trade_date[bridge_row.reference_trade_date].append(bridge_row)
    for reference_trade_date in sorted(rows_by_trade_date):
        date_rows = rows_by_trade_date[reference_trade_date]
        capacity_snapshot_nk = _build_capacity_snapshot_nk(
            portfolio_id=portfolio_id,
            capacity_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
            reference_trade_date=reference_trade_date,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
        )
        used_weight = 0.0
        remaining_weight = portfolio_gross_cap_weight
        date_admitted_count = 0
        date_blocked_count = 0
        date_trimmed_count = 0
        date_deferred_count = 0
        candidate_bundles: list[_CandidateLedgerBundle] = []
        for bridge_row in date_rows:
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
            )
            candidate_bundles.append(bundle)
            used_weight = evaluation.next_used_weight
            remaining_weight = evaluation.next_remaining_weight
            requested_total_weight += evaluation.requested_weight
            admitted_total_weight += evaluation.admitted_weight
            trimmed_total_weight += evaluation.trimmed_weight
            if evaluation.decision_status == "admitted":
                admitted_count += 1
                date_admitted_count += 1
            elif evaluation.decision_status == "trimmed":
                trimmed_count += 1
                date_trimmed_count += 1
            elif evaluation.decision_status == "deferred":
                deferred_count += 1
                date_deferred_count += 1
            else:
                blocked_count += 1
                date_blocked_count += 1
        capacity_row = _PortfolioCapacitySnapshotRow(
            capacity_snapshot_nk=capacity_snapshot_nk,
            portfolio_id=portfolio_id,
            capacity_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
            reference_trade_date=reference_trade_date,
            portfolio_gross_cap_weight=portfolio_gross_cap_weight,
            portfolio_gross_used_weight=used_weight,
            portfolio_gross_remaining_weight=remaining_weight,
            admitted_candidate_count=date_admitted_count,
            blocked_candidate_count=date_blocked_count,
            trimmed_candidate_count=date_trimmed_count,
            deferred_candidate_count=date_deferred_count,
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
                "admitted_candidate_count",
                "blocked_candidate_count",
                "trimmed_candidate_count",
                "deferred_candidate_count",
                "portfolio_plan_contract_version",
            ),
        )
        for bundle in candidate_bundles:
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
                    "blocking_reason_code",
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
                    "blocking_reason_code",
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
                    materialization_action
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    bundle.snapshot_row.plan_snapshot_nk,
                    bundle.decision_row.candidate_decision_nk,
                    bundle.decision_row.capacity_snapshot_nk,
                    bundle.allocation_row.allocation_snapshot_nk,
                    bundle.decision_row.candidate_nk,
                    bundle.decision_row.decision_status,
                    materialization_action,
                ],
            )
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1
        latest_used_weight = used_weight
        latest_remaining_weight = remaining_weight
    return PortfolioPlanBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        portfolio_id=portfolio_id,
        portfolio_plan_contract_version=portfolio_plan_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_candidate_count=len(bridge_rows),
        admitted_count=admitted_count,
        blocked_count=blocked_count,
        trimmed_count=trimmed_count,
        deferred_count=deferred_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        requested_total_weight=requested_total_weight,
        admitted_total_weight=admitted_total_weight,
        trimmed_total_weight=trimmed_total_weight,
        portfolio_gross_cap_weight=portfolio_gross_cap_weight,
        portfolio_gross_used_weight=latest_used_weight,
        portfolio_gross_remaining_weight=latest_remaining_weight,
        source_position_table=source_position_table,
        position_ledger_path=str(position_path),
        portfolio_plan_ledger_path=str(portfolio_plan_path),
    )
def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: PortfolioPlanBuildSummary,
) -> None:
    _update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        admitted_count=summary.admitted_count,
        blocked_count=summary.blocked_count,
        trimmed_count=summary.trimmed_count,
        deferred_count=summary.deferred_count,
        summary_payload=summary.as_dict(),
    )
def _update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    admitted_count: int,
    blocked_count: int,
    trimmed_count: int,
    deferred_count: int,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_RUN_TABLE}
        SET
            run_status = ?,
            admitted_count = ?,
            blocked_count = ?,
            trimmed_count = ?,
            deferred_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            admitted_count,
            blocked_count,
            trimmed_count,
            deferred_count,
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )
def _normalize_date_value(value: object, *, field_name: str) -> date:
    if value is None:
        raise ValueError(f"Missing required date field: {field_name}")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()
def _normalize_optional_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()
def _write_summary(summary: PortfolioPlanBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
