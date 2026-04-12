"""执行正式 bounded 的 `position -> portfolio_plan` 物化。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.portfolio_plan.bootstrap import (
    PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_RUN_TABLE,
    PORTFOLIO_PLAN_SNAPSHOT_TABLE,
    bootstrap_portfolio_plan_ledger,
    portfolio_plan_ledger_path,
)


DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION: Final[str] = "portfolio-plan-v1"
DEFAULT_SOURCE_POSITION_TABLE: Final[str] = (
    "position_candidate_audit+position_capacity_snapshot+position_sizing_snapshot"
)
DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE: Final[str] = "position_candidate_audit"
DEFAULT_POSITION_CAPACITY_TABLE: Final[str] = "position_capacity_snapshot"
DEFAULT_POSITION_SIZING_TABLE: Final[str] = "position_sizing_snapshot"


@dataclass(frozen=True)
class PortfolioPlanBuildSummary:
    """汇总一次 bounded `portfolio_plan` 物化运行。"""

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
    portfolio_plan_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


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
    runner_version: str = "v1",
    summary_path: Path | None = None,
) -> PortfolioPlanBuildSummary:
    """Read official `position` outputs and materialize portfolio plan decisions."""

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
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    requested_total_weight = 0.0
    admitted_total_weight = 0.0
    trimmed_total_weight = 0.0
    portfolio_gross_used_weight = 0.0
    portfolio_gross_remaining_weight = portfolio_gross_cap_weight

    for bridge_row in bridge_rows:
        snapshot_row = _build_snapshot_row(
            run_id=run_id,
            bridge_row=bridge_row,
            portfolio_id=portfolio_id,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
            portfolio_gross_cap_weight=portfolio_gross_cap_weight,
            portfolio_gross_used_weight=portfolio_gross_used_weight,
            portfolio_gross_remaining_weight=portfolio_gross_remaining_weight,
        )
        materialization_action = _upsert_snapshot(connection, snapshot_row=snapshot_row)
        connection.execute(
            f"""
            INSERT OR REPLACE INTO {PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE} (
                run_id,
                plan_snapshot_nk,
                candidate_nk,
                plan_status,
                materialization_action
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                run_id,
                snapshot_row.plan_snapshot_nk,
                snapshot_row.candidate_nk,
                snapshot_row.plan_status,
                materialization_action,
            ],
        )

        requested_total_weight += snapshot_row.requested_weight
        admitted_total_weight += snapshot_row.admitted_weight
        trimmed_total_weight += snapshot_row.trimmed_weight
        portfolio_gross_used_weight = snapshot_row.portfolio_gross_used_weight
        portfolio_gross_remaining_weight = snapshot_row.portfolio_gross_remaining_weight

        if snapshot_row.plan_status == "admitted":
            admitted_count += 1
        elif snapshot_row.plan_status == "trimmed":
            trimmed_count += 1
        else:
            blocked_count += 1

        if materialization_action == "inserted":
            inserted_count += 1
        elif materialization_action == "reused":
            reused_count += 1
        else:
            rematerialized_count += 1

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
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        requested_total_weight=requested_total_weight,
        admitted_total_weight=admitted_total_weight,
        trimmed_total_weight=trimmed_total_weight,
        portfolio_gross_cap_weight=portfolio_gross_cap_weight,
        portfolio_gross_used_weight=portfolio_gross_used_weight,
        portfolio_gross_remaining_weight=portfolio_gross_remaining_weight,
        source_position_table=source_position_table,
        position_ledger_path=str(position_path),
        portfolio_plan_ledger_path=str(portfolio_plan_path),
    )


def _build_snapshot_row(
    *,
    run_id: str,
    bridge_row: _PositionBridgeRow,
    portfolio_id: str,
    portfolio_plan_contract_version: str,
    portfolio_gross_cap_weight: float,
    portfolio_gross_used_weight: float,
    portfolio_gross_remaining_weight: float,
) -> _PortfolioPlanSnapshotRow:
    requested_weight = max(float(bridge_row.final_allowed_position_weight), 0.0)
    blocking_reason_code: str | None = None
    admitted_weight = 0.0
    trimmed_weight = 0.0

    if bridge_row.candidate_status != "admitted":
        plan_status = "blocked"
        blocking_reason_code = f"position_candidate_{bridge_row.candidate_status or 'blocked'}"
    elif requested_weight <= 0:
        plan_status = "blocked"
        blocking_reason_code = "position_weight_not_positive"
    elif portfolio_gross_remaining_weight >= requested_weight:
        plan_status = "admitted"
        admitted_weight = requested_weight
    elif portfolio_gross_remaining_weight > 0:
        plan_status = "trimmed"
        admitted_weight = portfolio_gross_remaining_weight
        trimmed_weight = max(requested_weight - admitted_weight, 0.0)
    else:
        plan_status = "blocked"
        blocking_reason_code = "portfolio_capacity_exhausted"

    next_used_weight = min(
        portfolio_gross_used_weight + admitted_weight,
        portfolio_gross_cap_weight,
    )
    next_remaining_weight = max(portfolio_gross_cap_weight - next_used_weight, 0.0)
    return _PortfolioPlanSnapshotRow(
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
        requested_weight=requested_weight,
        admitted_weight=admitted_weight,
        trimmed_weight=trimmed_weight,
        plan_status=plan_status,
        blocking_reason_code=blocking_reason_code,
        portfolio_gross_cap_weight=portfolio_gross_cap_weight,
        portfolio_gross_used_weight=next_used_weight,
        portfolio_gross_remaining_weight=next_remaining_weight,
        portfolio_plan_contract_version=portfolio_plan_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
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


def _upsert_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    snapshot_row: _PortfolioPlanSnapshotRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            position_action_decision,
            requested_weight,
            admitted_weight,
            trimmed_weight,
            plan_status,
            blocking_reason_code,
            portfolio_gross_cap_weight,
            portfolio_gross_used_weight,
            portfolio_gross_remaining_weight,
            first_seen_run_id
        FROM {PORTFOLIO_PLAN_SNAPSHOT_TABLE}
        WHERE plan_snapshot_nk = ?
        """,
        [snapshot_row.plan_snapshot_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {PORTFOLIO_PLAN_SNAPSHOT_TABLE} (
                plan_snapshot_nk,
                candidate_nk,
                portfolio_id,
                instrument,
                reference_trade_date,
                position_action_decision,
                requested_weight,
                admitted_weight,
                trimmed_weight,
                plan_status,
                blocking_reason_code,
                portfolio_gross_cap_weight,
                portfolio_gross_used_weight,
                portfolio_gross_remaining_weight,
                portfolio_plan_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                snapshot_row.plan_snapshot_nk,
                snapshot_row.candidate_nk,
                snapshot_row.portfolio_id,
                snapshot_row.instrument,
                snapshot_row.reference_trade_date,
                snapshot_row.position_action_decision,
                snapshot_row.requested_weight,
                snapshot_row.admitted_weight,
                snapshot_row.trimmed_weight,
                snapshot_row.plan_status,
                snapshot_row.blocking_reason_code,
                snapshot_row.portfolio_gross_cap_weight,
                snapshot_row.portfolio_gross_used_weight,
                snapshot_row.portfolio_gross_remaining_weight,
                snapshot_row.portfolio_plan_contract_version,
                snapshot_row.first_seen_run_id,
                snapshot_row.last_materialized_run_id,
            ],
        )
        return "inserted"

    current_payload = (
        _normalize_optional_str(existing_row[0]),
        float(existing_row[1] or 0.0),
        float(existing_row[2] or 0.0),
        float(existing_row[3] or 0.0),
        _normalize_optional_str(existing_row[4]).lower(),
        None if existing_row[5] is None else str(existing_row[5]),
        float(existing_row[6] or 0.0),
        float(existing_row[7] or 0.0),
        float(existing_row[8] or 0.0),
    )
    next_payload = (
        snapshot_row.position_action_decision,
        snapshot_row.requested_weight,
        snapshot_row.admitted_weight,
        snapshot_row.trimmed_weight,
        snapshot_row.plan_status,
        snapshot_row.blocking_reason_code,
        snapshot_row.portfolio_gross_cap_weight,
        snapshot_row.portfolio_gross_used_weight,
        snapshot_row.portfolio_gross_remaining_weight,
    )
    first_seen_run_id = str(existing_row[9]) if existing_row[9] is not None else snapshot_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_SNAPSHOT_TABLE}
        SET
            instrument = ?,
            position_action_decision = ?,
            requested_weight = ?,
            admitted_weight = ?,
            trimmed_weight = ?,
            plan_status = ?,
            blocking_reason_code = ?,
            portfolio_gross_cap_weight = ?,
            portfolio_gross_used_weight = ?,
            portfolio_gross_remaining_weight = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE plan_snapshot_nk = ?
        """,
        [
            snapshot_row.instrument,
            snapshot_row.position_action_decision,
            snapshot_row.requested_weight,
            snapshot_row.admitted_weight,
            snapshot_row.trimmed_weight,
            snapshot_row.plan_status,
            snapshot_row.blocking_reason_code,
            snapshot_row.portfolio_gross_cap_weight,
            snapshot_row.portfolio_gross_used_weight,
            snapshot_row.portfolio_gross_remaining_weight,
            first_seen_run_id,
            snapshot_row.last_materialized_run_id,
            snapshot_row.plan_snapshot_nk,
        ],
    )
    if current_payload == next_payload:
        return "reused"
    return "rematerialized"


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
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            admitted_count,
            blocked_count,
            trimmed_count,
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
