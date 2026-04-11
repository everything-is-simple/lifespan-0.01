"""执行 `system` 主链 bounded acceptance readout / audit 物化。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.system.bootstrap import (
    SYSTEM_CHILD_RUN_READOUT_TABLE,
    SYSTEM_MAINLINE_SNAPSHOT_TABLE,
    SYSTEM_RUN_SNAPSHOT_TABLE,
    SYSTEM_RUN_TABLE,
    bootstrap_system_ledger,
    system_ledger_path,
)


DEFAULT_SYSTEM_SCENE: Final[str] = "mainline_bounded_acceptance"
DEFAULT_SYSTEM_CONTRACT_VERSION: Final[str] = "system-mainline-readout-v1"


@dataclass(frozen=True)
class SystemMainlineReadoutSummary:
    """总结一次 bounded `system` mainline readout 结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    portfolio_id: str
    snapshot_date: str
    signal_start_date: str | None
    signal_end_date: str | None
    system_scene: str
    system_contract_version: str
    system_materialization_action: str
    bounded_child_run_count: int
    planned_entry_count: int
    blocked_upstream_count: int
    planned_carry_count: int
    carried_open_leg_count: int
    current_carry_weight: float
    child_readout_inserted_count: int
    child_readout_reused_count: int
    child_readout_rematerialized_count: int
    snapshot_inserted_count: int
    snapshot_reused_count: int
    snapshot_rematerialized_count: int
    acceptance_status: str
    system_ledger_path: str
    portfolio_plan_ledger_path: str
    trade_runtime_ledger_path: str
    child_runs: list[dict[str, object]]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _ChildRunRecord:
    child_module: str
    child_run_id: str
    child_run_status: str
    child_runner_name: str
    child_runner_version: str
    child_contract_version: str | None
    child_signal_start_date: date | None
    child_signal_end_date: date | None
    child_started_at: datetime | None
    child_completed_at: datetime | None
    child_summary_json: str
    child_ledger_path: str

    @property
    def child_run_readout_nk(self) -> str:
        return f"{self.child_module}|{self.child_run_id}"


@dataclass(frozen=True)
class _MainlineSnapshotRow:
    mainline_snapshot_nk: str
    portfolio_id: str
    snapshot_date: date
    system_scene: str
    acceptance_status: str
    acceptance_note: str
    planned_entry_count: int
    blocked_upstream_count: int
    planned_carry_count: int
    carried_open_leg_count: int
    current_carry_weight: float
    included_child_run_count: int
    source_portfolio_plan_run_id: str
    source_trade_run_id: str
    system_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


def run_system_mainline_readout_build(
    *,
    portfolio_id: str,
    settings: WorkspaceRoots | None = None,
    snapshot_date: str | date | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    system_path: Path | None = None,
    portfolio_plan_path: Path | None = None,
    trade_runtime_path: Path | None = None,
    run_id: str | None = None,
    system_scene: str = DEFAULT_SYSTEM_SCENE,
    system_contract_version: str = DEFAULT_SYSTEM_CONTRACT_VERSION,
    runner_name: str = "system_mainline_readout_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
) -> SystemMainlineReadoutSummary:
    """把官方主链结果上收为 `system` 层 readout / audit 快照。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_system_path = Path(system_path or system_ledger_path(workspace))
    resolved_portfolio_plan_path = Path(portfolio_plan_path or workspace.databases.portfolio_plan)
    resolved_trade_runtime_path = Path(trade_runtime_path or workspace.databases.trade_runtime)
    normalized_snapshot_date = _coerce_date(snapshot_date)
    normalized_signal_start_date = _coerce_date(signal_start_date)
    normalized_signal_end_date = _coerce_date(signal_end_date)
    system_run_id = run_id or _build_system_run_id()

    _ensure_database_exists(resolved_portfolio_plan_path, label="portfolio_plan")
    _ensure_database_exists(resolved_trade_runtime_path, label="trade_runtime")

    trade_run = _load_trade_run_record(
        workspace=workspace,
        portfolio_id=portfolio_id,
        snapshot_date=normalized_snapshot_date,
        signal_start_date=normalized_signal_start_date,
        signal_end_date=normalized_signal_end_date,
    )
    effective_snapshot_date = normalized_snapshot_date or _resolve_snapshot_date_from_trade_run(trade_run)
    effective_signal_start_date = normalized_signal_start_date or trade_run.child_signal_start_date
    effective_signal_end_date = normalized_signal_end_date or trade_run.child_signal_end_date

    portfolio_plan_run = _load_portfolio_plan_run_record(
        workspace=workspace,
        portfolio_id=portfolio_id,
        snapshot_date=effective_snapshot_date,
        signal_start_date=effective_signal_start_date,
        signal_end_date=effective_signal_end_date,
    )
    alpha_formal_run = _load_alpha_formal_signal_run_record(
        workspace,
        snapshot_date=effective_snapshot_date,
    )
    child_runs = [
        _load_structure_run_record(workspace, snapshot_date=effective_snapshot_date),
        _load_filter_run_record(workspace, snapshot_date=effective_snapshot_date),
        _load_alpha_trigger_run_record(workspace, snapshot_date=effective_snapshot_date),
        alpha_formal_run,
        _load_position_run_record(
            workspace,
            source_signal_run_id=alpha_formal_run.child_run_id,
        ),
        portfolio_plan_run,
        trade_run,
    ]

    snapshot_payload = _build_mainline_snapshot_row(
        trade_runtime_path=resolved_trade_runtime_path,
        portfolio_id=portfolio_id,
        snapshot_date=effective_snapshot_date,
        child_runs=child_runs,
        system_scene=system_scene,
        system_contract_version=system_contract_version,
        run_id=system_run_id,
        source_portfolio_plan_run_id=portfolio_plan_run.child_run_id,
        source_trade_run_id=trade_run.child_run_id,
    )

    connection = duckdb.connect(str(resolved_system_path))
    try:
        bootstrap_system_ledger(workspace, connection=connection)
        _insert_run_row(
            connection,
            run_id=system_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            portfolio_id=portfolio_id,
            snapshot_date=effective_snapshot_date,
            signal_start_date=effective_signal_start_date,
            signal_end_date=effective_signal_end_date,
            system_scene=system_scene,
            system_contract_version=system_contract_version,
            bounded_child_run_count=len(child_runs),
            planned_entry_count=snapshot_payload.planned_entry_count,
            blocked_upstream_count=snapshot_payload.blocked_upstream_count,
            planned_carry_count=snapshot_payload.planned_carry_count,
            carried_open_leg_count=snapshot_payload.carried_open_leg_count,
        )

        child_readout_inserted_count = 0
        child_readout_reused_count = 0
        child_readout_rematerialized_count = 0
        child_summary_rows: list[dict[str, object]] = []
        for child_run in child_runs:
            action = _upsert_child_run_readout(
                connection,
                child_run=child_run,
                run_id=system_run_id,
            )
            child_summary_rows.append(
                {
                    "child_module": child_run.child_module,
                    "child_run_id": child_run.child_run_id,
                    "materialization_action": action,
                    "child_run_status": child_run.child_run_status,
                }
            )
            if action == "inserted":
                child_readout_inserted_count += 1
            elif action == "reused":
                child_readout_reused_count += 1
            else:
                child_readout_rematerialized_count += 1

        snapshot_action = _upsert_mainline_snapshot(
            connection,
            snapshot_row=snapshot_payload,
        )
        connection.execute(
            f"""
            INSERT OR REPLACE INTO {SYSTEM_RUN_SNAPSHOT_TABLE} (
                run_id,
                mainline_snapshot_nk,
                acceptance_status,
                materialization_action
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                system_run_id,
                snapshot_payload.mainline_snapshot_nk,
                snapshot_payload.acceptance_status,
                snapshot_action,
            ],
        )

        system_materialization_action = _resolve_system_materialization_action(
            snapshot_action=snapshot_action,
            child_readout_inserted_count=child_readout_inserted_count,
            child_readout_reused_count=child_readout_reused_count,
            child_readout_rematerialized_count=child_readout_rematerialized_count,
        )
        summary = SystemMainlineReadoutSummary(
            run_id=system_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            portfolio_id=portfolio_id,
            snapshot_date=effective_snapshot_date.isoformat(),
            signal_start_date=None if effective_signal_start_date is None else effective_signal_start_date.isoformat(),
            signal_end_date=None if effective_signal_end_date is None else effective_signal_end_date.isoformat(),
            system_scene=system_scene,
            system_contract_version=system_contract_version,
            system_materialization_action=system_materialization_action,
            bounded_child_run_count=len(child_runs),
            planned_entry_count=snapshot_payload.planned_entry_count,
            blocked_upstream_count=snapshot_payload.blocked_upstream_count,
            planned_carry_count=snapshot_payload.planned_carry_count,
            carried_open_leg_count=snapshot_payload.carried_open_leg_count,
            current_carry_weight=snapshot_payload.current_carry_weight,
            child_readout_inserted_count=child_readout_inserted_count,
            child_readout_reused_count=child_readout_reused_count,
            child_readout_rematerialized_count=child_readout_rematerialized_count,
            snapshot_inserted_count=1 if snapshot_action == "inserted" else 0,
            snapshot_reused_count=1 if snapshot_action == "reused" else 0,
            snapshot_rematerialized_count=1 if snapshot_action == "rematerialized" else 0,
            acceptance_status=snapshot_payload.acceptance_status,
            system_ledger_path=str(resolved_system_path),
            portfolio_plan_ledger_path=str(resolved_portfolio_plan_path),
            trade_runtime_ledger_path=str(resolved_trade_runtime_path),
            child_runs=child_summary_rows,
        )
        _mark_run_completed(
            connection,
            run_id=system_run_id,
            summary=summary,
        )
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            connection,
            run_id=system_run_id,
            run_status="failed",
            system_materialization_action="failed",
            child_readout_inserted_count=0,
            child_readout_reused_count=0,
            child_readout_rematerialized_count=0,
            snapshot_inserted_count=0,
            snapshot_reused_count=0,
            snapshot_rematerialized_count=0,
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        connection.close()


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_system_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"system-mainline-readout-{timestamp}"


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _resolve_snapshot_date_from_trade_run(trade_run: _ChildRunRecord) -> date:
    if trade_run.child_signal_end_date is not None:
        return trade_run.child_signal_end_date
    if trade_run.child_signal_start_date is not None:
        return trade_run.child_signal_start_date
    if trade_run.child_completed_at is not None:
        return trade_run.child_completed_at.date()
    raise ValueError("Unable to resolve snapshot_date from trade run.")


def _load_structure_run_record(
    workspace: WorkspaceRoots,
    *,
    snapshot_date: date,
) -> _ChildRunRecord:
    return _load_standard_child_run_record(
        db_path=workspace.databases.structure,
        child_module="structure",
        run_table="structure_run",
        runner_name_column="runner_name",
        runner_version_column="runner_version",
        contract_version_column="structure_contract_version",
        started_at_column="started_at",
        completed_at_column="completed_at",
        summary_column="summary_json",
        snapshot_date=snapshot_date,
    )


def _load_filter_run_record(
    workspace: WorkspaceRoots,
    *,
    snapshot_date: date,
) -> _ChildRunRecord:
    return _load_standard_child_run_record(
        db_path=workspace.databases.filter,
        child_module="filter",
        run_table="filter_run",
        runner_name_column="runner_name",
        runner_version_column="runner_version",
        contract_version_column="filter_contract_version",
        started_at_column="started_at",
        completed_at_column="completed_at",
        summary_column="summary_json",
        snapshot_date=snapshot_date,
    )


def _load_alpha_trigger_run_record(
    workspace: WorkspaceRoots,
    *,
    snapshot_date: date,
) -> _ChildRunRecord:
    return _load_standard_child_run_record(
        db_path=workspace.databases.alpha,
        child_module="alpha_trigger",
        run_table="alpha_trigger_run",
        runner_name_column="runner_name",
        runner_version_column="runner_version",
        contract_version_column="trigger_contract_version",
        started_at_column="started_at",
        completed_at_column="completed_at",
        summary_column="summary_json",
        snapshot_date=snapshot_date,
    )


def _load_alpha_formal_signal_run_record(
    workspace: WorkspaceRoots,
    *,
    snapshot_date: date,
) -> _ChildRunRecord:
    return _load_standard_child_run_record(
        db_path=workspace.databases.alpha,
        child_module="alpha_formal_signal",
        run_table="alpha_formal_signal_run",
        runner_name_column="producer_name",
        runner_version_column="producer_version",
        contract_version_column="signal_contract_version",
        started_at_column="started_at",
        completed_at_column="completed_at",
        summary_column="summary_json",
        snapshot_date=snapshot_date,
    )


def _load_position_run_record(
    workspace: WorkspaceRoots,
    *,
    source_signal_run_id: str | None,
) -> _ChildRunRecord:
    connection = duckdb.connect(str(workspace.databases.position), read_only=True)
    try:
        where_sql = "WHERE run_status = 'completed'"
        parameters: list[object] = []
        if source_signal_run_id:
            where_sql += " AND source_signal_run_id LIKE ?"
            parameters.append(f"%{source_signal_run_id}%")
        row = connection.execute(
            f"""
            SELECT
                run_id,
                run_status,
                source_signal_contract_version,
                source_signal_run_id,
                run_started_at,
                run_completed_at,
                notes
            FROM position_run
            {where_sql}
            ORDER BY run_completed_at DESC NULLS LAST, run_id DESC
            LIMIT 1
            """,
            parameters,
        ).fetchone()
        if row is None:
            raise ValueError("Missing completed child run for `position`.")
        summary_json = _normalize_summary_text(
            row[6],
            fallback_payload={
                "notes": row[6],
                "source_signal_contract_version": row[2],
                "source_signal_run_id": row[3],
            },
        )
        return _ChildRunRecord(
            child_module="position",
            child_run_id=str(row[0]),
            child_run_status=_normalize_optional_str(row[1]).lower(),
            child_runner_name="position_formal_signal_materializer",
            child_runner_version="v1",
            child_contract_version=None if row[2] is None else str(row[2]),
            child_signal_start_date=None,
            child_signal_end_date=None,
            child_started_at=_normalize_datetime_value(row[4]),
            child_completed_at=_normalize_datetime_value(row[5]),
            child_summary_json=summary_json,
            child_ledger_path=str(workspace.databases.position),
        )
    finally:
        connection.close()


def _load_portfolio_plan_run_record(
    workspace: WorkspaceRoots,
    *,
    portfolio_id: str,
    snapshot_date: date,
    signal_start_date: date | None,
    signal_end_date: date | None,
) -> _ChildRunRecord:
    return _load_portfolio_or_trade_run_record(
        db_path=workspace.databases.portfolio_plan,
        child_module="portfolio_plan",
        run_table="portfolio_plan_run",
        portfolio_id=portfolio_id,
        snapshot_date=snapshot_date,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        runner_name_column="runner_name",
        runner_version_column="runner_version",
        contract_version_column="portfolio_plan_contract_version",
    )


def _load_trade_run_record(
    workspace: WorkspaceRoots,
    *,
    portfolio_id: str,
    snapshot_date: date | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
) -> _ChildRunRecord:
    return _load_portfolio_or_trade_run_record(
        db_path=workspace.databases.trade_runtime,
        child_module="trade",
        run_table="trade_run",
        portfolio_id=portfolio_id,
        snapshot_date=snapshot_date,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        runner_name_column="runner_name",
        runner_version_column="runner_version",
        contract_version_column="trade_contract_version",
    )


def _load_standard_child_run_record(
    *,
    db_path: Path,
    child_module: str,
    run_table: str,
    runner_name_column: str,
    runner_version_column: str,
    contract_version_column: str,
    started_at_column: str,
    completed_at_column: str,
    summary_column: str,
    snapshot_date: date,
) -> _ChildRunRecord:
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        row = connection.execute(
            f"""
            SELECT
                run_id,
                {runner_name_column},
                {runner_version_column},
                run_status,
                signal_start_date,
                signal_end_date,
                {contract_version_column},
                {started_at_column},
                {completed_at_column},
                {summary_column}
            FROM {run_table}
            WHERE run_status = 'completed'
              AND COALESCE(signal_end_date, signal_start_date) <= ?
            ORDER BY COALESCE(signal_end_date, signal_start_date) DESC, {completed_at_column} DESC, run_id DESC
            LIMIT 1
            """,
            [snapshot_date],
        ).fetchone()
        if row is None:
            raise ValueError(f"Missing completed child run for `{child_module}`.")
        return _ChildRunRecord(
            child_module=child_module,
            child_run_id=str(row[0]),
            child_run_status=_normalize_optional_str(row[3]).lower(),
            child_runner_name=_normalize_optional_str(row[1]),
            child_runner_version=_normalize_optional_str(row[2]),
            child_contract_version=None if row[6] is None else str(row[6]),
            child_signal_start_date=_normalize_date_value(row[4], allow_none=True),
            child_signal_end_date=_normalize_date_value(row[5], allow_none=True),
            child_started_at=_normalize_datetime_value(row[7]),
            child_completed_at=_normalize_datetime_value(row[8]),
            child_summary_json=_normalize_summary_text(row[9], fallback_payload={}),
            child_ledger_path=str(db_path),
        )
    finally:
        connection.close()


def _load_portfolio_or_trade_run_record(
    *,
    db_path: Path,
    child_module: str,
    run_table: str,
    portfolio_id: str,
    snapshot_date: date | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    runner_name_column: str,
    runner_version_column: str,
    contract_version_column: str,
) -> _ChildRunRecord:
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        where_clauses = ["run_status = 'completed'", "portfolio_id = ?"]
        parameters: list[object] = [portfolio_id]
        if signal_start_date is not None:
            where_clauses.append("signal_start_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("signal_end_date <= ?")
            parameters.append(signal_end_date)
        if snapshot_date is not None:
            where_clauses.append("COALESCE(signal_end_date, signal_start_date) <= ?")
            parameters.append(snapshot_date)
        row = connection.execute(
            f"""
            SELECT
                run_id,
                {runner_name_column},
                {runner_version_column},
                run_status,
                signal_start_date,
                signal_end_date,
                {contract_version_column},
                started_at,
                completed_at,
                summary_json
            FROM {run_table}
            WHERE {" AND ".join(where_clauses)}
            ORDER BY COALESCE(signal_end_date, signal_start_date) DESC, completed_at DESC, run_id DESC
            LIMIT 1
            """,
            parameters,
        ).fetchone()
        if row is None:
            raise ValueError(f"Missing completed child run for `{child_module}`.")
        return _ChildRunRecord(
            child_module=child_module,
            child_run_id=str(row[0]),
            child_run_status=_normalize_optional_str(row[3]).lower(),
            child_runner_name=_normalize_optional_str(row[1]),
            child_runner_version=_normalize_optional_str(row[2]),
            child_contract_version=None if row[6] is None else str(row[6]),
            child_signal_start_date=_normalize_date_value(row[4], allow_none=True),
            child_signal_end_date=_normalize_date_value(row[5], allow_none=True),
            child_started_at=_normalize_datetime_value(row[7]),
            child_completed_at=_normalize_datetime_value(row[8]),
            child_summary_json=_normalize_summary_text(row[9], fallback_payload={}),
            child_ledger_path=str(db_path),
        )
    finally:
        connection.close()


def _build_mainline_snapshot_row(
    *,
    trade_runtime_path: Path,
    portfolio_id: str,
    snapshot_date: date,
    child_runs: list[_ChildRunRecord],
    system_scene: str,
    system_contract_version: str,
    run_id: str,
    source_portfolio_plan_run_id: str,
    source_trade_run_id: str,
) -> _MainlineSnapshotRow:
    planned_entry_count, blocked_upstream_count, planned_carry_count = _load_trade_status_counts(
        trade_runtime_path=trade_runtime_path,
        portfolio_id=portfolio_id,
        snapshot_date=snapshot_date,
    )
    carried_open_leg_count = _load_open_leg_count(
        trade_runtime_path=trade_runtime_path,
        portfolio_id=portfolio_id,
    )
    current_carry_weight = _load_current_carry_weight(
        trade_runtime_path=trade_runtime_path,
        portfolio_id=portfolio_id,
        snapshot_date=snapshot_date,
    )
    acceptance_status = _resolve_acceptance_status(
        planned_entry_count=planned_entry_count,
        blocked_upstream_count=blocked_upstream_count,
        planned_carry_count=planned_carry_count,
        carried_open_leg_count=carried_open_leg_count,
        current_carry_weight=current_carry_weight,
    )
    acceptance_note = (
        f"planned_entry={planned_entry_count}; "
        f"blocked_upstream={blocked_upstream_count}; "
        f"planned_carry={planned_carry_count}; "
        f"open_leg={carried_open_leg_count}; "
        f"current_carry_weight={current_carry_weight:.6f}"
    )
    return _MainlineSnapshotRow(
        mainline_snapshot_nk=_build_mainline_snapshot_nk(
            portfolio_id=portfolio_id,
            snapshot_date=snapshot_date,
            system_scene=system_scene,
            system_contract_version=system_contract_version,
        ),
        portfolio_id=portfolio_id,
        snapshot_date=snapshot_date,
        system_scene=system_scene,
        acceptance_status=acceptance_status,
        acceptance_note=acceptance_note,
        planned_entry_count=planned_entry_count,
        blocked_upstream_count=blocked_upstream_count,
        planned_carry_count=planned_carry_count,
        carried_open_leg_count=carried_open_leg_count,
        current_carry_weight=current_carry_weight,
        included_child_run_count=len(child_runs),
        source_portfolio_plan_run_id=source_portfolio_plan_run_id,
        source_trade_run_id=source_trade_run_id,
        system_contract_version=system_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _load_trade_status_counts(
    *,
    trade_runtime_path: Path,
    portfolio_id: str,
    snapshot_date: date,
) -> tuple[int, int, int]:
    connection = duckdb.connect(str(trade_runtime_path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE execution_status = 'planned_entry') AS planned_entry_count,
                COUNT(*) FILTER (WHERE execution_status = 'blocked_upstream') AS blocked_upstream_count,
                COUNT(*) FILTER (WHERE execution_status = 'planned_carry') AS planned_carry_count
            FROM trade_execution_plan
            WHERE portfolio_id = ?
              AND signal_date = ?
            """,
            [portfolio_id, snapshot_date],
        ).fetchone()
        return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
    finally:
        connection.close()


def _load_open_leg_count(
    *,
    trade_runtime_path: Path,
    portfolio_id: str,
) -> int:
    connection = duckdb.connect(str(trade_runtime_path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM trade_position_leg
            WHERE portfolio_id = ?
              AND leg_status = 'open'
              AND carry_eligible = TRUE
            """,
            [portfolio_id],
        ).fetchone()
        return int(row[0] or 0)
    finally:
        connection.close()


def _load_current_carry_weight(
    *,
    trade_runtime_path: Path,
    portfolio_id: str,
    snapshot_date: date,
) -> float:
    connection = duckdb.connect(str(trade_runtime_path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT COALESCE(SUM(current_position_weight), 0)
            FROM trade_carry_snapshot
            WHERE portfolio_id = ?
              AND snapshot_date = ?
            """,
            [portfolio_id, snapshot_date],
        ).fetchone()
        return float(row[0] or 0.0)
    finally:
        connection.close()


def _resolve_acceptance_status(
    *,
    planned_entry_count: int,
    blocked_upstream_count: int,
    planned_carry_count: int,
    carried_open_leg_count: int,
    current_carry_weight: float,
) -> str:
    if planned_entry_count > 0:
        return "planned_entry_ready"
    if planned_carry_count > 0 or carried_open_leg_count > 0 or current_carry_weight > 0:
        return "carry_forward_only"
    if blocked_upstream_count > 0:
        return "blocked_upstream_only"
    return "idle"


def _build_mainline_snapshot_nk(
    *,
    portfolio_id: str,
    snapshot_date: date,
    system_scene: str,
    system_contract_version: str,
) -> str:
    return "|".join(
        [
            portfolio_id,
            snapshot_date.isoformat(),
            system_scene,
            system_contract_version,
        ]
    )


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    portfolio_id: str,
    snapshot_date: date,
    signal_start_date: date | None,
    signal_end_date: date | None,
    system_scene: str,
    system_contract_version: str,
    bounded_child_run_count: int,
    planned_entry_count: int,
    blocked_upstream_count: int,
    planned_carry_count: int,
    carried_open_leg_count: int,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {SYSTEM_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            portfolio_id,
            snapshot_date,
            signal_start_date,
            signal_end_date,
            system_scene,
            system_contract_version,
            bounded_child_run_count,
            planned_entry_count,
            blocked_upstream_count,
            planned_carry_count,
            carried_open_leg_count
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            portfolio_id,
            snapshot_date,
            signal_start_date,
            signal_end_date,
            system_scene,
            system_contract_version,
            bounded_child_run_count,
            planned_entry_count,
            blocked_upstream_count,
            planned_carry_count,
            carried_open_leg_count,
        ],
    )


def _upsert_child_run_readout(
    connection: duckdb.DuckDBPyConnection,
    *,
    child_run: _ChildRunRecord,
    run_id: str,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            child_run_status,
            child_runner_name,
            child_runner_version,
            child_contract_version,
            child_signal_start_date,
            child_signal_end_date,
            child_started_at,
            child_completed_at,
            child_summary_json,
            child_ledger_path,
            first_seen_run_id
        FROM {SYSTEM_CHILD_RUN_READOUT_TABLE}
        WHERE child_run_readout_nk = ?
        """,
        [child_run.child_run_readout_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {SYSTEM_CHILD_RUN_READOUT_TABLE} (
                child_run_readout_nk,
                child_module,
                child_run_id,
                child_run_status,
                child_runner_name,
                child_runner_version,
                child_contract_version,
                child_signal_start_date,
                child_signal_end_date,
                child_started_at,
                child_completed_at,
                child_summary_json,
                child_ledger_path,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                child_run.child_run_readout_nk,
                child_run.child_module,
                child_run.child_run_id,
                child_run.child_run_status,
                child_run.child_runner_name,
                child_run.child_runner_version,
                child_run.child_contract_version,
                child_run.child_signal_start_date,
                child_run.child_signal_end_date,
                child_run.child_started_at,
                child_run.child_completed_at,
                child_run.child_summary_json,
                child_run.child_ledger_path,
                run_id,
                run_id,
            ],
        )
        return "inserted"

    current_payload = (
        _normalize_optional_str(existing_row[0]),
        _normalize_optional_str(existing_row[1]),
        _normalize_optional_str(existing_row[2]),
        None if existing_row[3] is None else str(existing_row[3]),
        _normalize_date_value(existing_row[4], allow_none=True),
        _normalize_date_value(existing_row[5], allow_none=True),
        _normalize_datetime_value(existing_row[6]),
        _normalize_datetime_value(existing_row[7]),
        _normalize_summary_text(existing_row[8], fallback_payload={}),
        _normalize_optional_str(existing_row[9]),
    )
    next_payload = (
        child_run.child_run_status,
        child_run.child_runner_name,
        child_run.child_runner_version,
        child_run.child_contract_version,
        child_run.child_signal_start_date,
        child_run.child_signal_end_date,
        child_run.child_started_at,
        child_run.child_completed_at,
        child_run.child_summary_json,
        child_run.child_ledger_path,
    )
    first_seen_run_id = str(existing_row[10]) if existing_row[10] is not None else run_id
    connection.execute(
        f"""
        UPDATE {SYSTEM_CHILD_RUN_READOUT_TABLE}
        SET
            child_run_status = ?,
            child_runner_name = ?,
            child_runner_version = ?,
            child_contract_version = ?,
            child_signal_start_date = ?,
            child_signal_end_date = ?,
            child_started_at = ?,
            child_completed_at = ?,
            child_summary_json = ?,
            child_ledger_path = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE child_run_readout_nk = ?
        """,
        [
            child_run.child_run_status,
            child_run.child_runner_name,
            child_run.child_runner_version,
            child_run.child_contract_version,
            child_run.child_signal_start_date,
            child_run.child_signal_end_date,
            child_run.child_started_at,
            child_run.child_completed_at,
            child_run.child_summary_json,
            child_run.child_ledger_path,
            first_seen_run_id,
            run_id,
            child_run.child_run_readout_nk,
        ],
    )
    if current_payload == next_payload:
        return "reused"
    return "rematerialized"


def _upsert_mainline_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    snapshot_row: _MainlineSnapshotRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            acceptance_status,
            acceptance_note,
            planned_entry_count,
            blocked_upstream_count,
            planned_carry_count,
            carried_open_leg_count,
            current_carry_weight,
            included_child_run_count,
            source_portfolio_plan_run_id,
            source_trade_run_id,
            first_seen_run_id
        FROM {SYSTEM_MAINLINE_SNAPSHOT_TABLE}
        WHERE mainline_snapshot_nk = ?
        """,
        [snapshot_row.mainline_snapshot_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {SYSTEM_MAINLINE_SNAPSHOT_TABLE} (
                mainline_snapshot_nk,
                portfolio_id,
                snapshot_date,
                system_scene,
                acceptance_status,
                acceptance_note,
                planned_entry_count,
                blocked_upstream_count,
                planned_carry_count,
                carried_open_leg_count,
                current_carry_weight,
                included_child_run_count,
                source_portfolio_plan_run_id,
                source_trade_run_id,
                system_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                snapshot_row.mainline_snapshot_nk,
                snapshot_row.portfolio_id,
                snapshot_row.snapshot_date,
                snapshot_row.system_scene,
                snapshot_row.acceptance_status,
                snapshot_row.acceptance_note,
                snapshot_row.planned_entry_count,
                snapshot_row.blocked_upstream_count,
                snapshot_row.planned_carry_count,
                snapshot_row.carried_open_leg_count,
                snapshot_row.current_carry_weight,
                snapshot_row.included_child_run_count,
                snapshot_row.source_portfolio_plan_run_id,
                snapshot_row.source_trade_run_id,
                snapshot_row.system_contract_version,
                snapshot_row.first_seen_run_id,
                snapshot_row.last_materialized_run_id,
            ],
        )
        return "inserted"

    current_payload = (
        _normalize_optional_str(existing_row[0]),
        _normalize_optional_str(existing_row[1]),
        int(existing_row[2] or 0),
        int(existing_row[3] or 0),
        int(existing_row[4] or 0),
        int(existing_row[5] or 0),
        float(existing_row[6] or 0.0),
        int(existing_row[7] or 0),
        _normalize_optional_str(existing_row[8]),
        _normalize_optional_str(existing_row[9]),
    )
    next_payload = (
        snapshot_row.acceptance_status,
        snapshot_row.acceptance_note,
        snapshot_row.planned_entry_count,
        snapshot_row.blocked_upstream_count,
        snapshot_row.planned_carry_count,
        snapshot_row.carried_open_leg_count,
        snapshot_row.current_carry_weight,
        snapshot_row.included_child_run_count,
        snapshot_row.source_portfolio_plan_run_id,
        snapshot_row.source_trade_run_id,
    )
    first_seen_run_id = str(existing_row[10]) if existing_row[10] is not None else snapshot_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {SYSTEM_MAINLINE_SNAPSHOT_TABLE}
        SET
            acceptance_status = ?,
            acceptance_note = ?,
            planned_entry_count = ?,
            blocked_upstream_count = ?,
            planned_carry_count = ?,
            carried_open_leg_count = ?,
            current_carry_weight = ?,
            included_child_run_count = ?,
            source_portfolio_plan_run_id = ?,
            source_trade_run_id = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE mainline_snapshot_nk = ?
        """,
        [
            snapshot_row.acceptance_status,
            snapshot_row.acceptance_note,
            snapshot_row.planned_entry_count,
            snapshot_row.blocked_upstream_count,
            snapshot_row.planned_carry_count,
            snapshot_row.carried_open_leg_count,
            snapshot_row.current_carry_weight,
            snapshot_row.included_child_run_count,
            snapshot_row.source_portfolio_plan_run_id,
            snapshot_row.source_trade_run_id,
            first_seen_run_id,
            snapshot_row.last_materialized_run_id,
            snapshot_row.mainline_snapshot_nk,
        ],
    )
    if current_payload == next_payload:
        return "reused"
    return "rematerialized"


def _resolve_system_materialization_action(
    *,
    snapshot_action: str,
    child_readout_inserted_count: int,
    child_readout_reused_count: int,
    child_readout_rematerialized_count: int,
) -> str:
    if (
        snapshot_action == "reused"
        and child_readout_inserted_count == 0
        and child_readout_rematerialized_count == 0
        and child_readout_reused_count >= 0
    ):
        return "reused"
    if (
        snapshot_action == "inserted"
        and child_readout_reused_count == 0
        and child_readout_rematerialized_count == 0
    ):
        return "inserted"
    return "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: SystemMainlineReadoutSummary,
) -> None:
    _update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        system_materialization_action=summary.system_materialization_action,
        child_readout_inserted_count=summary.child_readout_inserted_count,
        child_readout_reused_count=summary.child_readout_reused_count,
        child_readout_rematerialized_count=summary.child_readout_rematerialized_count,
        snapshot_inserted_count=summary.snapshot_inserted_count,
        snapshot_reused_count=summary.snapshot_reused_count,
        snapshot_rematerialized_count=summary.snapshot_rematerialized_count,
        summary_payload=summary.as_dict(),
    )


def _update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    system_materialization_action: str,
    child_readout_inserted_count: int,
    child_readout_reused_count: int,
    child_readout_rematerialized_count: int,
    snapshot_inserted_count: int,
    snapshot_reused_count: int,
    snapshot_rematerialized_count: int,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {SYSTEM_RUN_TABLE}
        SET
            run_status = ?,
            system_materialization_action = ?,
            child_readout_inserted_count = ?,
            child_readout_reused_count = ?,
            child_readout_rematerialized_count = ?,
            snapshot_inserted_count = ?,
            snapshot_reused_count = ?,
            snapshot_rematerialized_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            system_materialization_action,
            child_readout_inserted_count,
            child_readout_reused_count,
            child_readout_rematerialized_count,
            snapshot_inserted_count,
            snapshot_reused_count,
            snapshot_rematerialized_count,
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def _normalize_summary_text(value: object, *, fallback_payload: dict[str, object]) -> str:
    if value is None:
        return json.dumps(fallback_payload, ensure_ascii=False, sort_keys=True)
    raw = str(value).strip()
    if not raw:
        return json.dumps(fallback_payload, ensure_ascii=False, sort_keys=True)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = {"notes": raw, **fallback_payload}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _normalize_optional_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_date_value(value: object, *, allow_none: bool = False) -> date | None:
    if value is None:
        if allow_none:
            return None
        raise ValueError("Missing required date value.")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_datetime_value(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _write_summary(summary: SystemMainlineReadoutSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
