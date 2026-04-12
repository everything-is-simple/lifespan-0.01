"""沉淀 `system` mainline readout 对上游 child run 的读取逻辑。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import WorkspaceRoots
from mlq.system.readout_shared import (
    _ChildRunRecord,
    _normalize_date_value,
    _normalize_datetime_value,
    _normalize_optional_str,
    _normalize_summary_text,
)


def _resolve_snapshot_date_from_trade_run(trade_run: _ChildRunRecord) -> date:
    if trade_run.child_signal_end_date is not None:
        return trade_run.child_signal_end_date
    if trade_run.child_signal_start_date is not None:
        return trade_run.child_signal_start_date
    if trade_run.child_completed_at is not None:
        return trade_run.child_completed_at.date()
    raise ValueError("Unable to resolve snapshot_date from trade run.")


def _load_structure_run_record(workspace: WorkspaceRoots, *, snapshot_date: date) -> _ChildRunRecord:
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


def _load_filter_run_record(workspace: WorkspaceRoots, *, snapshot_date: date) -> _ChildRunRecord:
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


def _load_alpha_trigger_run_record(workspace: WorkspaceRoots, *, snapshot_date: date) -> _ChildRunRecord:
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
