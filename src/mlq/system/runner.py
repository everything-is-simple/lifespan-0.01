"""执行 `system` 主链 bounded acceptance readout / audit 物化。"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.system.bootstrap import (
    SYSTEM_RUN_SNAPSHOT_TABLE,
    bootstrap_system_ledger,
    system_ledger_path,
)
from mlq.system.readout_children import (
    _load_alpha_formal_signal_run_record,
    _load_alpha_trigger_run_record,
    _load_filter_run_record,
    _load_portfolio_plan_run_record,
    _load_position_run_record,
    _load_structure_run_record,
    _load_trade_run_record,
    _resolve_snapshot_date_from_trade_run,
)
from mlq.system.readout_materialization import (
    _insert_run_row,
    _mark_run_completed,
    _resolve_system_materialization_action,
    _update_run_summary,
    _upsert_child_run_readout,
    _upsert_mainline_snapshot,
    _write_summary,
)
from mlq.system.readout_shared import SystemMainlineReadoutSummary
from mlq.system.readout_snapshot import _build_mainline_snapshot_row


DEFAULT_SYSTEM_SCENE: Final[str] = "mainline_bounded_acceptance"
DEFAULT_SYSTEM_CONTRACT_VERSION: Final[str] = "system-mainline-readout-v1"


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
