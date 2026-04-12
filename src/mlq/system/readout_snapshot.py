"""沉淀 `system` mainline readout 的聚合快照构建逻辑。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.system.readout_shared import _ChildRunRecord, _MainlineSnapshotRow


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
