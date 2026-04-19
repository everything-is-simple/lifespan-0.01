"""沉淀 `system` mainline readout 的落表与重用判定逻辑。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from mlq.system.bootstrap import (
    SYSTEM_CHILD_RUN_READOUT_TABLE,
    SYSTEM_MAINLINE_SNAPSHOT_TABLE,
    SYSTEM_RUN_TABLE,
)
from mlq.system.readout_shared import (
    SystemMainlineReadoutSummary,
    _ChildRunRecord,
    _MainlineSnapshotRow,
    _normalize_date_value,
    _normalize_datetime_value,
    _normalize_optional_str,
    _normalize_summary_text,
)


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    portfolio_id: str,
    snapshot_date: Any,
    signal_start_date: Any,
    signal_end_date: Any,
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


def _write_summary(summary: SystemMainlineReadoutSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
