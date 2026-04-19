"""承接 `trade runtime` runner 的正式落表与 run summary 写回。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from mlq.trade.bootstrap import (
    TRADE_CARRY_SNAPSHOT_TABLE,
    TRADE_EXECUTION_PLAN_TABLE,
    TRADE_POSITION_LEG_TABLE,
    TRADE_RUN_TABLE,
)
from mlq.trade.runtime_shared import (
    TradeRuntimeBuildSummary,
    _TradeExecutionPlanRow,
    _normalize_date_value,
    _normalize_optional_str,
)


def _upsert_execution_plan(
    connection: duckdb.DuckDBPyConnection,
    *,
    execution_row: _TradeExecutionPlanRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            candidate_nk,
            portfolio_id,
            instrument,
            signal_date,
            execution_action,
            execution_status,
            requested_weight,
            planned_entry_weight,
            trimmed_weight,
            carry_source_status,
            entry_timing_policy,
            risk_unit_policy,
            take_profit_policy,
            fast_failure_policy,
            trailing_stop_policy,
            time_stop_policy,
            first_seen_run_id
        FROM {TRADE_EXECUTION_PLAN_TABLE}
        WHERE execution_plan_nk = ?
        """,
        [execution_row.execution_plan_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {TRADE_EXECUTION_PLAN_TABLE} (
                execution_plan_nk,
                plan_snapshot_nk,
                candidate_nk,
                portfolio_id,
                instrument,
                signal_date,
                planned_entry_trade_date,
                execution_action,
                execution_status,
                requested_weight,
                planned_entry_weight,
                trimmed_weight,
                carry_source_status,
                entry_timing_policy,
                risk_unit_policy,
                take_profit_policy,
                fast_failure_policy,
                trailing_stop_policy,
                time_stop_policy,
                trade_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                execution_row.execution_plan_nk,
                execution_row.plan_snapshot_nk,
                execution_row.candidate_nk,
                execution_row.portfolio_id,
                execution_row.instrument,
                execution_row.signal_date,
                execution_row.planned_entry_trade_date,
                execution_row.execution_action,
                execution_row.execution_status,
                execution_row.requested_weight,
                execution_row.planned_entry_weight,
                execution_row.trimmed_weight,
                execution_row.carry_source_status,
                execution_row.entry_timing_policy,
                execution_row.risk_unit_policy,
                execution_row.take_profit_policy,
                execution_row.fast_failure_policy,
                execution_row.trailing_stop_policy,
                execution_row.time_stop_policy,
                execution_row.trade_contract_version,
                execution_row.first_seen_run_id,
                execution_row.last_materialized_run_id,
            ],
        )
        return "inserted"

    current_payload = (
        _normalize_optional_str(existing_row[0]),
        _normalize_optional_str(existing_row[1]),
        _normalize_optional_str(existing_row[2]),
        _normalize_date_value(existing_row[3], field_name="signal_date"),
        _normalize_optional_str(existing_row[4]),
        _normalize_optional_str(existing_row[5]),
        float(existing_row[6] or 0.0),
        float(existing_row[7] or 0.0),
        float(existing_row[8] or 0.0),
        _normalize_optional_str(existing_row[9]),
        _normalize_optional_str(existing_row[10]),
        _normalize_optional_str(existing_row[11]),
        _normalize_optional_str(existing_row[12]),
        _normalize_optional_str(existing_row[13]),
        _normalize_optional_str(existing_row[14]),
        _normalize_optional_str(existing_row[15]),
    )
    next_payload = (
        execution_row.candidate_nk,
        execution_row.portfolio_id,
        execution_row.instrument,
        execution_row.signal_date,
        execution_row.execution_action,
        execution_row.execution_status,
        execution_row.requested_weight,
        execution_row.planned_entry_weight,
        execution_row.trimmed_weight,
        execution_row.carry_source_status,
        execution_row.entry_timing_policy,
        execution_row.risk_unit_policy,
        execution_row.take_profit_policy,
        execution_row.fast_failure_policy,
        execution_row.trailing_stop_policy,
        execution_row.time_stop_policy,
    )
    first_seen_run_id = (
        str(existing_row[16]) if existing_row[16] is not None else execution_row.first_seen_run_id
    )
    connection.execute(
        f"""
        UPDATE {TRADE_EXECUTION_PLAN_TABLE}
        SET
            plan_snapshot_nk = ?,
            candidate_nk = ?,
            portfolio_id = ?,
            instrument = ?,
            signal_date = ?,
            planned_entry_trade_date = ?,
            execution_action = ?,
            execution_status = ?,
            requested_weight = ?,
            planned_entry_weight = ?,
            trimmed_weight = ?,
            carry_source_status = ?,
            entry_timing_policy = ?,
            risk_unit_policy = ?,
            take_profit_policy = ?,
            fast_failure_policy = ?,
            trailing_stop_policy = ?,
            time_stop_policy = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE execution_plan_nk = ?
        """,
        [
            execution_row.plan_snapshot_nk,
            execution_row.candidate_nk,
            execution_row.portfolio_id,
            execution_row.instrument,
            execution_row.signal_date,
            execution_row.planned_entry_trade_date,
            execution_row.execution_action,
            execution_row.execution_status,
            execution_row.requested_weight,
            execution_row.planned_entry_weight,
            execution_row.trimmed_weight,
            execution_row.carry_source_status,
            execution_row.entry_timing_policy,
            execution_row.risk_unit_policy,
            execution_row.take_profit_policy,
            execution_row.fast_failure_policy,
            execution_row.trailing_stop_policy,
            execution_row.time_stop_policy,
            first_seen_run_id,
            execution_row.last_materialized_run_id,
            execution_row.execution_plan_nk,
        ],
    )
    if current_payload == next_payload:
        return "reused"
    return "rematerialized"


def _upsert_position_leg(
    connection: duckdb.DuckDBPyConnection,
    *,
    position_leg_nk: str,
    execution_plan_nk: str,
    instrument: str,
    portfolio_id: str,
    leg_role: str,
    entry_trade_date: date,
    entry_weight: float,
    remaining_weight: float,
    leg_status: str,
    carry_eligible: bool,
    trade_contract_version: str,
    first_seen_run_id: str,
    last_materialized_run_id: str,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            execution_plan_nk,
            instrument,
            portfolio_id,
            leg_role,
            entry_trade_date,
            entry_weight,
            remaining_weight,
            leg_status,
            carry_eligible,
            first_seen_run_id
        FROM {TRADE_POSITION_LEG_TABLE}
        WHERE position_leg_nk = ?
        """,
        [position_leg_nk],
    ).fetchone()
    next_payload = (
        execution_plan_nk,
        instrument,
        portfolio_id,
        leg_role,
        entry_trade_date,
        entry_weight,
        remaining_weight,
        leg_status,
        carry_eligible,
    )
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {TRADE_POSITION_LEG_TABLE} (
                position_leg_nk,
                execution_plan_nk,
                instrument,
                portfolio_id,
                leg_role,
                entry_trade_date,
                entry_weight,
                remaining_weight,
                leg_status,
                carry_eligible,
                trade_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                position_leg_nk,
                execution_plan_nk,
                instrument,
                portfolio_id,
                leg_role,
                entry_trade_date,
                entry_weight,
                remaining_weight,
                leg_status,
                carry_eligible,
                trade_contract_version,
                first_seen_run_id,
                last_materialized_run_id,
            ],
        )
        return "inserted"
    current_payload = (
        _normalize_optional_str(existing_row[0]),
        _normalize_optional_str(existing_row[1]),
        _normalize_optional_str(existing_row[2]),
        _normalize_optional_str(existing_row[3]),
        _normalize_date_value(existing_row[4], field_name="entry_trade_date"),
        float(existing_row[5] or 0.0),
        float(existing_row[6] or 0.0),
        _normalize_optional_str(existing_row[7]),
        bool(existing_row[8]),
    )
    connection.execute(
        f"""
        UPDATE {TRADE_POSITION_LEG_TABLE}
        SET
            execution_plan_nk = ?,
            instrument = ?,
            portfolio_id = ?,
            leg_role = ?,
            entry_trade_date = ?,
            entry_weight = ?,
            remaining_weight = ?,
            leg_status = ?,
            carry_eligible = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE position_leg_nk = ?
        """,
        [
            execution_plan_nk,
            instrument,
            portfolio_id,
            leg_role,
            entry_trade_date,
            entry_weight,
            remaining_weight,
            leg_status,
            carry_eligible,
            str(existing_row[9]) if existing_row[9] is not None else first_seen_run_id,
            last_materialized_run_id,
            position_leg_nk,
        ],
    )
    if current_payload == next_payload:
        return "reused"
    return "rematerialized"


def _upsert_carry_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    carry_row: tuple[str, date, str, str, float, int, str | None, str | None, str, str],
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            current_position_weight,
            open_leg_count,
            carry_source_leg_nk,
            carry_source_run_id,
            carry_source_status
        FROM {TRADE_CARRY_SNAPSHOT_TABLE}
        WHERE carry_snapshot_nk = ?
        """,
        [carry_row[0]],
    ).fetchone()
    next_payload = (carry_row[4], carry_row[5], carry_row[6], carry_row[7], carry_row[8])
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {TRADE_CARRY_SNAPSHOT_TABLE} (
                carry_snapshot_nk,
                snapshot_date,
                instrument,
                portfolio_id,
                current_position_weight,
                open_leg_count,
                carry_source_leg_nk,
                carry_source_run_id,
                carry_source_status,
                trade_contract_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            list(carry_row),
        )
        return "inserted"
    current_payload = (
        float(existing_row[0] or 0.0),
        int(existing_row[1] or 0),
        None if existing_row[2] is None else str(existing_row[2]),
        None if existing_row[3] is None else str(existing_row[3]),
        _normalize_optional_str(existing_row[4]),
    )
    connection.execute(
        f"""
        UPDATE {TRADE_CARRY_SNAPSHOT_TABLE}
        SET
            snapshot_date = ?,
            instrument = ?,
            portfolio_id = ?,
            current_position_weight = ?,
            open_leg_count = ?,
            carry_source_leg_nk = ?,
            carry_source_run_id = ?,
            carry_source_status = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE carry_snapshot_nk = ?
        """,
        [
            carry_row[1],
            carry_row[2],
            carry_row[3],
            carry_row[4],
            carry_row[5],
            carry_row[6],
            carry_row[7],
            carry_row[8],
            carry_row[0],
        ],
    )
    if current_payload == next_payload:
        return "reused"
    return "rematerialized"


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    portfolio_id: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_plan_count: int,
    source_portfolio_plan_table: str,
    trade_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {TRADE_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            portfolio_id,
            signal_start_date,
            signal_end_date,
            bounded_plan_count,
            source_portfolio_plan_table,
            trade_contract_version
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
            bounded_plan_count,
            source_portfolio_plan_table,
            trade_contract_version,
        ],
    )


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: TradeRuntimeBuildSummary,
) -> None:
    _update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        planned_entry_count=summary.planned_entry_count,
        blocked_upstream_count=summary.blocked_upstream_count,
        carried_open_leg_count=summary.carried_open_leg_count,
        summary_payload=summary.as_dict(),
    )


def _update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    planned_entry_count: int,
    blocked_upstream_count: int,
    carried_open_leg_count: int,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {TRADE_RUN_TABLE}
        SET
            run_status = ?,
            planned_entry_count = ?,
            blocked_upstream_count = ?,
            carried_open_leg_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            planned_entry_count,
            blocked_upstream_count,
            carried_open_leg_count,
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def _write_summary(summary: TradeRuntimeBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
