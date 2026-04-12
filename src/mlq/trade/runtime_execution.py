"""承接 `trade runtime` runner 的执行计划构造、carry 继承与汇总物化。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.trade.bootstrap import (
    TRADE_CARRY_SNAPSHOT_TABLE,
    TRADE_POSITION_LEG_TABLE,
    TRADE_RUN_EXECUTION_PLAN_TABLE,
)
from mlq.trade.runtime_materialization import (
    _upsert_carry_snapshot,
    _upsert_execution_plan,
    _upsert_position_leg,
)
from mlq.trade.runtime_shared import (
    DEFAULT_ENTRY_TIMING_POLICY,
    DEFAULT_FAST_FAILURE_POLICY,
    DEFAULT_RISK_UNIT_POLICY,
    DEFAULT_TAKE_PROFIT_POLICY,
    DEFAULT_TIME_STOP_POLICY,
    DEFAULT_TRAILING_STOP_POLICY,
    TradeRuntimeBuildSummary,
    _CarrySnapshotSeedRow,
    _PortfolioPlanBridgeRow,
    _TradeExecutionPlanRow,
    _normalize_date_value,
)
from mlq.trade.runtime_source import _load_next_trade_date


def _materialize_trade_runtime(
    *,
    connection: duckdb.DuckDBPyConnection,
    plan_rows: list[_PortfolioPlanBridgeRow],
    portfolio_id: str,
    market_base_path: Path,
    market_price_table: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    run_id: str,
    runner_name: str,
    runner_version: str,
    trade_contract_version: str,
    source_portfolio_plan_table: str,
    portfolio_plan_path: Path,
    trade_runtime_path: Path,
) -> TradeRuntimeBuildSummary:
    prior_carry_by_instrument = _load_latest_carry_snapshot_by_instrument(
        connection,
        portfolio_id=portfolio_id,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
    )
    plan_execution_rows = _build_plan_execution_rows(
        plan_rows=plan_rows,
        prior_carry_by_instrument=prior_carry_by_instrument,
        market_base_path=market_base_path,
        market_price_table=market_price_table,
        trade_contract_version=trade_contract_version,
        run_id=run_id,
    )
    plan_instruments = {row.instrument for row in plan_rows}
    carry_only_rows = _build_carry_forward_execution_rows(
        prior_carry_by_instrument=prior_carry_by_instrument,
        market_base_path=market_base_path,
        market_price_table=market_price_table,
        trade_contract_version=trade_contract_version,
        run_id=run_id,
        excluded_instruments=plan_instruments,
    )
    execution_rows = [*plan_execution_rows, *carry_only_rows]

    planned_entry_count = 0
    blocked_upstream_count = 0
    planned_carry_count = 0
    execution_plan_inserted_count = 0
    execution_plan_reused_count = 0
    execution_plan_rematerialized_count = 0
    position_leg_inserted_count = 0
    position_leg_reused_count = 0
    position_leg_rematerialized_count = 0

    touched_instruments = set(plan_instruments)
    carry_anchor_dates: dict[str, date] = {}
    for plan_row in plan_rows:
        carry_anchor_dates[plan_row.instrument] = plan_row.reference_trade_date

    for execution_row in execution_rows:
        action = _upsert_execution_plan(connection, execution_row=execution_row)
        connection.execute(
            f"""
            INSERT OR REPLACE INTO {TRADE_RUN_EXECUTION_PLAN_TABLE} (
                run_id,
                execution_plan_nk,
                execution_status,
                materialization_action
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                run_id,
                execution_row.execution_plan_nk,
                execution_row.execution_status,
                action,
            ],
        )
        if execution_row.execution_status == "planned_entry":
            planned_entry_count += 1
        elif execution_row.execution_status == "blocked_upstream":
            blocked_upstream_count += 1
        elif execution_row.execution_status == "planned_carry":
            planned_carry_count += 1
        if action == "inserted":
            execution_plan_inserted_count += 1
        elif action == "reused":
            execution_plan_reused_count += 1
        else:
            execution_plan_rematerialized_count += 1
        touched_instruments.add(execution_row.instrument)
        anchor_date = execution_row.signal_date
        if execution_row.execution_status == "planned_carry" and signal_end_date is not None:
            anchor_date = signal_end_date
        carry_anchor_dates[execution_row.instrument] = max(
            carry_anchor_dates.get(execution_row.instrument, anchor_date),
            anchor_date,
        )

        leg_action = _materialize_leg_for_execution_row(
            connection,
            execution_row=execution_row,
            prior_carry_by_instrument=prior_carry_by_instrument,
            run_id=run_id,
            trade_contract_version=trade_contract_version,
        )
        if leg_action == "inserted":
            position_leg_inserted_count += 1
        elif leg_action == "reused":
            position_leg_reused_count += 1
        elif leg_action == "rematerialized":
            position_leg_rematerialized_count += 1

    carry_snapshot_inserted_count = 0
    carry_snapshot_reused_count = 0
    carry_snapshot_rematerialized_count = 0
    for instrument in sorted(touched_instruments):
        carry_row = _build_carry_snapshot_row(
            connection,
            instrument=instrument,
            portfolio_id=portfolio_id,
            snapshot_date=carry_anchor_dates[instrument],
            prior_carry_row=prior_carry_by_instrument.get(instrument),
            trade_contract_version=trade_contract_version,
        )
        carry_action = _upsert_carry_snapshot(connection, carry_row=carry_row)
        if carry_action == "inserted":
            carry_snapshot_inserted_count += 1
        elif carry_action == "reused":
            carry_snapshot_reused_count += 1
        else:
            carry_snapshot_rematerialized_count += 1

    carried_open_leg_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {TRADE_POSITION_LEG_TABLE}
            WHERE portfolio_id = ?
              AND leg_status = 'open'
              AND carry_eligible = TRUE
            """,
            [portfolio_id],
        ).fetchone()[0]
    )

    return TradeRuntimeBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        portfolio_id=portfolio_id,
        trade_contract_version=trade_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_plan_count=len(plan_rows),
        planned_entry_count=planned_entry_count,
        blocked_upstream_count=blocked_upstream_count,
        planned_carry_count=planned_carry_count,
        carried_open_leg_count=carried_open_leg_count,
        execution_plan_inserted_count=execution_plan_inserted_count,
        execution_plan_reused_count=execution_plan_reused_count,
        execution_plan_rematerialized_count=execution_plan_rematerialized_count,
        position_leg_inserted_count=position_leg_inserted_count,
        position_leg_reused_count=position_leg_reused_count,
        position_leg_rematerialized_count=position_leg_rematerialized_count,
        carry_snapshot_inserted_count=carry_snapshot_inserted_count,
        carry_snapshot_reused_count=carry_snapshot_reused_count,
        carry_snapshot_rematerialized_count=carry_snapshot_rematerialized_count,
        source_portfolio_plan_table=source_portfolio_plan_table,
        portfolio_plan_ledger_path=str(portfolio_plan_path),
        market_base_path=str(market_base_path),
        trade_runtime_ledger_path=str(trade_runtime_path),
    )


def _load_latest_carry_snapshot_by_instrument(
    connection: duckdb.DuckDBPyConnection,
    *,
    portfolio_id: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
) -> dict[str, _CarrySnapshotSeedRow]:
    parameters: list[object] = [portfolio_id]
    date_filter = ""
    if signal_start_date is not None:
        date_filter = "AND snapshot_date < ?"
        parameters.append(signal_start_date)
    elif signal_end_date is not None:
        date_filter = "AND snapshot_date <= ?"
        parameters.append(signal_end_date)
    rows = connection.execute(
        f"""
        SELECT
            carry_snapshot_nk,
            snapshot_date,
            instrument,
            portfolio_id,
            current_position_weight,
            open_leg_count,
            carry_source_leg_nk,
            carry_source_run_id,
            carry_source_status
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY instrument
                    ORDER BY snapshot_date DESC, updated_at DESC, created_at DESC
                ) AS row_number_desc
            FROM {TRADE_CARRY_SNAPSHOT_TABLE}
            WHERE portfolio_id = ?
              {date_filter}
        ) AS ranked
        WHERE row_number_desc = 1
        """,
        parameters,
    ).fetchall()
    return {
        str(row[2]): _CarrySnapshotSeedRow(
            carry_snapshot_nk=str(row[0]),
            snapshot_date=_normalize_date_value(row[1], field_name="snapshot_date"),
            instrument=str(row[2]),
            portfolio_id=str(row[3]),
            current_position_weight=float(row[4] or 0.0),
            open_leg_count=int(row[5] or 0),
            carry_source_leg_nk=None if row[6] is None else str(row[6]),
            carry_source_run_id=None if row[7] is None else str(row[7]),
            carry_source_status="" if row[8] is None else str(row[8]).strip(),
        )
        for row in rows
    }


def _build_plan_execution_rows(
    *,
    plan_rows: list[_PortfolioPlanBridgeRow],
    prior_carry_by_instrument: dict[str, _CarrySnapshotSeedRow],
    market_base_path: Path,
    market_price_table: str,
    trade_contract_version: str,
    run_id: str,
) -> list[_TradeExecutionPlanRow]:
    rows: list[_TradeExecutionPlanRow] = []
    for plan_row in plan_rows:
        planned_entry_trade_date = _load_next_trade_date(
            market_base_path=market_base_path,
            market_price_table=market_price_table,
            instrument=plan_row.instrument,
            reference_trade_date=plan_row.reference_trade_date,
        )
        carry_source_status = _derive_carry_source_status(
            prior_carry_by_instrument.get(plan_row.instrument)
        )
        if plan_row.plan_status == "blocked":
            execution_action = "block_upstream"
            execution_status = "blocked_upstream"
            planned_entry_weight = 0.0
        elif plan_row.plan_status in {"admitted", "trimmed"} and plan_row.admitted_weight > 0:
            execution_action = "enter"
            execution_status = "planned_entry"
            planned_entry_weight = plan_row.admitted_weight
        else:
            execution_action = "block_upstream"
            execution_status = "blocked_upstream"
            planned_entry_weight = 0.0
        rows.append(
            _TradeExecutionPlanRow(
                execution_plan_nk=_build_execution_plan_nk(
                    plan_snapshot_nk=plan_row.plan_snapshot_nk,
                    planned_entry_trade_date=planned_entry_trade_date,
                    trade_contract_version=trade_contract_version,
                ),
                plan_snapshot_nk=plan_row.plan_snapshot_nk,
                candidate_nk=plan_row.candidate_nk,
                portfolio_id=plan_row.portfolio_id,
                instrument=plan_row.instrument,
                signal_date=plan_row.reference_trade_date,
                planned_entry_trade_date=planned_entry_trade_date,
                execution_action=execution_action,
                execution_status=execution_status,
                requested_weight=plan_row.requested_weight,
                planned_entry_weight=planned_entry_weight,
                trimmed_weight=plan_row.trimmed_weight,
                carry_source_status=carry_source_status,
                entry_timing_policy=DEFAULT_ENTRY_TIMING_POLICY,
                risk_unit_policy=DEFAULT_RISK_UNIT_POLICY,
                take_profit_policy=DEFAULT_TAKE_PROFIT_POLICY,
                fast_failure_policy=DEFAULT_FAST_FAILURE_POLICY,
                trailing_stop_policy=DEFAULT_TRAILING_STOP_POLICY,
                time_stop_policy=DEFAULT_TIME_STOP_POLICY,
                trade_contract_version=trade_contract_version,
                first_seen_run_id=run_id,
                last_materialized_run_id=run_id,
            )
        )
    return rows


def _build_carry_forward_execution_rows(
    *,
    prior_carry_by_instrument: dict[str, _CarrySnapshotSeedRow],
    market_base_path: Path,
    market_price_table: str,
    trade_contract_version: str,
    run_id: str,
    excluded_instruments: set[str],
) -> list[_TradeExecutionPlanRow]:
    rows: list[_TradeExecutionPlanRow] = []
    for instrument, carry_row in sorted(prior_carry_by_instrument.items()):
        if instrument in excluded_instruments:
            continue
        if carry_row.open_leg_count <= 0 or carry_row.current_position_weight <= 0:
            continue
        planned_entry_trade_date = _load_next_trade_date(
            market_base_path=market_base_path,
            market_price_table=market_price_table,
            instrument=instrument,
            reference_trade_date=carry_row.snapshot_date,
        )
        plan_snapshot_nk = f"carry::{carry_row.carry_snapshot_nk}"
        rows.append(
            _TradeExecutionPlanRow(
                execution_plan_nk=_build_execution_plan_nk(
                    plan_snapshot_nk=plan_snapshot_nk,
                    planned_entry_trade_date=planned_entry_trade_date,
                    trade_contract_version=trade_contract_version,
                ),
                plan_snapshot_nk=plan_snapshot_nk,
                candidate_nk=f"carry::{instrument}|{carry_row.snapshot_date.isoformat()}",
                portfolio_id=carry_row.portfolio_id,
                instrument=instrument,
                signal_date=carry_row.snapshot_date,
                planned_entry_trade_date=planned_entry_trade_date,
                execution_action="carry_forward",
                execution_status="planned_carry",
                requested_weight=carry_row.current_position_weight,
                planned_entry_weight=0.0,
                trimmed_weight=0.0,
                carry_source_status="retained_open_leg_ready",
                entry_timing_policy=DEFAULT_ENTRY_TIMING_POLICY,
                risk_unit_policy=DEFAULT_RISK_UNIT_POLICY,
                take_profit_policy=DEFAULT_TAKE_PROFIT_POLICY,
                fast_failure_policy=DEFAULT_FAST_FAILURE_POLICY,
                trailing_stop_policy=DEFAULT_TRAILING_STOP_POLICY,
                time_stop_policy=DEFAULT_TIME_STOP_POLICY,
                trade_contract_version=trade_contract_version,
                first_seen_run_id=run_id,
                last_materialized_run_id=run_id,
            )
        )
    return rows


def _derive_carry_source_status(carry_seed: _CarrySnapshotSeedRow | None) -> str:
    if carry_seed is None:
        return "no_prior_trade_run"
    if carry_seed.open_leg_count > 0 and carry_seed.current_position_weight > 0:
        return "retained_open_leg_ready"
    return "flat_after_prior_run"


def _build_execution_plan_nk(
    *,
    plan_snapshot_nk: str,
    planned_entry_trade_date: date,
    trade_contract_version: str,
) -> str:
    return "|".join(
        [
            plan_snapshot_nk,
            planned_entry_trade_date.isoformat(),
            trade_contract_version,
        ]
    )


def _materialize_leg_for_execution_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    execution_row: _TradeExecutionPlanRow,
    prior_carry_by_instrument: dict[str, _CarrySnapshotSeedRow],
    run_id: str,
    trade_contract_version: str,
) -> str | None:
    if execution_row.execution_action == "enter" and execution_row.planned_entry_weight > 0:
        return _upsert_position_leg(
            connection,
            position_leg_nk=f"{execution_row.execution_plan_nk}|core",
            execution_plan_nk=execution_row.execution_plan_nk,
            instrument=execution_row.instrument,
            portfolio_id=execution_row.portfolio_id,
            leg_role="core",
            entry_trade_date=execution_row.planned_entry_trade_date,
            entry_weight=execution_row.planned_entry_weight,
            remaining_weight=execution_row.planned_entry_weight,
            leg_status="open",
            carry_eligible=True,
            trade_contract_version=trade_contract_version,
            first_seen_run_id=run_id,
            last_materialized_run_id=run_id,
        )
    if execution_row.execution_action != "carry_forward":
        return None
    carry_seed = prior_carry_by_instrument.get(execution_row.instrument)
    if carry_seed is None or not carry_seed.carry_source_leg_nk:
        return None
    existing_leg = connection.execute(
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
        [carry_seed.carry_source_leg_nk],
    ).fetchone()
    if existing_leg is None:
        return None
    return _upsert_position_leg(
        connection,
        position_leg_nk=carry_seed.carry_source_leg_nk,
        execution_plan_nk=str(existing_leg[0]),
        instrument=str(existing_leg[1]),
        portfolio_id=str(existing_leg[2]),
        leg_role=str(existing_leg[3]),
        entry_trade_date=_normalize_date_value(existing_leg[4], field_name="entry_trade_date"),
        entry_weight=float(existing_leg[5] or 0.0),
        remaining_weight=float(existing_leg[6] or 0.0),
        leg_status="open",
        carry_eligible=bool(existing_leg[8]),
        trade_contract_version=trade_contract_version,
        first_seen_run_id=str(existing_leg[9]),
        last_materialized_run_id=run_id,
    )


def _build_carry_snapshot_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    instrument: str,
    portfolio_id: str,
    snapshot_date: date,
    prior_carry_row: _CarrySnapshotSeedRow | None,
    trade_contract_version: str,
) -> tuple[str, date, str, str, float, int, str | None, str | None, str, str]:
    open_leg_rows = connection.execute(
        f"""
        SELECT
            position_leg_nk,
            last_materialized_run_id,
            remaining_weight
        FROM {TRADE_POSITION_LEG_TABLE}
        WHERE portfolio_id = ?
          AND instrument = ?
          AND leg_status = 'open'
          AND carry_eligible = TRUE
        ORDER BY updated_at DESC, created_at DESC, position_leg_nk
        """,
        [portfolio_id, instrument],
    ).fetchall()
    if open_leg_rows:
        current_position_weight = float(sum(float(row[2] or 0.0) for row in open_leg_rows))
        carry_source_leg_nk = str(open_leg_rows[0][0])
        carry_source_run_id = None if open_leg_rows[0][1] is None else str(open_leg_rows[0][1])
        carry_source_status = "retained_open_leg_ready"
        open_leg_count = len(open_leg_rows)
    elif prior_carry_row is None:
        current_position_weight = 0.0
        carry_source_leg_nk = None
        carry_source_run_id = None
        carry_source_status = "no_prior_trade_run"
        open_leg_count = 0
    else:
        current_position_weight = 0.0
        carry_source_leg_nk = prior_carry_row.carry_source_leg_nk
        carry_source_run_id = prior_carry_row.carry_source_run_id
        carry_source_status = "flat_after_prior_run"
        open_leg_count = 0
    return (
        _build_carry_snapshot_nk(
            portfolio_id=portfolio_id,
            instrument=instrument,
            snapshot_date=snapshot_date,
            trade_contract_version=trade_contract_version,
        ),
        snapshot_date,
        instrument,
        portfolio_id,
        current_position_weight,
        open_leg_count,
        carry_source_leg_nk,
        carry_source_run_id,
        carry_source_status,
        trade_contract_version,
    )


def _build_carry_snapshot_nk(
    *,
    portfolio_id: str,
    instrument: str,
    snapshot_date: date,
    trade_contract_version: str,
) -> str:
    return "|".join([portfolio_id, instrument, snapshot_date.isoformat(), trade_contract_version])
