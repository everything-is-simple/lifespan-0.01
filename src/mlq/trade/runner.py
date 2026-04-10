"""执行 `portfolio_plan -> trade_runtime` 的 bounded 正式桥接。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.trade.bootstrap import (
    TRADE_CARRY_SNAPSHOT_TABLE,
    TRADE_EXECUTION_PLAN_TABLE,
    TRADE_POSITION_LEG_TABLE,
    TRADE_RUN_EXECUTION_PLAN_TABLE,
    TRADE_RUN_TABLE,
    bootstrap_trade_runtime_ledger,
    trade_runtime_ledger_path,
)


DEFAULT_TRADE_CONTRACT_VERSION: Final[str] = "trade-runtime-v1"
DEFAULT_SOURCE_PORTFOLIO_PLAN_TABLE: Final[str] = "portfolio_plan_snapshot"
DEFAULT_MARKET_PRICE_TABLE: Final[str] = "stock_daily_adjusted"
DEFAULT_MARKET_PRICE_ADJUST_METHOD: Final[str] = "none"
DEFAULT_ENTRY_TIMING_POLICY: Final[str] = "t_plus_1_open"
DEFAULT_RISK_UNIT_POLICY: Final[str] = "entry_open_minus_signal_low"
DEFAULT_TAKE_PROFIT_POLICY: Final[str] = "half_at_1r"
DEFAULT_FAST_FAILURE_POLICY: Final[str] = "t1_close_below_signal_low_then_t2_open_exit"
DEFAULT_TRAILING_STOP_POLICY: Final[str] = "break_last_higher_low"
DEFAULT_TIME_STOP_POLICY: Final[str] = "no_new_high_for_2_days_then_day_3_open_exit"


@dataclass(frozen=True)
class TradeRuntimeBuildSummary:
    """汇总一次 bounded `trade_runtime` 物化结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    portfolio_id: str
    trade_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_plan_count: int
    planned_entry_count: int
    blocked_upstream_count: int
    planned_carry_count: int
    carried_open_leg_count: int
    execution_plan_inserted_count: int
    execution_plan_reused_count: int
    execution_plan_rematerialized_count: int
    position_leg_inserted_count: int
    position_leg_reused_count: int
    position_leg_rematerialized_count: int
    carry_snapshot_inserted_count: int
    carry_snapshot_reused_count: int
    carry_snapshot_rematerialized_count: int
    source_portfolio_plan_table: str
    portfolio_plan_ledger_path: str
    market_base_path: str
    trade_runtime_ledger_path: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _PortfolioPlanBridgeRow:
    plan_snapshot_nk: str
    candidate_nk: str
    portfolio_id: str
    instrument: str
    reference_trade_date: date
    requested_weight: float
    admitted_weight: float
    trimmed_weight: float
    plan_status: str


@dataclass(frozen=True)
class _CarrySnapshotSeedRow:
    carry_snapshot_nk: str
    snapshot_date: date
    instrument: str
    portfolio_id: str
    current_position_weight: float
    open_leg_count: int
    carry_source_leg_nk: str | None
    carry_source_run_id: str | None
    carry_source_status: str


@dataclass(frozen=True)
class _TradeExecutionPlanRow:
    execution_plan_nk: str
    plan_snapshot_nk: str
    candidate_nk: str
    portfolio_id: str
    instrument: str
    signal_date: date
    planned_entry_trade_date: date
    execution_action: str
    execution_status: str
    requested_weight: float
    planned_entry_weight: float
    trimmed_weight: float
    carry_source_status: str
    entry_timing_policy: str
    risk_unit_policy: str
    take_profit_policy: str
    fast_failure_policy: str
    trailing_stop_policy: str
    time_stop_policy: str
    trade_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


def run_trade_runtime_build(
    *,
    portfolio_id: str,
    settings: WorkspaceRoots | None = None,
    portfolio_plan_path: Path | None = None,
    market_base_path: Path | None = None,
    trade_runtime_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    source_portfolio_plan_table: str = DEFAULT_SOURCE_PORTFOLIO_PLAN_TABLE,
    market_price_table: str = DEFAULT_MARKET_PRICE_TABLE,
    trade_contract_version: str = DEFAULT_TRADE_CONTRACT_VERSION,
    runner_name: str = "trade_runtime_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
) -> TradeRuntimeBuildSummary:
    """把官方 `portfolio_plan` 裁决物化为最小 `trade_runtime` 正式账本。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_limit = max(int(limit), 1)
    trade_run_id = run_id or _build_trade_run_id()

    resolved_portfolio_plan_path = Path(portfolio_plan_path or workspace.databases.portfolio_plan)
    resolved_market_base_path = Path(market_base_path or workspace.databases.market_base)
    resolved_trade_runtime_path = Path(trade_runtime_path or trade_runtime_ledger_path(workspace))

    _ensure_database_exists(resolved_portfolio_plan_path, label="portfolio_plan")
    _ensure_database_exists(resolved_market_base_path, label="market_base")

    connection = duckdb.connect(str(resolved_trade_runtime_path))
    try:
        bootstrap_trade_runtime_ledger(workspace, connection=connection)
        plan_rows = _load_portfolio_plan_rows(
            portfolio_plan_path=resolved_portfolio_plan_path,
            source_portfolio_plan_table=source_portfolio_plan_table,
            portfolio_id=portfolio_id,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=normalized_instruments,
            limit=normalized_limit,
        )
        _insert_run_row(
            connection,
            run_id=trade_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            portfolio_id=portfolio_id,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_plan_count=len(plan_rows),
            source_portfolio_plan_table=source_portfolio_plan_table,
            trade_contract_version=trade_contract_version,
        )
        summary = _materialize_trade_runtime(
            connection=connection,
            plan_rows=plan_rows,
            portfolio_id=portfolio_id,
            market_base_path=resolved_market_base_path,
            market_price_table=market_price_table,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            run_id=trade_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            trade_contract_version=trade_contract_version,
            source_portfolio_plan_table=source_portfolio_plan_table,
            portfolio_plan_path=resolved_portfolio_plan_path,
            trade_runtime_path=resolved_trade_runtime_path,
        )
        _mark_run_completed(connection, run_id=trade_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            connection,
            run_id=trade_run_id,
            run_status="failed",
            planned_entry_count=0,
            blocked_upstream_count=0,
            carried_open_leg_count=0,
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


def _build_trade_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"trade-runtime-{timestamp}"


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _load_portfolio_plan_rows(
    *,
    portfolio_plan_path: Path,
    source_portfolio_plan_table: str,
    portfolio_id: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_PortfolioPlanBridgeRow]:
    connection = duckdb.connect(str(portfolio_plan_path), read_only=True)
    try:
        _ensure_required_columns(
            connection,
            source_portfolio_plan_table,
            required_columns=(
                "plan_snapshot_nk",
                "candidate_nk",
                "portfolio_id",
                "instrument",
                "reference_trade_date",
                "requested_weight",
                "admitted_weight",
                "trimmed_weight",
                "plan_status",
            ),
        )
        parameters: list[object] = [portfolio_id]
        where_clauses: list[str] = ["portfolio_id = ?"]
        if signal_start_date is not None:
            where_clauses.append("reference_trade_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("reference_trade_date <= ?")
            parameters.append(signal_end_date)
        if instruments:
            placeholders = ", ".join("?" for _ in instruments)
            where_clauses.append(f"instrument IN ({placeholders})")
            parameters.extend(instruments)
        rows = connection.execute(
            f"""
            SELECT
                plan_snapshot_nk,
                candidate_nk,
                portfolio_id,
                instrument,
                reference_trade_date,
                requested_weight,
                admitted_weight,
                trimmed_weight,
                plan_status
            FROM {source_portfolio_plan_table}
            WHERE {" AND ".join(where_clauses)}
            ORDER BY reference_trade_date, instrument, candidate_nk
            LIMIT ?
            """,
            [*parameters, limit],
        ).fetchall()
        return [
            _PortfolioPlanBridgeRow(
                plan_snapshot_nk=str(row[0]),
                candidate_nk=str(row[1]),
                portfolio_id=str(row[2]),
                instrument=str(row[3]),
                reference_trade_date=_normalize_date_value(row[4], field_name="reference_trade_date"),
                requested_weight=float(row[5] or 0.0),
                admitted_weight=float(row[6] or 0.0),
                trimmed_weight=float(row[7] or 0.0),
                plan_status=_normalize_optional_str(row[8]).lower(),
            )
            for row in rows
        ]
    finally:
        connection.close()


def _ensure_required_columns(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    *,
    required_columns: tuple[str, ...],
) -> None:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchall()
    if not rows:
        raise ValueError(f"Missing table: {table_name}")
    available_columns = {str(row[0]) for row in rows}
    missing_columns = sorted(set(required_columns) - available_columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns in `{table_name}`: {', '.join(missing_columns)}"
        )


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
            carry_source_status=_normalize_optional_str(row[8]),
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


def _load_next_trade_date(
    *,
    market_base_path: Path,
    market_price_table: str,
    instrument: str,
    reference_trade_date: date,
) -> date:
    connection = duckdb.connect(str(market_base_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, market_price_table)
        instrument_column = _resolve_existing_column(
            available_columns,
            ("code", "instrument"),
            field_name="instrument",
            table_name=market_price_table,
        )
        parameters: list[object] = [instrument, reference_trade_date]
        adjust_filter_sql = ""
        if "adjust_method" in available_columns:
            adjust_filter_sql = "AND adjust_method = ?"
            parameters.append(DEFAULT_MARKET_PRICE_ADJUST_METHOD)
        row = connection.execute(
            f"""
            SELECT trade_date
            FROM {market_price_table}
            WHERE {instrument_column} = ?
              AND trade_date > ?
              AND open IS NOT NULL
              {adjust_filter_sql}
            ORDER BY trade_date
            LIMIT 1
            """,
            parameters,
        ).fetchone()
        if row is None:
            raise ValueError(
                f"Missing next trading day in `{market_price_table}` for {instrument} after {reference_trade_date.isoformat()}."
            )
        return _normalize_date_value(row[0], field_name="trade_date")
    finally:
        connection.close()


def _load_table_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchall()
    if not rows:
        raise ValueError(f"Missing table: {table_name}")
    return {str(row[0]) for row in rows}


def _resolve_existing_column(
    available_columns: set[str],
    candidates: tuple[str, ...],
    *,
    field_name: str,
    table_name: str,
) -> str:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    raise ValueError(f"Missing required column `{field_name}` in table `{table_name}`.")


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
    first_seen_run_id = str(existing_row[16]) if existing_row[16] is not None else execution_row.first_seen_run_id
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


def _write_summary(summary: TradeRuntimeBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
