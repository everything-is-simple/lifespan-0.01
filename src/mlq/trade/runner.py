"""执行 `portfolio_plan -> trade_runtime` 的 bounded 正式桥接。"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.trade.bootstrap import bootstrap_trade_runtime_ledger, trade_runtime_ledger_path
from mlq.trade.runtime_execution import _materialize_trade_runtime
from mlq.trade.runtime_materialization import (
    _insert_run_row,
    _mark_run_completed,
    _update_run_summary,
    _write_summary,
)
from mlq.trade.runtime_shared import (
    DEFAULT_ENTRY_TIMING_POLICY,
    DEFAULT_FAST_FAILURE_POLICY,
    DEFAULT_MARKET_PRICE_ADJUST_METHOD,
    DEFAULT_RISK_UNIT_POLICY,
    DEFAULT_TAKE_PROFIT_POLICY,
    DEFAULT_TIME_STOP_POLICY,
    DEFAULT_TRAILING_STOP_POLICY,
    TradeRuntimeBuildSummary,
)
from mlq.trade.runtime_source import _ensure_database_exists, _load_portfolio_plan_rows


DEFAULT_TRADE_CONTRACT_VERSION: Final[str] = "trade-runtime-v1"
DEFAULT_SOURCE_PORTFOLIO_PLAN_TABLE: Final[str] = "portfolio_plan_snapshot"
DEFAULT_MARKET_PRICE_TABLE: Final[str] = "stock_daily_adjusted"


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
