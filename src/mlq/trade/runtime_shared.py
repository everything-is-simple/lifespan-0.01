"""定义 `trade runtime` runner 拆分后的共享常量、数据结构与归一化工具。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime


DEFAULT_MARKET_PRICE_ADJUST_METHOD = "none"
DEFAULT_ENTRY_TIMING_POLICY = "t_plus_1_open"
DEFAULT_RISK_UNIT_POLICY = "entry_open_minus_signal_low"
DEFAULT_TAKE_PROFIT_POLICY = "half_at_1r"
DEFAULT_FAST_FAILURE_POLICY = "t1_close_below_signal_low_then_t2_open_exit"
DEFAULT_TRAILING_STOP_POLICY = "break_last_higher_low"
DEFAULT_TIME_STOP_POLICY = "no_new_high_for_2_days_then_day_3_open_exit"


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
