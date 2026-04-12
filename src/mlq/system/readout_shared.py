"""沉淀 `system` mainline readout 共享数据结构与标准化工具。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime


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
