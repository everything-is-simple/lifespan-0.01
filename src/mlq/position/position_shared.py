"""承载 `position bootstrap` 共享常量与输入/输出数据结构。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class PositionFormalSignalInput:
    """描述 `alpha formal signal` 回接 `position` 的最小输入行。"""

    signal_nk: str
    instrument: str
    signal_date: str
    asof_date: str
    trigger_family: str
    trigger_type: str
    pattern_code: str
    formal_signal_status: str
    trigger_admissible: bool
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int
    source_trigger_event_nk: str
    signal_contract_version: str
    reference_trade_date: str
    reference_price: float | None = None
    capital_base_value: float | None = None
    current_position_weight: float = 0.0
    remaining_single_name_capacity_weight: float | None = None
    remaining_portfolio_capacity_weight: float | None = None
    blocked_reason_code: str | None = None
    audit_note: str | None = None
    source_signal_run_id: str | None = None
    source_family_event_nk: str | None = None
    source_family_contract_version: str | None = None
    family_code: str | None = None
    family_role: str | None = None
    family_bias: str | None = None
    malf_alignment: str | None = None
    malf_phase_bucket: str | None = None
    family_source_context_fingerprint: str | None = None


@dataclass(frozen=True)
class PositionMaterializationSummary:
    """总结一次最小 materialization 的落表结果。"""

    run_id: str
    policy_id: str
    candidate_count: int
    admitted_count: int
    blocked_count: int
    sizing_count: int
    family_snapshot_count: int


def build_position_run_id() -> str:
    """生成稳定的 `position bootstrap` 审计运行标识。"""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"position-bootstrap-{timestamp}"


def resolve_signal_contract_version(
    formal_signals: list[PositionFormalSignalInput] | tuple[PositionFormalSignalInput, ...]
) -> str | None:
    """归并一次 materialization 输入里的 contract version。"""

    versions = {signal.signal_contract_version for signal in formal_signals if signal.signal_contract_version}
    if not versions:
        return None
    return ",".join(sorted(versions))


def resolve_signal_run_id(
    formal_signals: list[PositionFormalSignalInput] | tuple[PositionFormalSignalInput, ...]
) -> str | None:
    """归并一次 materialization 输入里的上游 run_id。"""

    run_ids = {signal.source_signal_run_id for signal in formal_signals if signal.source_signal_run_id}
    if not run_ids:
        return None
    return ",".join(sorted(run_ids))
