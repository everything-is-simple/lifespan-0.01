"""沉淀 `filter snapshot` runner 的共享数据结构与基础归一化工具。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final


FILTER_GATE_CODE_PRE_TRIGGER_PASSED: Final[str] = "pre_trigger_passed"
FILTER_GATE_CODE_PRE_TRIGGER_BLOCKED: Final[str] = "pre_trigger_blocked"
FILTER_GATE_CODES: Final[tuple[str, str]] = (
    FILTER_GATE_CODE_PRE_TRIGGER_PASSED,
    FILTER_GATE_CODE_PRE_TRIGGER_BLOCKED,
)

FILTER_REJECT_REASON_SECURITY_SUSPENDED: Final[str] = "security_suspended_or_unresumed"
FILTER_REJECT_REASON_SECURITY_RISK_WARNING: Final[str] = "security_risk_warning_excluded"
FILTER_REJECT_REASON_SECURITY_DELISTING: Final[str] = "security_delisting_arrangement"
FILTER_REJECT_REASON_SECURITY_TYPE_OUT_OF_UNIVERSE: Final[str] = "security_type_out_of_universe"
FILTER_REJECT_REASON_MARKET_TYPE_OUT_OF_UNIVERSE: Final[str] = "market_type_out_of_universe"
FILTER_REJECT_REASON_CODES: Final[tuple[str, ...]] = (
    FILTER_REJECT_REASON_SECURITY_SUSPENDED,
    FILTER_REJECT_REASON_SECURITY_RISK_WARNING,
    FILTER_REJECT_REASON_SECURITY_DELISTING,
    FILTER_REJECT_REASON_SECURITY_TYPE_OUT_OF_UNIVERSE,
    FILTER_REJECT_REASON_MARKET_TYPE_OUT_OF_UNIVERSE,
)
FILTER_ALLOWED_MARKET_TYPES: Final[tuple[str, ...]] = ("sh", "sz")
FILTER_ALLOWED_SECURITY_TYPES: Final[tuple[str, ...]] = ("stock", "equity", "a_share", "ashare")


@dataclass(frozen=True)
class FilterGateDecision:
    """冻结 `69` 后 filter 客观 gate 的正式裁决结果。"""

    trigger_admissible: bool
    filter_gate_code: str
    filter_reject_reason_code: str | None
    blocking_conditions: tuple[str, ...]


@dataclass(frozen=True)
class FilterSnapshotBuildSummary:
    """总结一次 `filter snapshot` producer 的运行结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    execution_mode: str
    filter_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    claimed_scope_count: int
    candidate_structure_count: int
    materialized_snapshot_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    missing_context_count: int
    admissible_count: int
    blocked_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    filter_ledger_path: str
    structure_ledger_path: str
    malf_ledger_path: str
    source_structure_table: str
    source_context_table: str
    source_timeframe: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _StructureSnapshotInputRow:
    structure_snapshot_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    daily_major_state: str | None
    daily_trend_direction: str | None
    daily_reversal_stage: str | None
    daily_wave_id: int | None
    daily_current_hh_count: int | None
    daily_current_ll_count: int | None
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_wave_id: int | None
    weekly_current_hh_count: int | None
    weekly_current_ll_count: int | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_wave_id: int | None
    monthly_current_hh_count: int | None
    monthly_current_ll_count: int | None
    monthly_source_context_nk: str | None
    structure_progress_state: str
    break_confirmation_status: str | None
    break_confirmation_ref: str | None
    stats_snapshot_nk: str | None
    exhaustion_risk_bucket: str | None
    reversal_probability_bucket: str | None
    source_context_nk: str


@dataclass(frozen=True)
class _ObjectiveStatusInputRow:
    instrument: str
    observed_trade_date: date
    market_type: str | None
    security_type: str | None
    suspension_status: str | None
    risk_warning_status: str | None
    delisting_status: str | None
    is_suspended_or_unresumed: bool
    is_risk_warning_excluded: bool
    is_delisting_arrangement: bool
    source_request_nk: str | None


@dataclass(frozen=True)
class _FilterSnapshotRow:
    filter_snapshot_nk: str
    structure_snapshot_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    daily_major_state: str | None
    daily_trend_direction: str | None
    daily_reversal_stage: str | None
    daily_wave_id: int | None
    daily_current_hh_count: int | None
    daily_current_ll_count: int | None
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_wave_id: int | None
    weekly_current_hh_count: int | None
    weekly_current_ll_count: int | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_wave_id: int | None
    monthly_current_hh_count: int | None
    monthly_current_ll_count: int | None
    monthly_source_context_nk: str | None
    trigger_admissible: bool
    filter_gate_code: str
    filter_reject_reason_code: str | None
    primary_blocking_condition: str | None
    blocking_conditions_json: str
    admission_notes: str | None
    break_confirmation_status: str | None
    break_confirmation_ref: str | None
    stats_snapshot_nk: str | None
    exhaustion_risk_bucket: str | None
    reversal_probability_bucket: str | None
    source_context_nk: str
    filter_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


def derive_filter_gate_decision(
    *,
    is_suspended_or_unresumed: bool = False,
    is_risk_warning_excluded: bool = False,
    is_delisting_arrangement: bool = False,
    is_security_type_out_of_universe: bool = False,
    is_market_type_out_of_universe: bool = False,
) -> FilterGateDecision:
    """`filter` 只允许拦客观可交易性与正式宇宙排除状态。"""

    blocking_conditions: list[str] = []
    if is_suspended_or_unresumed:
        blocking_conditions.append(FILTER_REJECT_REASON_SECURITY_SUSPENDED)
    if is_risk_warning_excluded:
        blocking_conditions.append(FILTER_REJECT_REASON_SECURITY_RISK_WARNING)
    if is_delisting_arrangement:
        blocking_conditions.append(FILTER_REJECT_REASON_SECURITY_DELISTING)
    if is_security_type_out_of_universe:
        blocking_conditions.append(FILTER_REJECT_REASON_SECURITY_TYPE_OUT_OF_UNIVERSE)
    if is_market_type_out_of_universe:
        blocking_conditions.append(FILTER_REJECT_REASON_MARKET_TYPE_OUT_OF_UNIVERSE)
    primary_reason = blocking_conditions[0] if blocking_conditions else None
    return FilterGateDecision(
        trigger_admissible=primary_reason is None,
        filter_gate_code=(
            FILTER_GATE_CODE_PRE_TRIGGER_PASSED
            if primary_reason is None
            else FILTER_GATE_CODE_PRE_TRIGGER_BLOCKED
        ),
        filter_reject_reason_code=primary_reason,
        blocking_conditions=tuple(blocking_conditions),
    )


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_filter_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"filter-snapshot-{timestamp}"


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return "|".join([asset_type, code, timeframe])


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return f"{scope_nk}|{dirty_reason}"


def _normalize_date_value(value: object, *, field_name: str) -> date:
    if value is None:
        raise ValueError(f"Missing required date field: {field_name}")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _to_python_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_progress_state(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"advancing", "stalled", "failed", "unknown"}:
        return normalized
    return "unknown"


def _normalize_optional_str(value: object, *, default: str = "") -> str:
    if value is None:
        return default
    candidate = str(value).strip()
    return candidate or default


def _normalize_optional_nullable_str(value: object) -> str | None:
    if value is None:
        return None
    candidate = str(value).strip()
    return candidate or None


def _normalize_optional_nullable_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _normalize_optional_int(value: object) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def _write_summary(summary: FilterSnapshotBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
