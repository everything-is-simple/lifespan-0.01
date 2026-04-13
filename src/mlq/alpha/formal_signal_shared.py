"""`alpha formal signal` 的共享常量、数据结构与纯函数。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final


DEFAULT_ALPHA_FORMAL_SIGNAL_TRIGGER_TABLE: Final[str] = "alpha_trigger_event"
DEFAULT_ALPHA_FORMAL_SIGNAL_FAMILY_TABLE: Final[str] = "alpha_family_event"
DEFAULT_ALPHA_FORMAL_SIGNAL_FILTER_TABLE: Final[str] = "filter_snapshot"
DEFAULT_ALPHA_FORMAL_SIGNAL_STRUCTURE_TABLE: Final[str] = "structure_snapshot"
DEFAULT_ALPHA_FORMAL_SIGNAL_CONTRACT_VERSION: Final[str] = "alpha-formal-signal-v3"


@dataclass(frozen=True)
class AlphaFormalSignalBuildSummary:
    """总结一次 `alpha formal signal` producer 的运行结果。"""

    run_id: str
    producer_name: str
    producer_version: str
    execution_mode: str
    signal_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    claimed_scope_count: int
    candidate_trigger_count: int
    materialized_signal_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    missing_context_count: int
    admitted_count: int
    blocked_count: int
    deferred_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    alpha_ledger_path: str
    filter_ledger_path: str
    structure_ledger_path: str
    source_trigger_table: str
    source_family_table: str
    source_filter_table: str
    source_structure_table: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _TriggerRow:
    source_trigger_event_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str


@dataclass(frozen=True)
class _ContextRow:
    instrument: str
    signal_date: date
    asof_date: date
    formal_signal_status: str
    trigger_admissible: bool
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_source_context_nk: str | None


@dataclass(frozen=True)
class _FamilyRow:
    source_family_event_nk: str
    source_trigger_event_nk: str
    family_code: str | None
    source_family_contract_version: str | None
    family_role: str | None
    family_bias: str | None
    malf_alignment: str | None
    malf_phase_bucket: str | None
    family_source_context_fingerprint: str | None


@dataclass(frozen=True)
class _FormalSignalEventRow:
    signal_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str
    formal_signal_status: str
    trigger_admissible: bool
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_source_context_nk: str | None
    source_family_event_nk: str | None
    family_code: str | None
    source_family_contract_version: str | None
    family_role: str | None
    family_bias: str | None
    malf_alignment: str | None
    malf_phase_bucket: str | None
    family_source_context_fingerprint: str | None
    source_trigger_event_nk: str
    signal_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


def _should_use_queue_execution(
    *,
    use_checkpoint_queue: bool | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> bool:
    if use_checkpoint_queue is not None:
        return use_checkpoint_queue
    return signal_start_date is None and signal_end_date is None and not instruments


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_alpha_formal_signal_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"alpha-formal-signal-{timestamp}"


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return "|".join([asset_type, code, timeframe])


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return f"{scope_nk}|{dirty_reason}"


def _build_signal_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    trigger_family: str,
    trigger_type: str,
    pattern_code: str,
    source_trigger_event_nk: str,
    signal_contract_version: str,
) -> str:
    return "|".join(
        [
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            trigger_family,
            trigger_type,
            pattern_code,
            source_trigger_event_nk,
            signal_contract_version,
        ]
    )


def _map_major_state_to_context_code(major_state: str) -> str:
    mapping = {
        "牛顺": "BULL_MAINSTREAM",
        "熊逆": "BULL_COUNTERTREND",
        "牛逆": "BEAR_COUNTERTREND",
        "熊顺": "BEAR_MAINSTREAM",
    }
    return mapping.get(major_state, "UNKNOWN")


def _derive_lifecycle_rank_high(
    *,
    malf_context_4: str,
    current_hh_count: int,
    current_ll_count: int,
) -> int:
    raw_rank = current_hh_count if malf_context_4.startswith("BULL_") else current_ll_count
    return max(0, min(raw_rank, 4))


def _normalize_formal_signal_status(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"admitted", "blocked", "deferred"}:
        return normalized
    if normalized in {"admit", "accepted"}:
        return "admitted"
    if normalized in {"reject", "rejected"}:
        return "blocked"
    return "blocked"


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


def _normalize_optional_int(value: object) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def _write_summary(summary: AlphaFormalSignalBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
