"""沉淀 `filter snapshot` runner 的共享数据结构与基础归一化工具。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path


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
