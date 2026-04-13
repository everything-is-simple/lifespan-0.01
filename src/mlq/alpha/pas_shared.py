"""alpha PAS 五触发 detector 的共享定义。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final


ALPHA_PAS_TRIGGER_SCOPE_ALL: Final[tuple[str, ...]] = ("bof", "tst", "pb", "cpb", "bpb")
DEFAULT_ALPHA_PAS_TRIGGER_FILTER_TABLE: Final[str] = "filter_snapshot"
DEFAULT_ALPHA_PAS_TRIGGER_STRUCTURE_TABLE: Final[str] = "structure_snapshot"
DEFAULT_ALPHA_PAS_TRIGGER_PRICE_TABLE: Final[str] = "stock_daily_adjusted"
DEFAULT_ALPHA_PAS_TRIGGER_ADJUST_METHOD: Final[str] = "backward"
DEFAULT_ALPHA_PAS_TRIGGER_TIMEFRAME: Final[str] = "D"
DEFAULT_ALPHA_PAS_TRIGGER_CONTRACT_VERSION: Final[str] = "alpha-pas-detector-v1"
PAS_MAX_HISTORY_BARS: Final[int] = 61
PAS_HISTORY_LOOKBACK_DAYS: Final[int] = 180

DEFAULT_ALPHA_PAS_FAMILY_CODE_BY_TRIGGER: Final[dict[str, str]] = {
    "bof": "bof_core",
    "tst": "tst_core",
    "pb": "pb_core",
    "cpb": "cpb_core",
    "bpb": "bpb_core",
}


@dataclass(frozen=True)
class AlphaPasTriggerBuildSummary:
    """官方 PAS detector bounded runner 的执行摘要。"""

    run_id: str
    runner_name: str
    runner_version: str
    execution_mode: str
    detector_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    claimed_scope_count: int
    evaluated_snapshot_count: int
    materialized_candidate_count: int
    skipped_pattern_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    family_counts: dict[str, int]
    alpha_ledger_path: str
    filter_ledger_path: str
    structure_ledger_path: str
    market_base_ledger_path: str
    source_filter_table: str
    source_structure_table: str
    source_price_table: str
    source_adjust_method: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _DetectorScopeRow:
    filter_snapshot_nk: str
    structure_snapshot_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_admissible: bool
    primary_blocking_condition: str | None
    break_confirmation_status: str | None
    break_confirmation_ref: str | None
    exhaustion_risk_bucket: str | None
    reversal_probability_bucket: str | None
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    structure_progress_state: str
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
class _CandidateRow:
    candidate_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str
    family_code: str
    trigger_strength: float
    detect_reason: str
    skip_reason: str | None
    price_context_json: str
    structure_context_json: str
    detector_trace_json: str
    source_filter_snapshot_nk: str
    source_structure_snapshot_nk: str
    source_price_fingerprint: str
    detector_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_date_value(value: object, *, field_name: str) -> date:
    if value is None:
        raise ValueError(f"Missing required date field: {field_name}")
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
    candidate = _normalize_optional_str(value)
    return candidate or None


def _normalize_optional_int(value: object, *, default: int = 0) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _to_python_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _build_alpha_pas_trigger_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"alpha-pas-trigger-{timestamp}"


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return "|".join((asset_type, code, timeframe))


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return "|".join((scope_nk, dirty_reason))


def _build_candidate_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    trigger_family: str,
    trigger_type: str,
    pattern_code: str,
    detector_contract_version: str,
) -> str:
    return "|".join(
        (
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            trigger_family,
            trigger_type,
            pattern_code,
            detector_contract_version,
        )
    )


def _price_history_window_start(signal_start_date: date | None) -> date | None:
    if signal_start_date is None:
        return None
    return signal_start_date - timedelta(days=PAS_HISTORY_LOOKBACK_DAYS)


def _write_summary(summary: AlphaPasTriggerBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
