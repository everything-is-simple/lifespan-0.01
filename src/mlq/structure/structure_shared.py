"""`structure snapshot` 的共享常量、数据结构与纯函数。"""

from __future__ import annotations

from bisect import bisect_right
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final


DEFAULT_STRUCTURE_CONTEXT_TABLE: Final[str] = "malf_state_snapshot"
DEFAULT_STRUCTURE_INPUT_TABLE: Final[str] = "malf_state_snapshot"
DEFAULT_STRUCTURE_BREAK_CONFIRMATION_TABLE: Final[str | None] = "pivot_confirmed_break_ledger"
DEFAULT_STRUCTURE_STATS_TABLE: Final[str | None] = "same_timeframe_stats_snapshot"
DEFAULT_STRUCTURE_SOURCE_TIMEFRAME: Final[str] = "D"
DEFAULT_STRUCTURE_CONTRACT_VERSION: Final[str] = "structure-snapshot-v2"


@dataclass(frozen=True)
class StructureSnapshotBuildSummary:
    """总结一次 `structure snapshot` producer 的运行结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    execution_mode: str
    structure_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    claimed_scope_count: int
    candidate_input_count: int
    materialized_snapshot_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    missing_context_count: int
    advancing_count: int
    stalled_count: int
    failed_count: int
    unknown_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    structure_ledger_path: str
    malf_ledger_path: str
    source_context_table: str
    source_structure_input_table: str
    source_break_confirmation_table: str | None
    source_stats_table: str | None
    source_timeframe: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _StructureInputRow:
    instrument: str
    signal_date: date
    asof_date: date
    new_high_count: int
    new_low_count: int
    refresh_density: float
    advancement_density: float
    is_failed_extreme: bool
    failure_type: str | None


@dataclass(frozen=True)
class _StructureContextRow:
    instrument: str
    signal_date: date
    asof_date: date
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    source_context_nk: str


@dataclass(frozen=True)
class _MultiTimeframeContextRow:
    daily: _StructureContextRow
    weekly: _StructureContextRow | None
    monthly: _StructureContextRow | None


@dataclass(frozen=True)
class _BreakConfirmationRow:
    instrument: str
    trigger_bar_dt: date
    confirmation_status: str
    break_event_nk: str


@dataclass(frozen=True)
class _StatsSnapshotRow:
    instrument: str
    signal_date: date
    asof_bar_dt: date
    stats_snapshot_nk: str
    exhaustion_risk_bucket: str | None
    reversal_probability_bucket: str | None


@dataclass(frozen=True)
class _StructureSnapshotRow:
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
    daily_major_state: str
    daily_trend_direction: str
    daily_reversal_stage: str
    daily_wave_id: int
    daily_current_hh_count: int
    daily_current_ll_count: int
    daily_source_context_nk: str
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
    structure_contract_version: str
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


def _normalize_timeframe(value: str | None) -> str:
    candidate = str(value or DEFAULT_STRUCTURE_SOURCE_TIMEFRAME).strip().upper()
    return candidate or DEFAULT_STRUCTURE_SOURCE_TIMEFRAME


def _build_structure_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"structure-snapshot-{timestamp}"


def _build_context_series_index(
    context_rows: list[_StructureContextRow],
) -> dict[str, tuple[list[date], list[_StructureContextRow]]]:
    index: dict[str, tuple[list[date], list[_StructureContextRow]]] = {}
    grouped_rows: dict[str, list[_StructureContextRow]] = {}
    for row in context_rows:
        grouped_rows.setdefault(row.instrument, []).append(row)
    for instrument, rows in grouped_rows.items():
        ordered_rows = sorted(rows, key=lambda item: item.asof_date)
        index[instrument] = ([row.asof_date for row in ordered_rows], ordered_rows)
    return index


def _lookup_latest_context_row(
    context_index: dict[str, tuple[list[date], list[_StructureContextRow]]],
    *,
    instrument: str,
    asof_date: date,
) -> _StructureContextRow | None:
    instrument_context = context_index.get(instrument)
    if instrument_context is None:
        return None
    asof_dates, rows = instrument_context
    matched_index = bisect_right(asof_dates, asof_date) - 1
    if matched_index < 0:
        return None
    return rows[matched_index]


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return f"{asset_type}|{code}|{timeframe}"


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return f"{scope_nk}|{dirty_reason}"


def _map_major_state_to_context_code(major_state: str) -> str:
    mapping = {
        "牛顺": "BULL_MAINSTREAM",
        "熊逆": "BULL_COUNTERTREND",
        "牛逆": "BEAR_COUNTERTREND",
        "熊顺": "BEAR_MAINSTREAM",
    }
    return mapping.get(major_state, "UNKNOWN")


def _derive_trend_direction_from_major_state(major_state: str) -> str:
    if major_state in {"牛顺", "熊逆"}:
        return "up"
    if major_state in {"牛逆", "熊顺"}:
        return "down"
    return "down"


def _derive_failure_type_from_major_state(major_state: str) -> str | None:
    if major_state == "熊顺":
        return "failed_breakdown"
    if major_state == "牛逆":
        return "failed_extreme"
    return None


def _build_canonical_context_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    major_state: str,
    trend_direction: str,
    reversal_stage: str,
    wave_id: int,
) -> str:
    return "|".join(
        [
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            major_state,
            trend_direction,
            reversal_stage,
            str(wave_id),
        ]
    )


def _build_structure_snapshot_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    source_context_nk: str,
    structure_contract_version: str,
) -> str:
    return "|".join(
        [
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            source_context_nk,
            structure_contract_version,
        ]
    )


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


def _normalize_optional_float(value: object) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def _write_summary(summary: StructureSnapshotBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
