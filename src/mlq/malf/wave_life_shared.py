"""`malf wave life` runner 的共享常量、类型与通用工具。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import pandas as pd

from mlq.malf.bootstrap import (
    MALF_SAME_LEVEL_STATS_TABLE,
    MALF_STATE_SNAPSHOT_TABLE,
    MALF_WAVE_LEDGER_TABLE,
)


DEFAULT_WAVE_LIFE_CONTRACT_VERSION: Final[str] = "malf-wave-life-v1"
DEFAULT_WAVE_LIFE_SAMPLE_VERSION: Final[str] = "wave-life-v1"
DEFAULT_WAVE_LIFE_METRIC_NAME: Final[str] = "wave_duration_bars"
DEFAULT_WAVE_LIFE_SOURCE_WAVE_TABLE: Final[str] = MALF_WAVE_LEDGER_TABLE
DEFAULT_WAVE_LIFE_SOURCE_STATE_TABLE: Final[str] = MALF_STATE_SNAPSHOT_TABLE
DEFAULT_WAVE_LIFE_SOURCE_STATS_TABLE: Final[str] = MALF_SAME_LEVEL_STATS_TABLE
DEFAULT_TIMEFRAMES: Final[tuple[str, ...]] = ("D", "W", "M")
SUPPORTED_TIMEFRAMES: Final[tuple[str, ...]] = ("D", "W", "M")


@dataclass(frozen=True)
class MalfWaveLifeBuildSummary:
    """汇总一次 `malf wave life` sidecar 物化运行。"""

    run_id: str
    runner_name: str
    runner_version: str
    execution_mode: str
    life_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_scope_count: int
    claimed_scope_count: int
    profile_row_count: int
    snapshot_row_count: int
    profile_inserted_count: int
    profile_reused_count: int
    profile_rematerialized_count: int
    snapshot_inserted_count: int
    snapshot_reused_count: int
    snapshot_rematerialized_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    active_snapshot_count: int
    completed_wave_sample_count: int
    fallback_profile_count: int
    malf_ledger_path: str
    source_wave_table: str
    source_state_table: str
    source_stats_table: str
    sample_version: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _WaveRow:
    wave_nk: str
    asset_type: str
    code: str
    timeframe: str
    wave_id: int
    major_state: str
    reversal_stage: str
    start_bar_dt: date
    end_bar_dt: date | None
    active_flag: bool
    bar_count: int


@dataclass(frozen=True)
class _StateRow:
    snapshot_nk: str
    asset_type: str
    code: str
    timeframe: str
    asof_bar_dt: date
    major_state: str
    reversal_stage: str
    wave_id: int


@dataclass(frozen=True)
class _ProfileMaterialization:
    profile_nk: str
    timeframe: str
    major_state: str
    reversal_stage: str
    metric_name: str
    sample_version: str
    sample_size: int
    profile_origin: str
    p10: float | None
    p25: float | None
    p50: float | None
    p75: float | None
    p90: float | None
    mean: float | None
    std: float | None
    source_stats_nk: str | None
    first_seen_run_id: str
    last_materialized_run_id: str
    sample_values: tuple[float, ...]

    def as_row(self) -> dict[str, object]:
        return {
            "profile_nk": self.profile_nk,
            "timeframe": self.timeframe,
            "major_state": self.major_state,
            "reversal_stage": self.reversal_stage,
            "metric_name": self.metric_name,
            "sample_version": self.sample_version,
            "sample_size": self.sample_size,
            "profile_origin": self.profile_origin,
            "p10": self.p10,
            "p25": self.p25,
            "p50": self.p50,
            "p75": self.p75,
            "p90": self.p90,
            "mean": self.mean,
            "std": self.std,
            "source_stats_nk": self.source_stats_nk,
            "first_seen_run_id": self.first_seen_run_id,
            "last_materialized_run_id": self.last_materialized_run_id,
        }


def _estimate_wave_life_percentile(profile: _ProfileMaterialization | None, active_wave_bar_age: int) -> float | None:
    if profile is None:
        return None
    if profile.sample_values:
        values = sorted(profile.sample_values)
        less_or_equal = sum(1 for value in values if value <= float(active_wave_bar_age))
        return float(less_or_equal) / float(len(values))
    thresholds = (
        (profile.p10, 0.10),
        (profile.p25, 0.25),
        (profile.p50, 0.50),
        (profile.p75, 0.75),
        (profile.p90, 0.90),
    )
    for threshold, percentile in thresholds:
        if threshold is not None and float(active_wave_bar_age) <= float(threshold):
            return float(percentile)
    return None if profile.p90 is None else 1.0


def _estimate_remaining_life(target_life: float | None, active_wave_bar_age: int) -> float | None:
    if target_life is None:
        return None
    return float(max(float(target_life) - float(active_wave_bar_age), 0.0))


def _derive_termination_risk_bucket(percentile: float | None) -> str | None:
    if percentile is None:
        return None
    if percentile >= 0.90:
        return "high"
    if percentile >= 0.75:
        return "elevated"
    return "normal"


def _resolve_wave_nk(
    *,
    wave_row: _WaveRow | None,
    asset_type: str,
    code: str,
    timeframe: str,
    wave_id: int,
) -> str:
    if wave_row is not None:
        return wave_row.wave_nk
    return f"{asset_type}|{code}|{timeframe}|wave|{wave_id}"


def _wave_row_from_tuple(row: tuple[object, ...]) -> _WaveRow:
    return _WaveRow(
        wave_nk=str(row[0]),
        asset_type=str(row[1]),
        code=str(row[2]),
        timeframe=str(row[3]),
        wave_id=int(row[4]),
        major_state=str(row[5]),
        reversal_stage=str(row[6]),
        start_bar_dt=_normalize_date_value(row[7], field_name="start_bar_dt"),
        end_bar_dt=_to_python_date(row[8]),
        active_flag=bool(row[9]),
        bar_count=0 if row[10] is None else int(row[10]),
    )


def _state_row_from_tuple(row: tuple[object, ...]) -> _StateRow:
    return _StateRow(
        snapshot_nk=str(row[0]),
        asset_type=str(row[1]),
        code=str(row[2]),
        timeframe=str(row[3]),
        asof_bar_dt=_normalize_date_value(row[4], field_name="asof_bar_dt"),
        major_state=str(row[5]),
        reversal_stage=str(row[6]),
        wave_id=int(row[7]),
    )


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


def _normalize_instruments(instruments: list[str] | tuple[str, ...] | None) -> set[str]:
    normalized: set[str] = set()
    for instrument in instruments or ():
        candidate = str(instrument).strip().upper()
        if not candidate:
            continue
        normalized.add(candidate)
        if "." in candidate:
            normalized.add(candidate.split(".", 1)[0])
    return normalized


def _normalize_timeframes(timeframes: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if not timeframes:
        return DEFAULT_TIMEFRAMES
    normalized = tuple(dict.fromkeys(str(value).strip().upper() for value in timeframes if str(value).strip()))
    invalid = [value for value in normalized if value not in SUPPORTED_TIMEFRAMES]
    if invalid:
        raise ValueError(f"Unsupported timeframes: {invalid}")
    return normalized


def _normalize_date_value(value: object, *, field_name: str) -> date:
    normalized = _to_python_date(value)
    if normalized is None:
        raise ValueError(f"Missing required date field: {field_name}")
    return normalized


def _to_python_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _coerce_optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _series_quantile(series: pd.Series, quantile: float) -> float | None:
    if series.empty:
        return None
    return float(series.quantile(quantile))


def _normalize_row_for_compare(row: dict[str, object]) -> dict[str, object]:
    ignored = {"first_seen_run_id", "last_materialized_run_id", "created_at", "updated_at"}
    normalized: dict[str, object] = {}
    for key, value in row.items():
        if key in ignored:
            continue
        if pd.isna(value):
            normalized[key] = None
        elif isinstance(value, (datetime, pd.Timestamp)):
            normalized[key] = value.date().isoformat()
        elif isinstance(value, date):
            normalized[key] = value.isoformat()
        elif isinstance(value, float):
            normalized[key] = round(float(value), 12)
        else:
            normalized[key] = value
    return normalized


def _build_run_id(*, prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}"


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return f"{asset_type}|{code}|{timeframe}"


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return f"{scope_nk}|{dirty_reason}"


def _build_snapshot_nk(*, asset_type: str, code: str, timeframe: str, asof_bar_dt: date) -> str:
    return f"{asset_type}|{code}|{timeframe}|wave-life|{asof_bar_dt.isoformat()}"


def _build_profile_nk(
    *,
    timeframe: str,
    major_state: str,
    reversal_stage: str,
    metric_name: str,
    sample_version: str,
) -> str:
    return "|".join([timeframe, major_state, reversal_stage, metric_name, sample_version])


def _write_summary(payload: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
