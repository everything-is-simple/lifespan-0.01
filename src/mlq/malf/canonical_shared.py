"""沉淀 canonical MALF runner 的共享结构与基础归一化工具。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd


DEFAULT_TIMEFRAMES: tuple[str, ...] = ("D", "W", "M")
SUPPORTED_TIMEFRAMES: tuple[str, ...] = ("D", "W", "M")


@dataclass(frozen=True)
class MalfCanonicalBuildSummary:
    run_id: str
    runner_name: str
    runner_version: str
    canonical_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    timeframe_list: list[str]
    bounded_scope_count: int
    claimed_scope_count: int
    completed_scope_count: int
    failed_scope_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    pivot_row_count: int
    wave_row_count: int
    extreme_row_count: int
    state_row_count: int
    stats_row_count: int
    pivot_inserted_count: int
    pivot_reused_count: int
    pivot_rematerialized_count: int
    wave_inserted_count: int
    wave_reused_count: int
    wave_rematerialized_count: int
    extreme_inserted_count: int
    extreme_reused_count: int
    extreme_rematerialized_count: int
    state_inserted_count: int
    state_reused_count: int
    state_rematerialized_count: int
    stats_inserted_count: int
    stats_reused_count: int
    stats_rematerialized_count: int
    market_base_path: str
    malf_ledger_path: str
    source_price_table: str
    source_adjust_method: str
    pivot_confirmation_window: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _Pivot:
    pivot_nk: str
    asset_type: str
    code: str
    timeframe: str
    pivot_type: str
    pivot_bar_dt: date
    confirmed_at: date
    pivot_price: float
    prior_pivot_nk: str | None


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


def _to_python_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date()
    return pd.Timestamp(value).date()


def _coerce_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_run_id(*, prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}"


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return f"{asset_type}|{code}|{timeframe}"


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return f"{scope_nk}|{dirty_reason}"


def _build_pivot_nk(asset_type: str, code: str, timeframe: str, pivot_type: str, pivot_bar_dt: date) -> str:
    return f"{asset_type}|{code}|{timeframe}|{pivot_type}|{pivot_bar_dt.isoformat()}"


def _build_wave_nk(asset_type: str, code: str, timeframe: str, wave_id: int) -> str:
    return f"{asset_type}|{code}|{timeframe}|wave|{wave_id}"


def _build_extreme_nk(asset_type: str, code: str, timeframe: str, wave_id: int, extreme_seq: int) -> str:
    return f"{asset_type}|{code}|{timeframe}|wave|{wave_id}|extreme|{extreme_seq}"


def _build_snapshot_nk(asset_type: str, code: str, timeframe: str, asof_bar_dt: date) -> str:
    return f"{asset_type}|{code}|{timeframe}|snapshot|{asof_bar_dt.isoformat()}"


def _build_stats_nk(universe: str, timeframe: str, major_state: str, metric_name: str, sample_version: str) -> str:
    return f"{universe}|{timeframe}|{major_state}|{metric_name}|{sample_version}"


def _series_quantile(series: pd.Series, quantile: float) -> float | None:
    if series.empty:
        return None
    return float(series.quantile(quantile))


def _write_summary(summary: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
