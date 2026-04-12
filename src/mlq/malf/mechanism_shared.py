"""沉淀 `malf mechanism` runner 的共享结构与归一化工具。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path


_METRIC_FIELDS: tuple[str, ...] = (
    "new_high_count",
    "new_low_count",
    "refresh_density",
    "advancement_density",
)


@dataclass(frozen=True)
class MalfMechanismBuildSummary:
    """总结一次机制层 sidecar bounded runner 的执行结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    timeframe: str
    stats_sample_version: str
    mechanism_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    source_candidate_count: int
    break_ledger_count: int
    stats_profile_count: int
    stats_snapshot_count: int
    break_inserted_count: int
    break_reused_count: int
    break_rematerialized_count: int
    profile_inserted_count: int
    profile_reused_count: int
    profile_rematerialized_count: int
    snapshot_inserted_count: int
    snapshot_reused_count: int
    snapshot_rematerialized_count: int
    checkpoint_upserted_count: int
    confirmed_break_count: int
    pending_break_count: int
    malf_ledger_path: str
    source_context_table: str
    source_structure_input_table: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _MechanismInputRow:
    instrument: str
    signal_date: date
    asof_date: date
    source_context_nk: str
    source_candidate_nk: str
    malf_context_4: str
    new_high_count: int
    new_low_count: int
    refresh_density: float
    advancement_density: float
    is_failed_extreme: bool
    failure_type: str | None


def _normalize_date_value(value: object, *, field_name: str) -> date:
    normalized = _coerce_date(value)
    if normalized is None:
        raise ValueError(f"Missing required date field: {field_name}")
    return normalized


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
    if value is None:
        return 0
    return int(value)


def _normalize_optional_float(value: object) -> float:
    if value is None:
        return 0.0
    return float(value)


def _coerce_date(value: object | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_run_id(*, prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"


def _build_guard_pivot_id(*, instrument: str, timeframe: str, signal_date: date, direction: str) -> str:
    return "|".join([instrument, timeframe, signal_date.isoformat(), direction, "guard"])


def _build_confirmation_pivot_id(*, instrument: str, timeframe: str, signal_date: date, direction: str) -> str:
    return "|".join([instrument, timeframe, signal_date.isoformat(), direction, "confirmation"])


def _build_break_event_nk(*, instrument: str, timeframe: str, guard_pivot_id: str, trigger_bar_dt: date) -> str:
    return "|".join([instrument, timeframe, guard_pivot_id, trigger_bar_dt.isoformat()])


def _build_stats_profile_nk(
    *,
    universe: str,
    timeframe: str,
    regime_family: str,
    metric_name: str,
    sample_version: str,
) -> str:
    return "|".join([universe, timeframe, regime_family, metric_name, sample_version])


def _build_stats_snapshot_nk(
    *,
    instrument: str,
    timeframe: str,
    asof_bar_dt: date,
    sample_version: str,
    mechanism_contract_version: str,
) -> str:
    return "|".join([instrument, timeframe, asof_bar_dt.isoformat(), sample_version, mechanism_contract_version])


def _build_source_context_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    malf_context_4: str,
) -> str:
    return "|".join([instrument, signal_date.isoformat(), asof_date.isoformat(), malf_context_4])


def _build_source_candidate_nk(*, instrument: str, signal_date: date, asof_date: date) -> str:
    return "|".join([instrument, signal_date.isoformat(), asof_date.isoformat(), "mechanism-source"])


def _write_summary(payload: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
