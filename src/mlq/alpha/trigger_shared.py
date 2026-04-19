"""收敛 `alpha trigger` runner 的共享结构与通用归一化工具。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class AlphaTriggerBuildSummary:
    """总结一次 `alpha trigger ledger` runner 的运行结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    execution_mode: str
    trigger_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    claimed_scope_count: int
    candidate_trigger_count: int
    materialized_trigger_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    missing_context_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    alpha_ledger_path: str
    filter_ledger_path: str
    structure_ledger_path: str
    source_trigger_input_table: str
    source_filter_table: str
    source_structure_table: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _TriggerInputRow:
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str


@dataclass(frozen=True)
class _OfficialContextRow:
    instrument: str
    signal_date: date
    asof_date: date
    filter_snapshot_nk: str
    structure_snapshot_nk: str
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_source_context_nk: str | None
    upstream_context_fingerprint: str


@dataclass(frozen=True)
class _TriggerEventRow:
    trigger_event_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str
    source_filter_snapshot_nk: str
    source_structure_snapshot_nk: str
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_source_context_nk: str | None
    upstream_context_fingerprint: str
    trigger_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_alpha_trigger_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"alpha-trigger-{timestamp}"


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return "|".join([asset_type, code, timeframe])


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return f"{scope_nk}|{dirty_reason}"


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


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
