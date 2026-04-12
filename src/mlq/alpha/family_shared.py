"""`alpha family ledger` runner 的共享常量与通用数据结构。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final


ALPHA_FAMILY_SCOPE_ALL: Final[tuple[str, ...]] = ("bof", "tst", "pb", "cpb", "bpb")
DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE: Final[str] = "alpha_trigger_event"
DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE: Final[str] = "alpha_trigger_candidate"
DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION: Final[str] = "alpha-family-v1"

_DEFAULT_FAMILY_CODE_BY_TYPE: Final[dict[str, str]] = {
    "bof": "bof_core",
    "tst": "tst_core",
    "pb": "pb_core",
    "cpb": "cpb_core",
    "bpb": "bpb_core",
}


@dataclass(frozen=True)
class AlphaFamilyBuildSummary:
    """总结一次 `alpha family ledger` runner 的 bounded 运行结果。"""

    run_id: str
    producer_name: str
    producer_version: str
    family_contract_version: str
    family_scope: list[str]
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    candidate_trigger_count: int
    materialized_family_event_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    family_counts: dict[str, int]
    alpha_ledger_path: str
    source_trigger_table: str
    source_candidate_table: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _TriggerRow:
    trigger_event_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str
    source_filter_snapshot_nk: str
    source_structure_snapshot_nk: str
    upstream_context_fingerprint: str


@dataclass(frozen=True)
class _FamilyEventRow:
    family_event_nk: str
    trigger_event_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str
    family_code: str
    family_contract_version: str
    payload_json: str
    first_seen_run_id: str
    last_materialized_run_id: str


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_family_scope(family_scope: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    normalized = tuple(
        dict.fromkeys(
            item.strip().lower()
            for item in (family_scope or ALPHA_FAMILY_SCOPE_ALL)
            if str(item).strip()
        )
    )
    if not normalized:
        raise ValueError("Family scope cannot be empty.")
    invalid_scope = tuple(item for item in normalized if item not in ALPHA_FAMILY_SCOPE_ALL)
    if invalid_scope:
        raise ValueError(
            "Unsupported family scope: "
            + ", ".join(invalid_scope)
            + f". Supported families: {', '.join(ALPHA_FAMILY_SCOPE_ALL)}."
        )
    return normalized


def _build_alpha_family_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"alpha-family-{timestamp}"


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


def _write_summary(summary: AlphaFamilyBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
