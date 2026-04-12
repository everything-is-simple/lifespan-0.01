"""`malf snapshot` bridge v1 runner 的共享常量与通用工具。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path


DEFAULT_MARKET_PRICE_TABLE = "stock_daily_adjusted"
DEFAULT_MALF_CONTRACT_VERSION = "malf-snapshot-v1"
DEFAULT_MALF_ADJUST_METHOD = "backward"


@dataclass(frozen=True)
class MalfSnapshotBuildSummary:
    run_id: str
    runner_name: str
    runner_version: str
    malf_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    source_price_row_count: int
    context_snapshot_count: int
    structure_candidate_count: int
    context_inserted_count: int
    context_reused_count: int
    context_rematerialized_count: int
    structure_inserted_count: int
    structure_reused_count: int
    structure_rematerialized_count: int
    market_base_path: str
    malf_ledger_path: str
    source_price_table: str
    adjust_method: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


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


def _coerce_date(value: object | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _chunked(items: list[str], *, size: int) -> list[list[str]]:
    if not items:
        return []
    return [items[index : index + size] for index in range(0, len(items), size)]


def _build_run_id(*, prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"


def _write_summary(payload: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
