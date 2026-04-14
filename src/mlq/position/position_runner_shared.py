"""承载 `position` data-grade runner 的共享常量与摘要结构。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

from mlq.position.position_shared import PositionFormalSignalInput


DEFAULT_ALPHA_FORMAL_SIGNAL_TABLE: Final[str] = "alpha_formal_signal_event"
DEFAULT_MARKET_BASE_PRICE_TABLE: Final[str] = "stock_daily_adjusted"
DEFAULT_MARKET_BASE_ADJUST_METHOD: Final[str] = "none"
DEFAULT_POSITION_RUNNER_NAME: Final[str] = "position_formal_signal_runner"
DEFAULT_POSITION_RUNNER_VERSION: Final[str] = "v2"


@dataclass(frozen=True)
class PositionFormalSignalRunnerSummary:
    """总结一次 `position` runner 对 `alpha -> position` 的正式桥接结果。"""

    policy_id: str
    execution_mode: str
    position_run_id: str
    alpha_signal_count: int
    enriched_signal_count: int
    missing_reference_price_count: int
    candidate_count: int
    admitted_count: int
    blocked_count: int
    risk_budget_count: int
    sizing_count: int
    family_snapshot_count: int
    entry_leg_count: int
    exit_plan_count: int
    exit_leg_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    alpha_ledger_path: str
    market_base_path: str
    position_ledger_path: str
    alpha_formal_signal_table: str
    market_price_table: str
    adjust_method: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class PositionCandidateMaterializationInput:
    signal: PositionFormalSignalInput
    candidate_nk: str
    checkpoint_nk: str
    source_signal_fingerprint: str


def resolve_execution_mode(
    *,
    use_checkpoint_queue: bool | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> str:
    if use_checkpoint_queue is not None:
        return "checkpoint_queue" if use_checkpoint_queue else "bounded"
    if signal_start_date is None and signal_end_date is None and not instruments:
        return "checkpoint_queue"
    return "bounded"


def build_position_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"position-formal-signal-{timestamp}"


def coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def write_summary(
    summary: PositionFormalSignalRunnerSummary,
    summary_path: Path | None,
) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
