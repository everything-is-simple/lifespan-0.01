"""`alpha formal signal` 的共享常量、数据结构与纯函数。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final


DEFAULT_ALPHA_FORMAL_SIGNAL_TRIGGER_TABLE: Final[str] = "alpha_trigger_event"
DEFAULT_ALPHA_FORMAL_SIGNAL_FAMILY_TABLE: Final[str] = "alpha_family_event"
DEFAULT_ALPHA_FORMAL_SIGNAL_FILTER_TABLE: Final[str] = "filter_snapshot"
DEFAULT_ALPHA_FORMAL_SIGNAL_STRUCTURE_TABLE: Final[str] = "structure_snapshot"
DEFAULT_ALPHA_FORMAL_SIGNAL_WAVE_LIFE_TABLE: Final[str] = "malf_wave_life_snapshot"
DEFAULT_ALPHA_FORMAL_SIGNAL_CONTRACT_VERSION: Final[str] = "alpha-formal-signal-v5"
DEFAULT_ALPHA_STAGE_PERCENTILE_CONTRACT_VERSION: Final[str] = "alpha-stage-percentile-v1"


@dataclass(frozen=True)
class AlphaFormalSignalBuildSummary:
    """总结一次 `alpha formal signal` producer 的运行结果。"""

    run_id: str
    producer_name: str
    producer_version: str
    execution_mode: str
    signal_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    claimed_scope_count: int
    candidate_trigger_count: int
    materialized_signal_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    missing_context_count: int
    admitted_count: int
    blocked_count: int
    deferred_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    alpha_ledger_path: str
    filter_ledger_path: str
    structure_ledger_path: str
    malf_ledger_path: str
    source_trigger_table: str
    source_family_table: str
    source_filter_table: str
    source_structure_table: str
    source_wave_life_table: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _TriggerRow:
    source_trigger_event_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str


@dataclass(frozen=True)
class _ContextRow:
    instrument: str
    signal_date: date
    asof_date: date
    trigger_admissible: bool
    filter_gate_code: str
    filter_reject_reason_code: str | None
    filter_admission_notes: str | None
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_source_context_nk: str | None
    wave_life_percentile: float | None
    remaining_life_bars_p50: float | None
    remaining_life_bars_p75: float | None
    termination_risk_bucket: str | None


@dataclass(frozen=True)
class _FamilyRow:
    source_family_event_nk: str
    source_trigger_event_nk: str
    family_code: str | None
    source_family_contract_version: str | None
    family_role: str | None
    family_bias: str | None
    malf_alignment: str | None
    malf_phase_bucket: str | None
    family_source_context_fingerprint: str | None


@dataclass(frozen=True)
class _WaveLifeRow:
    source_state_snapshot_nk: str
    wave_life_percentile: float | None
    remaining_life_bars_p50: float | None
    remaining_life_bars_p75: float | None
    termination_risk_bucket: str | None


@dataclass(frozen=True)
class _FormalSignalEventRow:
    signal_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str
    formal_signal_status: str
    trigger_admissible: bool
    admission_verdict_code: str
    admission_verdict_owner: str
    admission_reason_code: str | None
    admission_audit_note: str | None
    filter_gate_code: str
    filter_reject_reason_code: str | None
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_source_context_nk: str | None
    source_family_event_nk: str | None
    family_code: str | None
    source_family_contract_version: str | None
    family_role: str | None
    family_bias: str | None
    malf_alignment: str | None
    malf_phase_bucket: str | None
    family_source_context_fingerprint: str | None
    wave_life_percentile: float | None
    remaining_life_bars_p50: float | None
    remaining_life_bars_p75: float | None
    termination_risk_bucket: str | None
    stage_percentile_decision_code: str
    stage_percentile_action_owner: str
    stage_percentile_note: str
    stage_percentile_contract_version: str
    source_trigger_event_nk: str
    signal_contract_version: str
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


def _build_alpha_formal_signal_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"alpha-formal-signal-{timestamp}"


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return "|".join([asset_type, code, timeframe])


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return f"{scope_nk}|{dirty_reason}"


def _build_signal_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    trigger_family: str,
    trigger_type: str,
    pattern_code: str,
    source_trigger_event_nk: str,
    signal_contract_version: str,
) -> str:
    return "|".join(
        [
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            trigger_family,
            trigger_type,
            pattern_code,
            source_trigger_event_nk,
            signal_contract_version,
        ]
    )


def _map_major_state_to_context_code(major_state: str) -> str:
    mapping = {
        "牛顺": "BULL_MAINSTREAM",
        "熊逆": "BULL_COUNTERTREND",
        "牛逆": "BEAR_COUNTERTREND",
        "熊顺": "BEAR_MAINSTREAM",
    }
    return mapping.get(major_state, "UNKNOWN")


def _derive_lifecycle_rank_high(
    *,
    malf_context_4: str,
    current_hh_count: int,
    current_ll_count: int,
) -> int:
    raw_rank = current_hh_count if malf_context_4.startswith("BULL_") else current_ll_count
    return max(0, min(raw_rank, 4))


def _normalize_optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _merge_audit_notes(*notes: str | None) -> str | None:
    parts: list[str] = []
    seen: set[str] = set()
    for note in notes:
        normalized = _normalize_optional_nullable_str(note)
        if normalized is None or normalized in seen:
            continue
        seen.add(normalized)
        parts.append(normalized)
    if not parts:
        return None
    return "; ".join(parts)


def _derive_stage_percentile_decision(
    *,
    malf_phase_bucket: str | None,
    termination_risk_bucket: str | None,
) -> tuple[str, str, str]:
    normalized_phase = _normalize_optional_nullable_str(malf_phase_bucket)
    normalized_risk = _normalize_optional_nullable_str(termination_risk_bucket)
    phase = "unknown" if normalized_phase is None else normalized_phase.strip().lower()
    risk = "unknown" if normalized_risk is None else normalized_risk.strip().lower()

    if phase == "late" and risk in {"elevated", "high"}:
        return (
            "position_trim_bias",
            "position",
            "late phase with elevated termination risk is reserved for position sizing or trim.",
        )
    if phase == "middle" and risk == "high":
        return (
            "alpha_caution_note",
            "alpha_note",
            "middle phase with high termination risk stays note-only until admission authority is reallocated.",
        )
    if phase == "unknown" or risk == "unknown":
        return (
            "observe_only",
            "none",
            "stage or percentile sample is missing, so the matrix remains observational only.",
        )
    return (
        "observe_only",
        "none",
        "current stage and percentile do not escalate beyond observational sidecar.",
    )


def _derive_formal_signal_admission(
    *,
    trigger_admissible: bool,
    family_role: str | None,
    malf_alignment: str | None,
    stage_percentile_decision_code: str,
    stage_percentile_action_owner: str,
    stage_percentile_note: str | None,
    filter_reject_reason_code: str | None,
    filter_admission_notes: str | None,
) -> tuple[str, str, str, str | None, str | None, str, str | None]:
    filter_gate_code = "pre_trigger_passed" if trigger_admissible else "pre_trigger_blocked"
    normalized_role = _normalize_optional_nullable_str(family_role)
    normalized_alignment = _normalize_optional_nullable_str(malf_alignment)
    if not trigger_admissible:
        reject_reason = filter_reject_reason_code or "filter_pre_trigger_blocked"
        return (
            "blocked",
            "blocked",
            "filter_pre_trigger",
            reject_reason,
            _merge_audit_notes("blocked by filter pre-trigger gate.", filter_admission_notes),
            filter_gate_code,
            reject_reason,
        )
    if normalized_role is None:
        return (
            "deferred",
            "note_only",
            "alpha_formal_signal",
            "missing_family_event",
            _merge_audit_notes("missing family interpretation keeps the signal note-only.", filter_admission_notes),
            filter_gate_code,
            None,
        )
    if normalized_role in {"warning", "scout"}:
        return (
            "deferred",
            "note_only",
            "alpha_formal_signal",
            f"family_role_{normalized_role}",
            _merge_audit_notes(
                f"family role `{normalized_role}` remains audit-only at alpha admission layer.",
                stage_percentile_note if stage_percentile_action_owner == "alpha_note" else None,
                filter_admission_notes,
            ),
            filter_gate_code,
            None,
        )
    if stage_percentile_action_owner == "alpha_note":
        return (
            "deferred",
            "note_only",
            "alpha_formal_signal",
            f"stage_percentile_{stage_percentile_decision_code}",
            _merge_audit_notes(stage_percentile_note, filter_admission_notes),
            filter_gate_code,
            None,
        )
    if normalized_alignment == "conflicted":
        return (
            "deferred",
            "downgraded",
            "alpha_formal_signal",
            "family_alignment_conflicted",
            _merge_audit_notes(
                "conflicted family alignment downgrades the signal until stronger alpha evidence arrives.",
                filter_admission_notes,
            ),
            filter_gate_code,
            None,
        )
    return (
        "admitted",
        "admitted",
        "alpha_formal_signal",
        None,
        _merge_audit_notes(
            stage_percentile_note if stage_percentile_decision_code != "observe_only" else None,
            filter_admission_notes,
        ),
        filter_gate_code,
        None,
    )


def _normalize_formal_signal_status(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"admitted", "blocked", "deferred"}:
        return normalized
    if normalized in {"admit", "accepted"}:
        return "admitted"
    if normalized in {"reject", "rejected"}:
        return "blocked"
    return "blocked"


def _normalize_admission_verdict_code(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"admitted", "blocked", "downgraded", "note_only"}:
        return normalized
    return "blocked"


def _normalize_admission_verdict_owner(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"filter_pre_trigger", "alpha_formal_signal"}:
        return normalized
    return "alpha_formal_signal"


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


def _write_summary(summary: AlphaFormalSignalBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
