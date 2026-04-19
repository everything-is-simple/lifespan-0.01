"""`alpha family ledger` runner 的落表与审计 helper。"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import duckdb

from mlq.alpha.bootstrap import (
    ALPHA_FAMILY_EVENT_TABLE,
    ALPHA_FAMILY_RUN_EVENT_TABLE,
    ALPHA_FAMILY_RUN_TABLE,
)
from mlq.alpha.family_shared import (
    AlphaFamilyBuildSummary,
    _DEFAULT_FAMILY_BIAS_BY_TYPE,
    _DEFAULT_FAMILY_CODE_BY_TYPE,
    _DEFAULT_FAMILY_ROLE_BY_TYPE,
    _FamilyContextRow,
    _FamilyEventRow,
    _MalfStateRow,
    _TriggerRow,
    _normalize_optional_bool,
    _normalize_optional_float,
    _normalize_optional_str,
    _parse_json_blob,
    _stable_json_fingerprint,
)


def _materialize_family_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    trigger_rows: list[_TriggerRow],
    candidate_map: dict[tuple[str, date, date, str, str], dict[str, object]],
    family_context_map: dict[str, _FamilyContextRow],
    family_contract_version: str,
    family_scope: tuple[str, ...],
    producer_name: str,
    producer_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    alpha_path: Path,
    structure_path: Path,
    malf_path: Path,
    source_trigger_table: str,
    source_candidate_table: str,
    source_structure_table: str,
    source_malf_table: str,
    batch_size: int,
) -> AlphaFamilyBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    family_counts = {family_name: 0 for family_name in family_scope}
    for trigger_batch in _bounded_by_instrument_batches(trigger_rows, batch_size=batch_size):
        for trigger_row in trigger_batch:
            candidate_payload = candidate_map.get(
                (
                    trigger_row.instrument,
                    trigger_row.signal_date,
                    trigger_row.asof_date,
                    trigger_row.trigger_type,
                    trigger_row.pattern_code,
                )
            )
            event_row = _build_family_event_row(
                run_id=run_id,
                trigger_row=trigger_row,
                candidate_payload=candidate_payload,
                family_context=family_context_map.get(trigger_row.trigger_event_nk),
                family_contract_version=family_contract_version,
            )
            materialization_action = _upsert_family_event(connection, event_row=event_row)
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {ALPHA_FAMILY_RUN_EVENT_TABLE} (
                    run_id,
                    family_event_nk,
                    trigger_event_nk,
                    materialization_action,
                    family_code
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    event_row.family_event_nk,
                    event_row.trigger_event_nk,
                    materialization_action,
                    event_row.family_code,
                ],
            )
            family_counts[trigger_row.trigger_type] = family_counts.get(trigger_row.trigger_type, 0) + 1
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1
    materialized_family_event_count = inserted_count + reused_count + rematerialized_count
    return AlphaFamilyBuildSummary(
        run_id=run_id,
        producer_name=producer_name,
        producer_version=producer_version,
        family_contract_version=family_contract_version,
        family_scope=list(family_scope),
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in trigger_rows}),
        candidate_trigger_count=len(trigger_rows),
        materialized_family_event_count=materialized_family_event_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        family_counts=family_counts,
        alpha_ledger_path=str(alpha_path),
        structure_ledger_path=str(structure_path),
        malf_ledger_path=str(malf_path),
        source_trigger_table=source_trigger_table,
        source_candidate_table=source_candidate_table,
        source_structure_table=source_structure_table,
        source_malf_table=source_malf_table,
    )


def _bounded_by_instrument_batches(
    trigger_rows: list[_TriggerRow],
    *,
    batch_size: int,
) -> list[list[_TriggerRow]]:
    if not trigger_rows:
        return []
    normalized_batch_size = max(int(batch_size), 1)
    batches: list[list[_TriggerRow]] = []
    current_batch: list[_TriggerRow] = []
    current_instruments: set[str] = set()
    for row in trigger_rows:
        if current_batch and row.instrument not in current_instruments and len(current_instruments) >= normalized_batch_size:
            batches.append(current_batch)
            current_batch = []
            current_instruments = set()
        current_batch.append(row)
        current_instruments.add(row.instrument)
    if current_batch:
        batches.append(current_batch)
    return batches


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    producer_name: str,
    producer_version: str,
    family_scope: tuple[str, ...],
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    source_trigger_table: str,
    source_candidate_table: str,
    family_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {ALPHA_FAMILY_RUN_TABLE} (
            run_id,
            producer_name,
            producer_version,
            run_status,
            family_scope,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_trigger_table,
            source_candidate_table,
            family_contract_version,
            notes
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            producer_name,
            producer_version,
            json.dumps(list(family_scope), ensure_ascii=False),
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_trigger_table,
            source_candidate_table,
            family_contract_version,
            "bounded alpha family ledger materialization",
        ],
    )


def _build_family_event_row(
    *,
    run_id: str,
    trigger_row: _TriggerRow,
    candidate_payload: dict[str, object] | None,
    family_context: _FamilyContextRow | None,
    family_contract_version: str,
) -> _FamilyEventRow:
    family_code = _resolve_family_code(trigger_row.trigger_type, candidate_payload)
    payload_json = json.dumps(
        _build_payload(
            trigger_row=trigger_row,
            family_code=family_code,
            candidate_payload=candidate_payload,
            family_context=family_context,
        ),
        ensure_ascii=False,
        sort_keys=True,
    )
    return _FamilyEventRow(
        family_event_nk=_build_family_event_nk(
            trigger_event_nk=trigger_row.trigger_event_nk,
            family_contract_version=family_contract_version,
        ),
        trigger_event_nk=trigger_row.trigger_event_nk,
        instrument=trigger_row.instrument,
        signal_date=trigger_row.signal_date,
        asof_date=trigger_row.asof_date,
        trigger_family=trigger_row.trigger_family,
        trigger_type=trigger_row.trigger_type,
        pattern_code=trigger_row.pattern_code,
        family_code=family_code,
        family_contract_version=family_contract_version,
        payload_json=payload_json,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _resolve_family_code(trigger_type: str, candidate_payload: dict[str, object] | None) -> str:
    if candidate_payload is not None:
        candidate_family_code = _normalize_optional_str(candidate_payload.get("family_code"))
        if candidate_family_code:
            return candidate_family_code
    return _DEFAULT_FAMILY_CODE_BY_TYPE.get(trigger_type, f"{trigger_type}_core")


def _build_family_event_nk(
    *,
    trigger_event_nk: str,
    family_contract_version: str,
) -> str:
    return "|".join([trigger_event_nk, family_contract_version])


def _build_payload(
    *,
    trigger_row: _TriggerRow,
    family_code: str,
    candidate_payload: dict[str, object] | None,
    family_context: _FamilyContextRow | None,
) -> dict[str, object]:
    normalized_candidate_payload = _normalize_candidate_payload(candidate_payload)
    family_bias = _DEFAULT_FAMILY_BIAS_BY_TYPE.get(trigger_row.trigger_type, "trend_continuation")
    pb_first_pullback = _derive_pb_first_pullback(
        trigger_type=trigger_row.trigger_type,
        family_context=family_context,
    )
    malf_alignment = _derive_malf_alignment(
        trigger_type=trigger_row.trigger_type,
        family_context=family_context,
    )
    malf_phase_bucket = _derive_malf_phase_bucket(family_context)
    family_role = _derive_family_role(
        trigger_type=trigger_row.trigger_type,
        malf_alignment=malf_alignment,
        pb_first_pullback=pb_first_pullback,
    )
    source_context_snapshot = _build_source_context_snapshot(
        trigger_row=trigger_row,
        normalized_candidate_payload=normalized_candidate_payload,
        family_context=family_context,
    )
    payload: dict[str, object] = {
        "family_code": family_code,
        "family_role": family_role,
        "family_bias": family_bias,
        "malf_alignment": malf_alignment,
        "malf_phase_bucket": malf_phase_bucket,
        "trigger_reason": _build_trigger_reason(
            trigger_row=trigger_row,
            family_role=family_role,
            family_bias=family_bias,
            malf_alignment=malf_alignment,
            malf_phase_bucket=malf_phase_bucket,
            pb_first_pullback=pb_first_pullback,
            family_context=family_context,
        ),
        "pattern_code": trigger_row.pattern_code,
        "trigger_type": trigger_row.trigger_type,
        "structure_anchor_nk": trigger_row.source_structure_snapshot_nk,
        "source_context_fingerprint": _stable_json_fingerprint(source_context_snapshot),
        "source_context_snapshot": source_context_snapshot,
        "source_trigger": {
            "trigger_event_nk": trigger_row.trigger_event_nk,
            "source_filter_snapshot_nk": trigger_row.source_filter_snapshot_nk,
            "source_structure_snapshot_nk": trigger_row.source_structure_snapshot_nk,
            "upstream_context_fingerprint": _normalize_upstream_context_fingerprint(
                trigger_row.upstream_context_fingerprint
            ),
        },
        "official_context": _build_official_context_payload(family_context),
        "candidate_payload": normalized_candidate_payload,
    }
    if pb_first_pullback is not None:
        payload["pb_first_pullback"] = pb_first_pullback
    trigger_strength = _normalize_optional_float(normalized_candidate_payload.get("trigger_strength"))
    if trigger_strength is not None:
        payload["trigger_strength"] = trigger_strength
    detect_reason = normalized_candidate_payload.get("detect_reason")
    if detect_reason is not None and str(detect_reason).strip():
        payload["detect_reason"] = str(detect_reason)
    skip_reason = normalized_candidate_payload.get("skip_reason")
    if skip_reason is not None and str(skip_reason).strip():
        payload["skip_reason"] = str(skip_reason)
    return payload


def _normalize_candidate_payload(candidate_payload: dict[str, object] | None) -> dict[str, object]:
    if candidate_payload is None:
        return {}
    normalized_payload: dict[str, object] = {}
    for key, value in candidate_payload.items():
        if key in {"instrument", "signal_date", "asof_date", "trigger_family", "trigger_type", "pattern_code"}:
            continue
        if key.endswith("_json"):
            normalized_payload[key] = _parse_json_blob(value)
            continue
        normalized_payload[key] = _normalize_json_value(value)
    return normalized_payload


def _normalize_json_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _normalize_upstream_context_fingerprint(value: object) -> object:
    normalized = _normalize_optional_str(value, default="{}")
    try:
        parsed = json.loads(normalized)
    except json.JSONDecodeError:
        return normalized
    return parsed if isinstance(parsed, dict) else normalized


def _derive_family_role(
    *,
    trigger_type: str,
    malf_alignment: str,
    pb_first_pullback: bool | None,
) -> str:
    base_role = _DEFAULT_FAMILY_ROLE_BY_TYPE.get(trigger_type, "supporting")
    if trigger_type in {"bof", "tst"} and malf_alignment == "conflicted":
        return "supporting"
    if trigger_type == "pb" and pb_first_pullback and malf_alignment == "aligned":
        return "mainline"
    return base_role


def _derive_malf_alignment(
    *,
    trigger_type: str,
    family_context: _FamilyContextRow | None,
) -> str:
    if family_context is None:
        return "unknown"
    if family_context.structure_progress_state == "failed":
        return "conflicted"
    daily_state = family_context.daily_malf_state
    if daily_state is None:
        return "unknown"
    higher_trend_directions = [
        state.trend_direction
        for state in (family_context.weekly_malf_state, family_context.monthly_malf_state)
        if state is not None and state.trend_direction in {"up", "down"}
    ]
    higher_down_count = sum(1 for item in higher_trend_directions if item == "down")
    higher_up_count = sum(1 for item in higher_trend_directions if item == "up")
    higher_conflicted = higher_down_count > 0 and higher_down_count >= higher_up_count
    higher_mixed = higher_down_count > 0 and higher_up_count > 0

    if trigger_type == "bpb":
        if higher_conflicted or family_context.break_confirmation_status == "failed":
            return "conflicted"
        return "cautious"
    if trigger_type == "bof":
        reversal_ready = (
            daily_state.reversal_stage in {"trigger", "hold", "expand"}
            or family_context.reversal_stage in {"trigger", "hold", "expand"}
        )
        if reversal_ready and family_context.structure_progress_state == "advancing":
            return "cautious" if higher_mixed else "aligned"
        return "cautious" if higher_up_count > 0 else "unknown"
    if trigger_type == "cpb":
        if higher_conflicted:
            return "conflicted"
        if family_context.structure_progress_state == "advancing":
            return "cautious"
        return "unknown"

    if daily_state.trend_direction == "up" and family_context.structure_progress_state == "advancing":
        if higher_conflicted:
            return "cautious"
        return "cautious" if higher_mixed else "aligned"
    if daily_state.trend_direction == "down":
        return "conflicted"
    return "unknown"


def _derive_malf_phase_bucket(family_context: _FamilyContextRow | None) -> str:
    if family_context is None:
        return "unknown"
    daily_state = family_context.daily_malf_state
    if daily_state is None:
        return "unknown"
    if daily_state.reversal_stage in {"trigger", "hold"}:
        return "early"
    leg_count = max(
        daily_state.current_hh_count,
        daily_state.current_ll_count,
        family_context.current_hh_count,
        family_context.current_ll_count,
    )
    if leg_count <= 1:
        return "early"
    if leg_count <= 3:
        return "middle"
    return "late"


def _derive_pb_first_pullback(
    *,
    trigger_type: str,
    family_context: _FamilyContextRow | None,
) -> bool | None:
    if trigger_type != "pb":
        return None
    if family_context is None:
        return False
    daily_state = family_context.daily_malf_state
    if daily_state is None:
        return False
    continuation_leg_count = max(daily_state.current_hh_count, family_context.current_hh_count)
    pullback_leg_count = max(daily_state.current_ll_count, family_context.current_ll_count)
    return (
        family_context.structure_progress_state == "advancing"
        and daily_state.trend_direction == "up"
        and continuation_leg_count > 0
        and continuation_leg_count <= 4
        and pullback_leg_count == 0
    )


def _build_trigger_reason(
    *,
    trigger_row: _TriggerRow,
    family_role: str,
    family_bias: str,
    malf_alignment: str,
    malf_phase_bucket: str,
    pb_first_pullback: bool | None,
    family_context: _FamilyContextRow | None,
) -> str:
    progress_state = "unknown" if family_context is None else family_context.structure_progress_state
    if trigger_row.trigger_type == "pb" and pb_first_pullback:
        return (
            f"PB 默认 supporting；当前处于 {malf_phase_bucket} 阶段且满足第一回调窗口，"
            f"在 structure_progress={progress_state} 下升级为 {family_role}。"
        )
    return (
        f"{trigger_row.pattern_code} 归入 {family_role}，偏向 {family_bias}；"
        f"当前 malf_alignment={malf_alignment}，phase={malf_phase_bucket}，"
        f"structure_progress={progress_state}。"
    )


def _build_source_context_snapshot(
    *,
    trigger_row: _TriggerRow,
    normalized_candidate_payload: dict[str, object],
    family_context: _FamilyContextRow | None,
) -> dict[str, object]:
    return {
        "trigger_event_nk": trigger_row.trigger_event_nk,
        "source_structure_snapshot_nk": trigger_row.source_structure_snapshot_nk,
        "trigger_upstream_context": _normalize_upstream_context_fingerprint(
            trigger_row.upstream_context_fingerprint
        ),
        "candidate_family_code": _normalize_optional_str(
            normalized_candidate_payload.get("family_code"),
            default=_DEFAULT_FAMILY_CODE_BY_TYPE.get(trigger_row.trigger_type, f"{trigger_row.trigger_type}_core"),
        ),
        "candidate_trigger_strength": _normalize_optional_float(
            normalized_candidate_payload.get("trigger_strength")
        ),
        "candidate_detect_reason": _normalize_optional_str(
            normalized_candidate_payload.get("detect_reason")
        ),
        "structure_progress_state": (
            "unknown" if family_context is None else family_context.structure_progress_state
        ),
        "structure_break_confirmation_status": (
            None if family_context is None else family_context.break_confirmation_status
        ),
        "daily_context_nk": None if family_context is None else family_context.source_context_nk,
        "weekly_context_nk": None if family_context is None else family_context.weekly_source_context_nk,
        "monthly_context_nk": None if family_context is None else family_context.monthly_source_context_nk,
        "daily_malf": _build_malf_state_payload(None if family_context is None else family_context.daily_malf_state),
        "weekly_malf": _build_malf_state_payload(None if family_context is None else family_context.weekly_malf_state),
        "monthly_malf": _build_malf_state_payload(None if family_context is None else family_context.monthly_malf_state),
    }


def _build_official_context_payload(family_context: _FamilyContextRow | None) -> dict[str, object]:
    if family_context is None:
        return {}
    return {
        "structure": {
            "structure_snapshot_nk": family_context.structure_snapshot_nk,
            "major_state": family_context.major_state,
            "trend_direction": family_context.trend_direction,
            "reversal_stage": family_context.reversal_stage,
            "current_hh_count": family_context.current_hh_count,
            "current_ll_count": family_context.current_ll_count,
            "structure_progress_state": family_context.structure_progress_state,
            "break_confirmation_status": family_context.break_confirmation_status,
            "break_confirmation_ref": family_context.break_confirmation_ref,
            "exhaustion_risk_bucket": family_context.exhaustion_risk_bucket,
            "reversal_probability_bucket": family_context.reversal_probability_bucket,
            "source_context_nk": family_context.source_context_nk,
            "weekly_source_context_nk": family_context.weekly_source_context_nk,
            "monthly_source_context_nk": family_context.monthly_source_context_nk,
        },
        "malf": {
            "daily": _build_malf_state_payload(family_context.daily_malf_state),
            "weekly": _build_malf_state_payload(family_context.weekly_malf_state),
            "monthly": _build_malf_state_payload(family_context.monthly_malf_state),
        },
    }


def _build_malf_state_payload(state_row: _MalfStateRow | None) -> dict[str, object]:
    if state_row is None:
        return {}
    return {
        "snapshot_nk": state_row.snapshot_nk,
        "timeframe": state_row.timeframe,
        "major_state": state_row.major_state,
        "trend_direction": state_row.trend_direction,
        "reversal_stage": state_row.reversal_stage,
        "current_hh_count": state_row.current_hh_count,
        "current_ll_count": state_row.current_ll_count,
    }


def _upsert_family_event(
    connection: duckdb.DuckDBPyConnection,
    *,
    event_row: _FamilyEventRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            payload_json,
            first_seen_run_id
        FROM {ALPHA_FAMILY_EVENT_TABLE}
        WHERE family_event_nk = ?
        """,
        [event_row.family_event_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {ALPHA_FAMILY_EVENT_TABLE} (
                family_event_nk,
                trigger_event_nk,
                instrument,
                signal_date,
                asof_date,
                trigger_family,
                trigger_type,
                pattern_code,
                family_code,
                family_contract_version,
                payload_json,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event_row.family_event_nk,
                event_row.trigger_event_nk,
                event_row.instrument,
                event_row.signal_date,
                event_row.asof_date,
                event_row.trigger_family,
                event_row.trigger_type,
                event_row.pattern_code,
                event_row.family_code,
                event_row.family_contract_version,
                event_row.payload_json,
                event_row.first_seen_run_id,
                event_row.last_materialized_run_id,
            ],
        )
        return "inserted"
    existing_payload = _normalize_optional_str(existing_row[0], default="{}")
    first_seen_run_id = str(existing_row[1]) if existing_row[1] is not None else event_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {ALPHA_FAMILY_EVENT_TABLE}
        SET
            payload_json = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE family_event_nk = ?
        """,
        [
            event_row.payload_json,
            first_seen_run_id,
            event_row.last_materialized_run_id,
            event_row.family_event_nk,
        ],
    )
    if existing_payload == event_row.payload_json:
        return "reused"
    return "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: AlphaFamilyBuildSummary,
) -> None:
    _update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        materialized_family_event_count=summary.materialized_family_event_count,
        summary_payload=summary.as_dict(),
    )


def _update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    materialized_family_event_count: int = 0,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_FAMILY_RUN_TABLE}
        SET
            run_status = ?,
            materialized_family_event_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            materialized_family_event_count,
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )
