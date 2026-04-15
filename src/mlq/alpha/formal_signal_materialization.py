"""`alpha formal signal` 的落表物化逻辑。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from mlq.alpha.bootstrap import (
    ALPHA_FORMAL_SIGNAL_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_RUN_TABLE,
)
from mlq.alpha.formal_signal_shared import (
    DEFAULT_ALPHA_STAGE_PERCENTILE_CONTRACT_VERSION,
    AlphaFormalSignalBuildSummary,
    _ContextRow,
    _FamilyRow,
    _FormalSignalEventRow,
    _TriggerRow,
    _derive_formal_signal_admission,
    _build_signal_nk,
    _derive_stage_percentile_decision,
    _normalize_admission_verdict_code,
    _normalize_admission_verdict_owner,
    _normalize_formal_signal_status,
    _normalize_optional_float,
    _normalize_optional_int,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
)
from mlq.alpha.formal_signal_source import (
    _load_family_rows,
    _load_official_context_rows,
    _load_trigger_rows,
)


def _materialize_alpha_formal_signal_scope(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    alpha_path: Path,
    filter_path: Path,
    structure_path: Path,
    malf_path: Path,
    instrument: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    limit: int,
    batch_size: int,
    source_trigger_table: str,
    source_family_table: str,
    source_filter_table: str,
    source_structure_table: str,
    source_wave_life_table: str,
    signal_contract_version: str,
    producer_name: str,
    producer_version: str,
) -> AlphaFormalSignalBuildSummary:
    trigger_rows = _load_trigger_rows(
        connection=connection,
        table_name=source_trigger_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=(instrument,),
        limit=limit,
    )
    family_map = _load_family_rows(
        connection=connection,
        table_name=source_family_table,
        trigger_rows=trigger_rows,
    )
    context_rows = _load_official_context_rows(
        filter_path=filter_path,
        structure_path=structure_path,
        malf_path=malf_path,
        filter_table_name=source_filter_table,
        structure_table_name=source_structure_table,
        wave_life_table_name=source_wave_life_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=tuple(sorted({row.instrument for row in trigger_rows})),
    )
    context_map = {
        (row.instrument, row.signal_date, row.asof_date): row
        for row in context_rows
    }
    return _materialize_formal_signal_rows(
        connection=connection,
        run_id=run_id,
        trigger_rows=trigger_rows,
        family_map=family_map,
        context_map=context_map,
        signal_contract_version=signal_contract_version,
        producer_name=producer_name,
        producer_version=producer_version,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        alpha_path=alpha_path,
        filter_path=filter_path,
        structure_path=structure_path,
        malf_path=malf_path,
        source_trigger_table=source_trigger_table,
        source_family_table=source_family_table,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        source_wave_life_table=source_wave_life_table,
        batch_size=batch_size,
    )


def _materialize_formal_signal_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    trigger_rows: list[_TriggerRow],
    family_map: dict[str, _FamilyRow],
    context_map: dict[tuple[str, date, date], _ContextRow],
    signal_contract_version: str,
    producer_name: str,
    producer_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    alpha_path: Path,
    filter_path: Path,
    structure_path: Path,
    malf_path: Path,
    source_trigger_table: str,
    source_family_table: str,
    source_filter_table: str,
    source_structure_table: str,
    source_wave_life_table: str,
    batch_size: int,
) -> AlphaFormalSignalBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    missing_context_count = 0
    admitted_count = 0
    blocked_count = 0
    deferred_count = 0

    for trigger_batch in _bounded_by_instrument_batches(trigger_rows, batch_size=batch_size):
        for trigger_row in trigger_batch:
            context_row = context_map.get((trigger_row.instrument, trigger_row.signal_date, trigger_row.asof_date))
            if context_row is None:
                missing_context_count += 1
                continue
            event_row = _build_formal_signal_event_row(
                run_id=run_id,
                trigger_row=trigger_row,
                family_row=family_map.get(trigger_row.source_trigger_event_nk),
                context_row=context_row,
                signal_contract_version=signal_contract_version,
            )
            materialization_action = _upsert_formal_signal_event(
                connection,
                event_row=event_row,
            )
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE} (
                    run_id,
                    signal_nk,
                    materialization_action,
                    formal_signal_status,
                    admission_verdict_code,
                    admission_verdict_owner,
                    admission_reason_code,
                    source_family_event_nk,
                    family_code,
                    family_role,
                    malf_alignment,
                    family_source_context_fingerprint,
                    stage_percentile_decision_code,
                    stage_percentile_action_owner,
                    source_trigger_event_nk
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    event_row.signal_nk,
                    materialization_action,
                    event_row.formal_signal_status,
                    event_row.admission_verdict_code,
                    event_row.admission_verdict_owner,
                    event_row.admission_reason_code,
                    event_row.source_family_event_nk,
                    event_row.family_code,
                    event_row.family_role,
                    event_row.malf_alignment,
                    event_row.family_source_context_fingerprint,
                    event_row.stage_percentile_decision_code,
                    event_row.stage_percentile_action_owner,
                    event_row.source_trigger_event_nk,
                ],
            )
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

            if event_row.formal_signal_status == "admitted":
                admitted_count += 1
            elif event_row.formal_signal_status == "blocked":
                blocked_count += 1
            else:
                deferred_count += 1

    materialized_signal_count = inserted_count + reused_count + rematerialized_count
    return AlphaFormalSignalBuildSummary(
        run_id=run_id,
        producer_name=producer_name,
        producer_version=producer_version,
        execution_mode="bounded",
        signal_contract_version=signal_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in trigger_rows}),
        claimed_scope_count=len({row.instrument for row in trigger_rows}),
        candidate_trigger_count=len(trigger_rows),
        materialized_signal_count=materialized_signal_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        missing_context_count=missing_context_count,
        admitted_count=admitted_count,
        blocked_count=blocked_count,
        deferred_count=deferred_count,
        queue_enqueued_count=0,
        queue_claimed_count=len({row.instrument for row in trigger_rows}),
        checkpoint_upserted_count=0,
        alpha_ledger_path=str(alpha_path),
        filter_ledger_path=str(filter_path),
        structure_ledger_path=str(structure_path),
        malf_ledger_path=str(malf_path),
        source_trigger_table=source_trigger_table,
        source_family_table=source_family_table,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        source_wave_life_table=source_wave_life_table,
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
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    source_trigger_table: str,
    source_family_table: str,
    source_context_table: str,
    signal_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {ALPHA_FORMAL_SIGNAL_RUN_TABLE} (
            run_id,
            producer_name,
            producer_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_trigger_table,
            source_family_table,
            source_context_table,
            signal_contract_version,
            notes
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            producer_name,
            producer_version,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_trigger_table,
            source_family_table,
            source_context_table,
            signal_contract_version,
            "bounded alpha formal signal producer",
        ],
    )


def _build_formal_signal_event_row(
    *,
    run_id: str,
    trigger_row: _TriggerRow,
    family_row: _FamilyRow | None,
    context_row: _ContextRow,
    signal_contract_version: str,
) -> _FormalSignalEventRow:
    decision_code, action_owner, decision_note = _derive_stage_percentile_decision(
        malf_phase_bucket=None if family_row is None else family_row.malf_phase_bucket,
        termination_risk_bucket=context_row.termination_risk_bucket,
    )
    (
        formal_signal_status,
        admission_verdict_code,
        admission_verdict_owner,
        admission_reason_code,
        admission_audit_note,
        filter_gate_code,
        filter_reject_reason_code,
    ) = _derive_formal_signal_admission(
        trigger_admissible=context_row.trigger_admissible,
        family_role=None if family_row is None else family_row.family_role,
        malf_alignment=None if family_row is None else family_row.malf_alignment,
        stage_percentile_decision_code=decision_code,
        stage_percentile_action_owner=action_owner,
        stage_percentile_note=decision_note,
        filter_reject_reason_code=context_row.filter_reject_reason_code,
        filter_admission_notes=context_row.filter_admission_notes,
    )
    signal_nk = _build_signal_nk(
        instrument=trigger_row.instrument,
        signal_date=trigger_row.signal_date,
        asof_date=trigger_row.asof_date,
        trigger_family=trigger_row.trigger_family,
        trigger_type=trigger_row.trigger_type,
        pattern_code=trigger_row.pattern_code,
        source_trigger_event_nk=trigger_row.source_trigger_event_nk,
        signal_contract_version=signal_contract_version,
    )
    return _FormalSignalEventRow(
        signal_nk=signal_nk,
        instrument=trigger_row.instrument,
        signal_date=trigger_row.signal_date,
        asof_date=trigger_row.asof_date,
        trigger_family=trigger_row.trigger_family,
        trigger_type=trigger_row.trigger_type,
        pattern_code=trigger_row.pattern_code,
        formal_signal_status=formal_signal_status,
        trigger_admissible=context_row.trigger_admissible,
        admission_verdict_code=admission_verdict_code,
        admission_verdict_owner=admission_verdict_owner,
        admission_reason_code=admission_reason_code,
        admission_audit_note=admission_audit_note,
        filter_gate_code=filter_gate_code,
        filter_reject_reason_code=filter_reject_reason_code,
        major_state=context_row.major_state,
        trend_direction=context_row.trend_direction,
        reversal_stage=context_row.reversal_stage,
        wave_id=context_row.wave_id,
        current_hh_count=context_row.current_hh_count,
        current_ll_count=context_row.current_ll_count,
        malf_context_4=context_row.malf_context_4,
        lifecycle_rank_high=context_row.lifecycle_rank_high,
        lifecycle_rank_total=context_row.lifecycle_rank_total,
        daily_source_context_nk=context_row.daily_source_context_nk,
        weekly_major_state=context_row.weekly_major_state,
        weekly_trend_direction=context_row.weekly_trend_direction,
        weekly_reversal_stage=context_row.weekly_reversal_stage,
        weekly_source_context_nk=context_row.weekly_source_context_nk,
        monthly_major_state=context_row.monthly_major_state,
        monthly_trend_direction=context_row.monthly_trend_direction,
        monthly_reversal_stage=context_row.monthly_reversal_stage,
        monthly_source_context_nk=context_row.monthly_source_context_nk,
        source_family_event_nk=None if family_row is None else family_row.source_family_event_nk,
        family_code=None if family_row is None else family_row.family_code,
        source_family_contract_version=None if family_row is None else family_row.source_family_contract_version,
        family_role=None if family_row is None else family_row.family_role,
        family_bias=None if family_row is None else family_row.family_bias,
        malf_alignment=None if family_row is None else family_row.malf_alignment,
        malf_phase_bucket=None if family_row is None else family_row.malf_phase_bucket,
        family_source_context_fingerprint=None if family_row is None else family_row.family_source_context_fingerprint,
        wave_life_percentile=context_row.wave_life_percentile,
        remaining_life_bars_p50=context_row.remaining_life_bars_p50,
        remaining_life_bars_p75=context_row.remaining_life_bars_p75,
        termination_risk_bucket=context_row.termination_risk_bucket,
        stage_percentile_decision_code=decision_code,
        stage_percentile_action_owner=action_owner,
        stage_percentile_note=decision_note,
        stage_percentile_contract_version=DEFAULT_ALPHA_STAGE_PERCENTILE_CONTRACT_VERSION,
        source_trigger_event_nk=trigger_row.source_trigger_event_nk,
        signal_contract_version=signal_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _upsert_formal_signal_event(
    connection: duckdb.DuckDBPyConnection,
    *,
    event_row: _FormalSignalEventRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            formal_signal_status,
            trigger_admissible,
            admission_verdict_code,
            admission_verdict_owner,
            admission_reason_code,
            admission_audit_note,
            filter_gate_code,
            filter_reject_reason_code,
            major_state,
            trend_direction,
            reversal_stage,
            wave_id,
            current_hh_count,
            current_ll_count,
            malf_context_4,
            lifecycle_rank_high,
            lifecycle_rank_total,
            daily_source_context_nk,
            weekly_major_state,
            weekly_trend_direction,
            weekly_reversal_stage,
            weekly_source_context_nk,
            monthly_major_state,
            monthly_trend_direction,
            monthly_reversal_stage,
            monthly_source_context_nk,
            source_family_event_nk,
            family_code,
            source_family_contract_version,
            family_role,
            family_bias,
            malf_alignment,
            malf_phase_bucket,
            family_source_context_fingerprint,
            wave_life_percentile,
            remaining_life_bars_p50,
            remaining_life_bars_p75,
            termination_risk_bucket,
            stage_percentile_decision_code,
            stage_percentile_action_owner,
            stage_percentile_note,
            stage_percentile_contract_version,
            first_seen_run_id
        FROM {ALPHA_FORMAL_SIGNAL_EVENT_TABLE}
        WHERE signal_nk = ?
        """,
        [event_row.signal_nk],
    ).fetchone()

    write_values = [
        event_row.formal_signal_status,
        event_row.trigger_admissible,
        event_row.admission_verdict_code,
        event_row.admission_verdict_owner,
        event_row.admission_reason_code,
        event_row.admission_audit_note,
        event_row.filter_gate_code,
        event_row.filter_reject_reason_code,
        event_row.major_state,
        event_row.trend_direction,
        event_row.reversal_stage,
        event_row.wave_id,
        event_row.current_hh_count,
        event_row.current_ll_count,
        event_row.malf_context_4,
        event_row.lifecycle_rank_high,
        event_row.lifecycle_rank_total,
        event_row.daily_source_context_nk,
        event_row.weekly_major_state,
        event_row.weekly_trend_direction,
        event_row.weekly_reversal_stage,
        event_row.weekly_source_context_nk,
        event_row.monthly_major_state,
        event_row.monthly_trend_direction,
        event_row.monthly_reversal_stage,
        event_row.monthly_source_context_nk,
        event_row.source_family_event_nk,
        event_row.family_code,
        event_row.source_family_contract_version,
        event_row.family_role,
        event_row.family_bias,
        event_row.malf_alignment,
        event_row.malf_phase_bucket,
        event_row.family_source_context_fingerprint,
        event_row.wave_life_percentile,
        event_row.remaining_life_bars_p50,
        event_row.remaining_life_bars_p75,
        event_row.termination_risk_bucket,
        event_row.stage_percentile_decision_code,
        event_row.stage_percentile_action_owner,
        event_row.stage_percentile_note,
        event_row.stage_percentile_contract_version,
    ]

    if existing_row is None:
        placeholders = ', '.join(['?'] * 53)
        connection.execute(
            f"""
            INSERT INTO {ALPHA_FORMAL_SIGNAL_EVENT_TABLE} (
                signal_nk,
                instrument,
                signal_date,
                asof_date,
                trigger_family,
                trigger_type,
                pattern_code,
                formal_signal_status,
                trigger_admissible,
                admission_verdict_code,
                admission_verdict_owner,
                admission_reason_code,
                admission_audit_note,
                filter_gate_code,
                filter_reject_reason_code,
                major_state,
                trend_direction,
                reversal_stage,
                wave_id,
                current_hh_count,
                current_ll_count,
                malf_context_4,
                lifecycle_rank_high,
                lifecycle_rank_total,
                daily_source_context_nk,
                weekly_major_state,
                weekly_trend_direction,
                weekly_reversal_stage,
                weekly_source_context_nk,
                monthly_major_state,
                monthly_trend_direction,
                monthly_reversal_stage,
                monthly_source_context_nk,
                source_family_event_nk,
                family_code,
                source_family_contract_version,
                family_role,
                family_bias,
                malf_alignment,
                malf_phase_bucket,
                family_source_context_fingerprint,
                wave_life_percentile,
                remaining_life_bars_p50,
                remaining_life_bars_p75,
                termination_risk_bucket,
                stage_percentile_decision_code,
                stage_percentile_action_owner,
                stage_percentile_note,
                stage_percentile_contract_version,
                source_trigger_event_nk,
                signal_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES ({placeholders})
            """,
            [
                event_row.signal_nk,
                event_row.instrument,
                event_row.signal_date,
                event_row.asof_date,
                event_row.trigger_family,
                event_row.trigger_type,
                event_row.pattern_code,
                *write_values,
                event_row.source_trigger_event_nk,
                event_row.signal_contract_version,
                event_row.first_seen_run_id,
                event_row.last_materialized_run_id,
            ],
        )
        return 'inserted'

    existing_fingerprint = (
        _normalize_formal_signal_status(existing_row[0]),
        bool(existing_row[1]),
        _normalize_admission_verdict_code(existing_row[2]),
        _normalize_admission_verdict_owner(existing_row[3]),
        _normalize_optional_nullable_str(existing_row[4]),
        _normalize_optional_nullable_str(existing_row[5]),
        _normalize_optional_str(existing_row[6]),
        _normalize_optional_nullable_str(existing_row[7]),
        _normalize_optional_str(existing_row[8]),
        _normalize_optional_str(existing_row[9]),
        _normalize_optional_str(existing_row[10]),
        _normalize_optional_int(existing_row[11]),
        _normalize_optional_int(existing_row[12]),
        _normalize_optional_int(existing_row[13]),
        _normalize_optional_str(existing_row[14]),
        _normalize_optional_int(existing_row[15]),
        _normalize_optional_int(existing_row[16]),
        _normalize_optional_nullable_str(existing_row[17]),
        _normalize_optional_nullable_str(existing_row[18]),
        _normalize_optional_nullable_str(existing_row[19]),
        _normalize_optional_nullable_str(existing_row[20]),
        _normalize_optional_nullable_str(existing_row[21]),
        _normalize_optional_nullable_str(existing_row[22]),
        _normalize_optional_nullable_str(existing_row[23]),
        _normalize_optional_nullable_str(existing_row[24]),
        _normalize_optional_nullable_str(existing_row[25]),
        _normalize_optional_nullable_str(existing_row[26]),
        _normalize_optional_nullable_str(existing_row[27]),
        _normalize_optional_nullable_str(existing_row[28]),
        _normalize_optional_nullable_str(existing_row[29]),
        _normalize_optional_nullable_str(existing_row[30]),
        _normalize_optional_nullable_str(existing_row[31]),
        _normalize_optional_nullable_str(existing_row[32]),
        _normalize_optional_nullable_str(existing_row[33]),
        _normalize_optional_float(existing_row[34]),
        _normalize_optional_float(existing_row[35]),
        _normalize_optional_float(existing_row[36]),
        _normalize_optional_nullable_str(existing_row[37]),
        _normalize_optional_str(existing_row[38]),
        _normalize_optional_str(existing_row[39]),
        _normalize_optional_str(existing_row[40]),
        _normalize_optional_str(existing_row[41]),
    )
    new_fingerprint = tuple(write_values)
    first_seen_run_id = str(existing_row[-1]) if existing_row[-1] is not None else event_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {ALPHA_FORMAL_SIGNAL_EVENT_TABLE}
        SET
            formal_signal_status = ?,
            trigger_admissible = ?,
            admission_verdict_code = ?,
            admission_verdict_owner = ?,
            admission_reason_code = ?,
            admission_audit_note = ?,
            filter_gate_code = ?,
            filter_reject_reason_code = ?,
            major_state = ?,
            trend_direction = ?,
            reversal_stage = ?,
            wave_id = ?,
            current_hh_count = ?,
            current_ll_count = ?,
            malf_context_4 = ?,
            lifecycle_rank_high = ?,
            lifecycle_rank_total = ?,
            daily_source_context_nk = ?,
            weekly_major_state = ?,
            weekly_trend_direction = ?,
            weekly_reversal_stage = ?,
            weekly_source_context_nk = ?,
            monthly_major_state = ?,
            monthly_trend_direction = ?,
            monthly_reversal_stage = ?,
            monthly_source_context_nk = ?,
            source_family_event_nk = ?,
            family_code = ?,
            source_family_contract_version = ?,
            family_role = ?,
            family_bias = ?,
            malf_alignment = ?,
            malf_phase_bucket = ?,
            family_source_context_fingerprint = ?,
            wave_life_percentile = ?,
            remaining_life_bars_p50 = ?,
            remaining_life_bars_p75 = ?,
            termination_risk_bucket = ?,
            stage_percentile_decision_code = ?,
            stage_percentile_action_owner = ?,
            stage_percentile_note = ?,
            stage_percentile_contract_version = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE signal_nk = ?
        """,
        [
            *write_values,
            first_seen_run_id,
            event_row.last_materialized_run_id,
            event_row.signal_nk,
        ],
    )
    if existing_fingerprint == new_fingerprint:
        return 'reused'
    return 'rematerialized'


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: AlphaFormalSignalBuildSummary,
) -> None:
    _update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        summary_payload=summary.as_dict(),
    )


def _update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_FORMAL_SIGNAL_RUN_TABLE}
        SET
            run_status = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )
