"""承接 `filter snapshot` runner 的落表、run 审计与可重算物化。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from mlq.filter.bootstrap import (
    FILTER_RUN_SNAPSHOT_TABLE,
    FILTER_RUN_TABLE,
    FILTER_SNAPSHOT_TABLE,
)
from mlq.filter.filter_shared import (
    FilterSnapshotBuildSummary,
    FILTER_ALLOWED_MARKET_TYPES,
    FILTER_ALLOWED_SECURITY_TYPES,
    _FilterSnapshotRow,
    _ObjectiveStatusInputRow,
    _StructureSnapshotInputRow,
    derive_filter_gate_decision,
)
from mlq.filter.filter_source import _resolve_objective_status_for_signal


def _materialize_filter_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    structure_rows: list[_StructureSnapshotInputRow],
    context_presence: set[tuple[str, date]],
    objective_status_rows: dict[str, list[_ObjectiveStatusInputRow]],
    filter_contract_version: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    filter_path: Path,
    structure_path: Path,
    malf_path: Path,
    source_structure_table: str,
    source_context_table: str,
    source_timeframe: str,
    batch_size: int,
) -> FilterSnapshotBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    missing_context_count = 0
    admissible_count = 0
    blocked_count = 0

    for structure_batch in _bounded_by_instrument_batches(structure_rows, batch_size=batch_size):
        for structure_row in structure_batch:
            has_context = (structure_row.instrument, structure_row.signal_date) in context_presence
            if not has_context:
                missing_context_count += 1
            filter_row = _build_filter_snapshot_row(
                run_id=run_id,
                structure_row=structure_row,
                has_context=has_context,
                objective_status_row=_resolve_objective_status_for_signal(
                    objective_status_rows,
                    instrument=structure_row.instrument,
                    signal_date=structure_row.signal_date,
                ),
                filter_contract_version=filter_contract_version,
            )
            materialization_action = _upsert_filter_snapshot(connection, filter_row=filter_row)
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {FILTER_RUN_SNAPSHOT_TABLE} (
                    run_id,
                    filter_snapshot_nk,
                    materialization_action,
                    trigger_admissible,
                    filter_gate_code,
                    filter_reject_reason_code,
                    primary_blocking_condition
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    filter_row.filter_snapshot_nk,
                    materialization_action,
                    filter_row.trigger_admissible,
                    filter_row.filter_gate_code,
                    filter_row.filter_reject_reason_code,
                    filter_row.primary_blocking_condition,
                ],
            )
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

            if filter_row.trigger_admissible:
                admissible_count += 1
            else:
                blocked_count += 1

    materialized_snapshot_count = inserted_count + reused_count + rematerialized_count
    return FilterSnapshotBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        execution_mode="bounded",
        filter_contract_version=filter_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in structure_rows}),
        claimed_scope_count=len({row.instrument for row in structure_rows}),
        candidate_structure_count=len(structure_rows),
        materialized_snapshot_count=materialized_snapshot_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        missing_context_count=missing_context_count,
        admissible_count=admissible_count,
        blocked_count=blocked_count,
        queue_enqueued_count=0,
        queue_claimed_count=len({row.instrument for row in structure_rows}),
        checkpoint_upserted_count=0,
        filter_ledger_path=str(filter_path),
        structure_ledger_path=str(structure_path),
        malf_ledger_path=str(malf_path),
        source_structure_table=source_structure_table,
        source_context_table=source_context_table,
        source_timeframe=source_timeframe,
    )


def _bounded_by_instrument_batches(
    structure_rows: list[_StructureSnapshotInputRow],
    *,
    batch_size: int,
) -> list[list[_StructureSnapshotInputRow]]:
    if not structure_rows:
        return []
    normalized_batch_size = max(int(batch_size), 1)
    batches: list[list[_StructureSnapshotInputRow]] = []
    current_batch: list[_StructureSnapshotInputRow] = []
    current_instruments: set[str] = set()
    for row in structure_rows:
        if (
            current_batch
            and row.instrument not in current_instruments
            and len(current_instruments) >= normalized_batch_size
        ):
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
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    source_structure_table: str,
    source_context_table: str,
    filter_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {FILTER_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_structure_table,
            source_context_table,
            filter_contract_version
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_structure_table,
            source_context_table,
            filter_contract_version,
        ],
    )


def _build_filter_snapshot_row(
    *,
    run_id: str,
    structure_row: _StructureSnapshotInputRow,
    has_context: bool,
    objective_status_row: _ObjectiveStatusInputRow | None,
    filter_contract_version: str,
) -> _FilterSnapshotRow:
    gate_decision = derive_filter_gate_decision(
        is_suspended_or_unresumed=(
            False if objective_status_row is None else objective_status_row.is_suspended_or_unresumed
        ),
        is_risk_warning_excluded=(
            False if objective_status_row is None else objective_status_row.is_risk_warning_excluded
        ),
        is_delisting_arrangement=(
            False if objective_status_row is None else objective_status_row.is_delisting_arrangement
        ),
        is_security_type_out_of_universe=(
            objective_status_row is not None
            and objective_status_row.security_type is not None
            and objective_status_row.security_type not in FILTER_ALLOWED_SECURITY_TYPES
        ),
        is_market_type_out_of_universe=(
            objective_status_row is not None
            and objective_status_row.market_type is not None
            and objective_status_row.market_type not in FILTER_ALLOWED_MARKET_TYPES
        ),
    )
    admission_notes: list[str] = []
    if structure_row.structure_progress_state == "failed":
        admission_notes.append("structure_progress=failed 仅保留结构观察，不在 filter 层硬拦截")
    elif structure_row.reversal_stage in {"trigger", "hold"} and structure_row.trend_direction == "down":
        admission_notes.append(f"reversal_stage_pending={structure_row.reversal_stage}")
    if structure_row.structure_progress_state in {"stalled", "unknown"} and gate_decision.trigger_admissible:
        admission_notes.append(f"structure_progress={structure_row.structure_progress_state}")
    admission_notes.append(
        f"canonical_context={structure_row.major_state}/{structure_row.trend_direction}/{structure_row.reversal_stage}"
    )
    if structure_row.weekly_major_state or structure_row.monthly_major_state:
        admission_notes.append(
            "read_only_context="
            f"W:{structure_row.weekly_major_state}/{structure_row.weekly_reversal_stage};"
            f"M:{structure_row.monthly_major_state}/{structure_row.monthly_reversal_stage}"
        )
    if not has_context:
        admission_notes.append("缺少执行上下文，仅保留研究观察并继续落表")
    if structure_row.break_confirmation_status == "confirmed":
        admission_notes.append("break_confirmation=confirmed 仅 sidecar 提示")
    if structure_row.exhaustion_risk_bucket in {"elevated", "high"}:
        admission_notes.append(f"exhaustion_risk={structure_row.exhaustion_risk_bucket}")
    if objective_status_row is None:
        admission_notes.append("objective_status=missing")
    else:
        objective_parts = []
        if objective_status_row.market_type is not None:
            objective_parts.append(f"market_type={objective_status_row.market_type}")
        if objective_status_row.security_type is not None:
            objective_parts.append(f"security_type={objective_status_row.security_type}")
        if objective_status_row.suspension_status is not None:
            objective_parts.append(f"suspension_status={objective_status_row.suspension_status}")
        if objective_status_row.risk_warning_status is not None:
            objective_parts.append(f"risk_warning_status={objective_status_row.risk_warning_status}")
        if objective_status_row.delisting_status is not None:
            objective_parts.append(f"delisting_status={objective_status_row.delisting_status}")
        if objective_status_row.source_request_nk is not None:
            objective_parts.append(f"objective_source_request={objective_status_row.source_request_nk}")
        if objective_parts:
            admission_notes.append("objective_status=" + ",".join(objective_parts))

    primary_blocking_condition = gate_decision.filter_reject_reason_code
    blocking_conditions_json = json.dumps(
        list(gate_decision.blocking_conditions),
        ensure_ascii=False,
        sort_keys=True,
    )
    filter_snapshot_nk = _build_filter_snapshot_nk(
        structure_snapshot_nk=structure_row.structure_snapshot_nk,
        source_context_nk=structure_row.source_context_nk,
        filter_contract_version=filter_contract_version,
    )
    return _FilterSnapshotRow(
        filter_snapshot_nk=filter_snapshot_nk,
        structure_snapshot_nk=structure_row.structure_snapshot_nk,
        instrument=structure_row.instrument,
        signal_date=structure_row.signal_date,
        asof_date=structure_row.asof_date,
        major_state=structure_row.major_state,
        trend_direction=structure_row.trend_direction,
        reversal_stage=structure_row.reversal_stage,
        wave_id=structure_row.wave_id,
        current_hh_count=structure_row.current_hh_count,
        current_ll_count=structure_row.current_ll_count,
        daily_major_state=structure_row.daily_major_state,
        daily_trend_direction=structure_row.daily_trend_direction,
        daily_reversal_stage=structure_row.daily_reversal_stage,
        daily_wave_id=structure_row.daily_wave_id,
        daily_current_hh_count=structure_row.daily_current_hh_count,
        daily_current_ll_count=structure_row.daily_current_ll_count,
        daily_source_context_nk=structure_row.daily_source_context_nk,
        weekly_major_state=structure_row.weekly_major_state,
        weekly_trend_direction=structure_row.weekly_trend_direction,
        weekly_reversal_stage=structure_row.weekly_reversal_stage,
        weekly_wave_id=structure_row.weekly_wave_id,
        weekly_current_hh_count=structure_row.weekly_current_hh_count,
        weekly_current_ll_count=structure_row.weekly_current_ll_count,
        weekly_source_context_nk=structure_row.weekly_source_context_nk,
        monthly_major_state=structure_row.monthly_major_state,
        monthly_trend_direction=structure_row.monthly_trend_direction,
        monthly_reversal_stage=structure_row.monthly_reversal_stage,
        monthly_wave_id=structure_row.monthly_wave_id,
        monthly_current_hh_count=structure_row.monthly_current_hh_count,
        monthly_current_ll_count=structure_row.monthly_current_ll_count,
        monthly_source_context_nk=structure_row.monthly_source_context_nk,
        trigger_admissible=gate_decision.trigger_admissible,
        filter_gate_code=gate_decision.filter_gate_code,
        filter_reject_reason_code=gate_decision.filter_reject_reason_code,
        primary_blocking_condition=primary_blocking_condition,
        blocking_conditions_json=blocking_conditions_json,
        admission_notes="; ".join(admission_notes) if admission_notes else None,
        break_confirmation_status=structure_row.break_confirmation_status,
        break_confirmation_ref=structure_row.break_confirmation_ref,
        stats_snapshot_nk=structure_row.stats_snapshot_nk,
        exhaustion_risk_bucket=structure_row.exhaustion_risk_bucket,
        reversal_probability_bucket=structure_row.reversal_probability_bucket,
        source_context_nk=structure_row.source_context_nk,
        filter_contract_version=filter_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _build_filter_snapshot_nk(
    *,
    structure_snapshot_nk: str,
    source_context_nk: str,
    filter_contract_version: str,
) -> str:
    return "|".join([structure_snapshot_nk, source_context_nk, filter_contract_version])


def _upsert_filter_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    filter_row: _FilterSnapshotRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            major_state, trend_direction, reversal_stage, wave_id, current_hh_count, current_ll_count,
            daily_major_state, daily_trend_direction, daily_reversal_stage, daily_wave_id, daily_current_hh_count, daily_current_ll_count, daily_source_context_nk,
            weekly_major_state, weekly_trend_direction, weekly_reversal_stage, weekly_wave_id, weekly_current_hh_count, weekly_current_ll_count, weekly_source_context_nk,
            monthly_major_state, monthly_trend_direction, monthly_reversal_stage, monthly_wave_id, monthly_current_hh_count, monthly_current_ll_count, monthly_source_context_nk,
            trigger_admissible, filter_gate_code, filter_reject_reason_code, primary_blocking_condition, blocking_conditions_json, admission_notes,
            break_confirmation_status, break_confirmation_ref, stats_snapshot_nk, exhaustion_risk_bucket, reversal_probability_bucket,
            first_seen_run_id
        FROM {FILTER_SNAPSHOT_TABLE}
        WHERE filter_snapshot_nk = ?
        """,
        [filter_row.filter_snapshot_nk],
    ).fetchone()
    new_fingerprint = (
        filter_row.major_state,
        filter_row.trend_direction,
        filter_row.reversal_stage,
        filter_row.wave_id,
        filter_row.current_hh_count,
        filter_row.current_ll_count,
        filter_row.daily_major_state,
        filter_row.daily_trend_direction,
        filter_row.daily_reversal_stage,
        filter_row.daily_wave_id,
        filter_row.daily_current_hh_count,
        filter_row.daily_current_ll_count,
        filter_row.daily_source_context_nk,
        filter_row.weekly_major_state,
        filter_row.weekly_trend_direction,
        filter_row.weekly_reversal_stage,
        filter_row.weekly_wave_id,
        filter_row.weekly_current_hh_count,
        filter_row.weekly_current_ll_count,
        filter_row.weekly_source_context_nk,
        filter_row.monthly_major_state,
        filter_row.monthly_trend_direction,
        filter_row.monthly_reversal_stage,
        filter_row.monthly_wave_id,
        filter_row.monthly_current_hh_count,
        filter_row.monthly_current_ll_count,
        filter_row.monthly_source_context_nk,
        filter_row.trigger_admissible,
        filter_row.filter_gate_code,
        filter_row.filter_reject_reason_code,
        filter_row.primary_blocking_condition,
        filter_row.blocking_conditions_json,
        filter_row.admission_notes,
        filter_row.break_confirmation_status,
        filter_row.break_confirmation_ref,
        filter_row.stats_snapshot_nk,
        filter_row.exhaustion_risk_bucket,
        filter_row.reversal_probability_bucket,
    )
    insert_values = [
        filter_row.filter_snapshot_nk,
        filter_row.structure_snapshot_nk,
        filter_row.instrument,
        filter_row.signal_date,
        filter_row.asof_date,
        filter_row.major_state,
        filter_row.trend_direction,
        filter_row.reversal_stage,
        filter_row.wave_id,
        filter_row.current_hh_count,
        filter_row.current_ll_count,
        filter_row.daily_major_state,
        filter_row.daily_trend_direction,
        filter_row.daily_reversal_stage,
        filter_row.daily_wave_id,
        filter_row.daily_current_hh_count,
        filter_row.daily_current_ll_count,
        filter_row.daily_source_context_nk,
        filter_row.weekly_major_state,
        filter_row.weekly_trend_direction,
        filter_row.weekly_reversal_stage,
        filter_row.weekly_wave_id,
        filter_row.weekly_current_hh_count,
        filter_row.weekly_current_ll_count,
        filter_row.weekly_source_context_nk,
        filter_row.monthly_major_state,
        filter_row.monthly_trend_direction,
        filter_row.monthly_reversal_stage,
        filter_row.monthly_wave_id,
        filter_row.monthly_current_hh_count,
        filter_row.monthly_current_ll_count,
        filter_row.monthly_source_context_nk,
        filter_row.trigger_admissible,
        filter_row.filter_gate_code,
        filter_row.filter_reject_reason_code,
        filter_row.primary_blocking_condition,
        filter_row.blocking_conditions_json,
        filter_row.admission_notes,
        filter_row.break_confirmation_status,
        filter_row.break_confirmation_ref,
        filter_row.stats_snapshot_nk,
        filter_row.exhaustion_risk_bucket,
        filter_row.reversal_probability_bucket,
        filter_row.source_context_nk,
        filter_row.filter_contract_version,
        filter_row.first_seen_run_id,
        filter_row.last_materialized_run_id,
    ]
    if existing_row is None:
        placeholders = ", ".join("?" for _ in insert_values)
        connection.execute(
            f"""
            INSERT INTO {FILTER_SNAPSHOT_TABLE} (
                filter_snapshot_nk, structure_snapshot_nk, instrument, signal_date, asof_date,
                major_state, trend_direction, reversal_stage, wave_id, current_hh_count, current_ll_count,
                daily_major_state, daily_trend_direction, daily_reversal_stage, daily_wave_id, daily_current_hh_count, daily_current_ll_count, daily_source_context_nk,
                weekly_major_state, weekly_trend_direction, weekly_reversal_stage, weekly_wave_id, weekly_current_hh_count, weekly_current_ll_count, weekly_source_context_nk,
                monthly_major_state, monthly_trend_direction, monthly_reversal_stage, monthly_wave_id, monthly_current_hh_count, monthly_current_ll_count, monthly_source_context_nk,
                trigger_admissible, filter_gate_code, filter_reject_reason_code, primary_blocking_condition, blocking_conditions_json, admission_notes,
                break_confirmation_status, break_confirmation_ref, stats_snapshot_nk, exhaustion_risk_bucket, reversal_probability_bucket,
                source_context_nk, filter_contract_version, first_seen_run_id, last_materialized_run_id
            )
            VALUES ({placeholders})
            """,
            insert_values,
        )
        return "inserted"

    first_seen_run_id = str(existing_row[-1]) if existing_row[-1] is not None else filter_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {FILTER_SNAPSHOT_TABLE}
        SET
            major_state = ?, trend_direction = ?, reversal_stage = ?, wave_id = ?, current_hh_count = ?, current_ll_count = ?,
            daily_major_state = ?, daily_trend_direction = ?, daily_reversal_stage = ?, daily_wave_id = ?, daily_current_hh_count = ?, daily_current_ll_count = ?, daily_source_context_nk = ?,
            weekly_major_state = ?, weekly_trend_direction = ?, weekly_reversal_stage = ?, weekly_wave_id = ?, weekly_current_hh_count = ?, weekly_current_ll_count = ?, weekly_source_context_nk = ?,
            monthly_major_state = ?, monthly_trend_direction = ?, monthly_reversal_stage = ?, monthly_wave_id = ?, monthly_current_hh_count = ?, monthly_current_ll_count = ?, monthly_source_context_nk = ?,
            trigger_admissible = ?, filter_gate_code = ?, filter_reject_reason_code = ?, primary_blocking_condition = ?, blocking_conditions_json = ?, admission_notes = ?,
            break_confirmation_status = ?, break_confirmation_ref = ?, stats_snapshot_nk = ?, exhaustion_risk_bucket = ?, reversal_probability_bucket = ?,
            first_seen_run_id = ?, last_materialized_run_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE filter_snapshot_nk = ?
        """,
        [*new_fingerprint, first_seen_run_id, filter_row.last_materialized_run_id, filter_row.filter_snapshot_nk],
    )
    return "reused" if tuple(existing_row[:-1]) == new_fingerprint else "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: FilterSnapshotBuildSummary,
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
        UPDATE {FILTER_RUN_TABLE}
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
