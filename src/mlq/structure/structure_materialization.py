"""`structure snapshot` 的落表物化逻辑。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from mlq.structure.bootstrap import STRUCTURE_RUN_SNAPSHOT_TABLE, STRUCTURE_RUN_TABLE, STRUCTURE_SNAPSHOT_TABLE
from mlq.structure.structure_shared import (
    StructureSnapshotBuildSummary,
    _BreakConfirmationRow,
    _MultiTimeframeContextRow,
    _StatsSnapshotRow,
    _StructureInputRow,
    _StructureSnapshotRow,
    _build_context_series_index,
    _build_structure_snapshot_nk,
    _lookup_latest_context_row,
)
from mlq.structure.structure_source import (
    _load_break_confirmation_rows,
    _load_context_rows,
    _load_read_only_context_rows,
    _load_stats_snapshot_rows,
    _load_structure_input_rows,
    _resolve_optional_sidecar_table,
)


def _materialize_structure_scope(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    malf_path: Path,
    instrument: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    limit: int,
    batch_size: int,
    source_context_table: str,
    source_structure_input_table: str,
    source_break_confirmation_table: str | None,
    source_stats_table: str | None,
    source_timeframe: str,
    structure_contract_version: str,
    runner_name: str,
    runner_version: str,
    structure_path: Path,
) -> StructureSnapshotBuildSummary:
    actual_source_context_table = source_context_table
    actual_source_input_table = source_structure_input_table
    actual_break_confirmation_table = _resolve_optional_sidecar_table(
        malf_path=malf_path,
        requested_table=source_break_confirmation_table,
        fallback_table=None,
    )
    actual_stats_table = _resolve_optional_sidecar_table(
        malf_path=malf_path,
        requested_table=source_stats_table,
        fallback_table=None,
    )
    input_rows = _load_structure_input_rows(
        malf_path=malf_path,
        table_name=actual_source_input_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=(instrument,),
        limit=limit,
        timeframe=source_timeframe,
    )
    context_rows = _load_context_rows(
        malf_path=malf_path,
        table_name=actual_source_context_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=(instrument,),
        timeframe=source_timeframe,
    )
    daily_context_map = {
        (row.instrument, row.signal_date, row.asof_date): row
        for row in context_rows
    }
    weekly_context_index = _build_context_series_index(
        _load_read_only_context_rows(
            malf_path=malf_path,
            table_name=actual_source_context_table,
            signal_end_date=signal_end_date,
            instruments=(instrument,),
            timeframe="W",
        )
    )
    monthly_context_index = _build_context_series_index(
        _load_read_only_context_rows(
            malf_path=malf_path,
            table_name=actual_source_context_table,
            signal_end_date=signal_end_date,
            instruments=(instrument,),
            timeframe="M",
        )
    )
    break_confirmation_map = _load_break_confirmation_rows(
        malf_path=malf_path,
        table_name=actual_break_confirmation_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=(instrument,),
        timeframe=source_timeframe,
    )
    stats_snapshot_map = _load_stats_snapshot_rows(
        malf_path=malf_path,
        table_name=actual_stats_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=(instrument,),
        timeframe=source_timeframe,
    )
    return _materialize_structure_rows(
        connection=connection,
        run_id=run_id,
        input_rows=input_rows,
        daily_context_map=daily_context_map,
        weekly_context_index=weekly_context_index,
        monthly_context_index=monthly_context_index,
        break_confirmation_map=break_confirmation_map,
        stats_snapshot_map=stats_snapshot_map,
        structure_contract_version=structure_contract_version,
        runner_name=runner_name,
        runner_version=runner_version,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        structure_path=structure_path,
        malf_path=malf_path,
        source_context_table=actual_source_context_table,
        source_structure_input_table=actual_source_input_table,
        source_break_confirmation_table=actual_break_confirmation_table,
        source_stats_table=actual_stats_table,
        source_timeframe=source_timeframe,
        batch_size=batch_size,
    )


def _materialize_structure_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    input_rows: list[_StructureInputRow],
    daily_context_map: dict[tuple[str, date, date], object],
    weekly_context_index: dict[str, tuple[list[date], list[object]]],
    monthly_context_index: dict[str, tuple[list[date], list[object]]],
    break_confirmation_map: dict[tuple[str, date], _BreakConfirmationRow],
    stats_snapshot_map: dict[tuple[str, date, date], _StatsSnapshotRow],
    structure_contract_version: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    structure_path: Path,
    malf_path: Path,
    source_context_table: str,
    source_structure_input_table: str,
    source_break_confirmation_table: str | None,
    source_stats_table: str | None,
    source_timeframe: str,
    batch_size: int,
) -> StructureSnapshotBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    missing_context_count = 0
    advancing_count = 0
    stalled_count = 0
    failed_count = 0
    unknown_count = 0

    for input_batch in _bounded_by_instrument_batches(input_rows, batch_size=batch_size):
        for input_row in input_batch:
            daily_context_row = daily_context_map.get((input_row.instrument, input_row.signal_date, input_row.asof_date))
            if daily_context_row is None:
                # `structure` 必须挂在正式上下文上，缺上下文时宁可跳过，也不伪造自然键。
                missing_context_count += 1
                continue
            context_row = _MultiTimeframeContextRow(
                daily=daily_context_row,
                weekly=_lookup_latest_context_row(
                    weekly_context_index,
                    instrument=input_row.instrument,
                    asof_date=input_row.asof_date,
                ),
                monthly=_lookup_latest_context_row(
                    monthly_context_index,
                    instrument=input_row.instrument,
                    asof_date=input_row.asof_date,
                ),
            )
            snapshot_row = _build_structure_snapshot_row(
                run_id=run_id,
                input_row=input_row,
                context_row=context_row,
                break_confirmation_row=break_confirmation_map.get((input_row.instrument, input_row.signal_date)),
                stats_snapshot_row=stats_snapshot_map.get((input_row.instrument, input_row.signal_date, input_row.asof_date)),
                structure_contract_version=structure_contract_version,
            )
            materialization_action = _upsert_structure_snapshot(connection, snapshot_row=snapshot_row)
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {STRUCTURE_RUN_SNAPSHOT_TABLE} (
                    run_id,
                    structure_snapshot_nk,
                    materialization_action,
                    structure_progress_state
                )
                VALUES (?, ?, ?, ?)
                """,
                [
                    run_id,
                    snapshot_row.structure_snapshot_nk,
                    materialization_action,
                    snapshot_row.structure_progress_state,
                ],
            )
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

            if snapshot_row.structure_progress_state == "advancing":
                advancing_count += 1
            elif snapshot_row.structure_progress_state == "stalled":
                stalled_count += 1
            elif snapshot_row.structure_progress_state == "failed":
                failed_count += 1
            else:
                unknown_count += 1

    materialized_snapshot_count = inserted_count + reused_count + rematerialized_count
    return StructureSnapshotBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        execution_mode="bounded",
        structure_contract_version=structure_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in input_rows}),
        claimed_scope_count=len({row.instrument for row in input_rows}),
        candidate_input_count=len(input_rows),
        materialized_snapshot_count=materialized_snapshot_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        missing_context_count=missing_context_count,
        advancing_count=advancing_count,
        stalled_count=stalled_count,
        failed_count=failed_count,
        unknown_count=unknown_count,
        queue_enqueued_count=0,
        queue_claimed_count=len({row.instrument for row in input_rows}),
        checkpoint_upserted_count=0,
        structure_ledger_path=str(structure_path),
        malf_ledger_path=str(malf_path),
        source_context_table=source_context_table,
        source_structure_input_table=source_structure_input_table,
        source_break_confirmation_table=source_break_confirmation_table,
        source_stats_table=source_stats_table,
        source_timeframe=source_timeframe,
    )


def _bounded_by_instrument_batches(
    input_rows: list[_StructureInputRow],
    *,
    batch_size: int,
) -> list[list[_StructureInputRow]]:
    if not input_rows:
        return []
    normalized_batch_size = max(int(batch_size), 1)
    batches: list[list[_StructureInputRow]] = []
    current_batch: list[_StructureInputRow] = []
    current_instruments: set[str] = set()
    for row in input_rows:
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
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    source_context_table: str,
    source_structure_input_table: str,
    structure_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {STRUCTURE_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_context_table,
            source_structure_input_table,
            structure_contract_version
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
            source_context_table,
            source_structure_input_table,
            structure_contract_version,
        ],
    )


def _build_structure_snapshot_row(
    *,
    run_id: str,
    input_row: _StructureInputRow,
    context_row: _MultiTimeframeContextRow,
    break_confirmation_row: _BreakConfirmationRow | None,
    stats_snapshot_row: _StatsSnapshotRow | None,
    structure_contract_version: str,
) -> _StructureSnapshotRow:
    structure_progress_state = _derive_structure_progress_state(input_row)
    structure_snapshot_nk = _build_structure_snapshot_nk(
        instrument=input_row.instrument,
        signal_date=input_row.signal_date,
        asof_date=input_row.asof_date,
        source_context_nk=context_row.daily.source_context_nk,
        structure_contract_version=structure_contract_version,
    )
    return _StructureSnapshotRow(
        structure_snapshot_nk=structure_snapshot_nk,
        instrument=input_row.instrument,
        signal_date=input_row.signal_date,
        asof_date=input_row.asof_date,
        major_state=context_row.daily.major_state,
        trend_direction=context_row.daily.trend_direction,
        reversal_stage=context_row.daily.reversal_stage,
        wave_id=context_row.daily.wave_id,
        current_hh_count=context_row.daily.current_hh_count,
        current_ll_count=context_row.daily.current_ll_count,
        daily_major_state=context_row.daily.major_state,
        daily_trend_direction=context_row.daily.trend_direction,
        daily_reversal_stage=context_row.daily.reversal_stage,
        daily_wave_id=context_row.daily.wave_id,
        daily_current_hh_count=context_row.daily.current_hh_count,
        daily_current_ll_count=context_row.daily.current_ll_count,
        daily_source_context_nk=context_row.daily.source_context_nk,
        weekly_major_state=None if context_row.weekly is None else context_row.weekly.major_state,
        weekly_trend_direction=None if context_row.weekly is None else context_row.weekly.trend_direction,
        weekly_reversal_stage=None if context_row.weekly is None else context_row.weekly.reversal_stage,
        weekly_wave_id=None if context_row.weekly is None else context_row.weekly.wave_id,
        weekly_current_hh_count=None if context_row.weekly is None else context_row.weekly.current_hh_count,
        weekly_current_ll_count=None if context_row.weekly is None else context_row.weekly.current_ll_count,
        weekly_source_context_nk=None if context_row.weekly is None else context_row.weekly.source_context_nk,
        monthly_major_state=None if context_row.monthly is None else context_row.monthly.major_state,
        monthly_trend_direction=None if context_row.monthly is None else context_row.monthly.trend_direction,
        monthly_reversal_stage=None if context_row.monthly is None else context_row.monthly.reversal_stage,
        monthly_wave_id=None if context_row.monthly is None else context_row.monthly.wave_id,
        monthly_current_hh_count=None if context_row.monthly is None else context_row.monthly.current_hh_count,
        monthly_current_ll_count=None if context_row.monthly is None else context_row.monthly.current_ll_count,
        monthly_source_context_nk=None if context_row.monthly is None else context_row.monthly.source_context_nk,
        structure_progress_state=structure_progress_state,
        break_confirmation_status=None if break_confirmation_row is None else break_confirmation_row.confirmation_status,
        break_confirmation_ref=None if break_confirmation_row is None else break_confirmation_row.break_event_nk,
        stats_snapshot_nk=None if stats_snapshot_row is None else stats_snapshot_row.stats_snapshot_nk,
        exhaustion_risk_bucket=None if stats_snapshot_row is None else stats_snapshot_row.exhaustion_risk_bucket,
        reversal_probability_bucket=None if stats_snapshot_row is None else stats_snapshot_row.reversal_probability_bucket,
        source_context_nk=context_row.daily.source_context_nk,
        structure_contract_version=structure_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _derive_structure_progress_state(input_row: _StructureInputRow) -> str:
    if input_row.is_failed_extreme or input_row.failure_type is not None:
        return "failed"
    if input_row.new_high_count > 0 or input_row.refresh_density > 0 or input_row.advancement_density > 0:
        return "advancing"
    if input_row.new_low_count > 0:
        return "stalled"
    return "unknown"


def _upsert_structure_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    snapshot_row: _StructureSnapshotRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            major_state,
            trend_direction,
            reversal_stage,
            wave_id,
            current_hh_count,
            current_ll_count,
            daily_major_state,
            daily_trend_direction,
            daily_reversal_stage,
            daily_wave_id,
            daily_current_hh_count,
            daily_current_ll_count,
            daily_source_context_nk,
            weekly_major_state,
            weekly_trend_direction,
            weekly_reversal_stage,
            weekly_wave_id,
            weekly_current_hh_count,
            weekly_current_ll_count,
            weekly_source_context_nk,
            monthly_major_state,
            monthly_trend_direction,
            monthly_reversal_stage,
            monthly_wave_id,
            monthly_current_hh_count,
            monthly_current_ll_count,
            monthly_source_context_nk,
            structure_progress_state,
            break_confirmation_status,
            break_confirmation_ref,
            stats_snapshot_nk,
            exhaustion_risk_bucket,
            reversal_probability_bucket,
            first_seen_run_id
        FROM {STRUCTURE_SNAPSHOT_TABLE}
        WHERE structure_snapshot_nk = ?
        """,
        [snapshot_row.structure_snapshot_nk],
    ).fetchone()
    new_fingerprint = (
        snapshot_row.major_state,
        snapshot_row.trend_direction,
        snapshot_row.reversal_stage,
        snapshot_row.wave_id,
        snapshot_row.current_hh_count,
        snapshot_row.current_ll_count,
        snapshot_row.daily_major_state,
        snapshot_row.daily_trend_direction,
        snapshot_row.daily_reversal_stage,
        snapshot_row.daily_wave_id,
        snapshot_row.daily_current_hh_count,
        snapshot_row.daily_current_ll_count,
        snapshot_row.daily_source_context_nk,
        snapshot_row.weekly_major_state,
        snapshot_row.weekly_trend_direction,
        snapshot_row.weekly_reversal_stage,
        snapshot_row.weekly_wave_id,
        snapshot_row.weekly_current_hh_count,
        snapshot_row.weekly_current_ll_count,
        snapshot_row.weekly_source_context_nk,
        snapshot_row.monthly_major_state,
        snapshot_row.monthly_trend_direction,
        snapshot_row.monthly_reversal_stage,
        snapshot_row.monthly_wave_id,
        snapshot_row.monthly_current_hh_count,
        snapshot_row.monthly_current_ll_count,
        snapshot_row.monthly_source_context_nk,
        snapshot_row.structure_progress_state,
        snapshot_row.break_confirmation_status,
        snapshot_row.break_confirmation_ref,
        snapshot_row.stats_snapshot_nk,
        snapshot_row.exhaustion_risk_bucket,
        snapshot_row.reversal_probability_bucket,
    )
    insert_values = [
        snapshot_row.structure_snapshot_nk,
        snapshot_row.instrument,
        snapshot_row.signal_date,
        snapshot_row.asof_date,
        snapshot_row.major_state,
        snapshot_row.trend_direction,
        snapshot_row.reversal_stage,
        snapshot_row.wave_id,
        snapshot_row.current_hh_count,
        snapshot_row.current_ll_count,
        snapshot_row.daily_major_state,
        snapshot_row.daily_trend_direction,
        snapshot_row.daily_reversal_stage,
        snapshot_row.daily_wave_id,
        snapshot_row.daily_current_hh_count,
        snapshot_row.daily_current_ll_count,
        snapshot_row.daily_source_context_nk,
        snapshot_row.weekly_major_state,
        snapshot_row.weekly_trend_direction,
        snapshot_row.weekly_reversal_stage,
        snapshot_row.weekly_wave_id,
        snapshot_row.weekly_current_hh_count,
        snapshot_row.weekly_current_ll_count,
        snapshot_row.weekly_source_context_nk,
        snapshot_row.monthly_major_state,
        snapshot_row.monthly_trend_direction,
        snapshot_row.monthly_reversal_stage,
        snapshot_row.monthly_wave_id,
        snapshot_row.monthly_current_hh_count,
        snapshot_row.monthly_current_ll_count,
        snapshot_row.monthly_source_context_nk,
        snapshot_row.structure_progress_state,
        snapshot_row.break_confirmation_status,
        snapshot_row.break_confirmation_ref,
        snapshot_row.stats_snapshot_nk,
        snapshot_row.exhaustion_risk_bucket,
        snapshot_row.reversal_probability_bucket,
        snapshot_row.source_context_nk,
        snapshot_row.structure_contract_version,
        snapshot_row.first_seen_run_id,
        snapshot_row.last_materialized_run_id,
    ]
    if existing_row is None:
        placeholders = ", ".join("?" for _ in insert_values)
        connection.execute(
            f"""
            INSERT INTO {STRUCTURE_SNAPSHOT_TABLE} (
                structure_snapshot_nk,
                instrument,
                signal_date,
                asof_date,
                major_state,
                trend_direction,
                reversal_stage,
                wave_id,
                current_hh_count,
                current_ll_count,
                daily_major_state,
                daily_trend_direction,
                daily_reversal_stage,
                daily_wave_id,
                daily_current_hh_count,
                daily_current_ll_count,
                daily_source_context_nk,
                weekly_major_state,
                weekly_trend_direction,
                weekly_reversal_stage,
                weekly_wave_id,
                weekly_current_hh_count,
                weekly_current_ll_count,
                weekly_source_context_nk,
                monthly_major_state,
                monthly_trend_direction,
                monthly_reversal_stage,
                monthly_wave_id,
                monthly_current_hh_count,
                monthly_current_ll_count,
                monthly_source_context_nk,
                structure_progress_state,
                break_confirmation_status,
                break_confirmation_ref,
                stats_snapshot_nk,
                exhaustion_risk_bucket,
                reversal_probability_bucket,
                source_context_nk,
                structure_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES ({placeholders})
            """,
            insert_values,
        )
        return "inserted"
    first_seen_run_id = str(existing_row[-1]) if existing_row[-1] is not None else snapshot_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {STRUCTURE_SNAPSHOT_TABLE}
        SET
            major_state = ?,
            trend_direction = ?,
            reversal_stage = ?,
            wave_id = ?,
            current_hh_count = ?,
            current_ll_count = ?,
            daily_major_state = ?,
            daily_trend_direction = ?,
            daily_reversal_stage = ?,
            daily_wave_id = ?,
            daily_current_hh_count = ?,
            daily_current_ll_count = ?,
            daily_source_context_nk = ?,
            weekly_major_state = ?,
            weekly_trend_direction = ?,
            weekly_reversal_stage = ?,
            weekly_wave_id = ?,
            weekly_current_hh_count = ?,
            weekly_current_ll_count = ?,
            weekly_source_context_nk = ?,
            monthly_major_state = ?,
            monthly_trend_direction = ?,
            monthly_reversal_stage = ?,
            monthly_wave_id = ?,
            monthly_current_hh_count = ?,
            monthly_current_ll_count = ?,
            monthly_source_context_nk = ?,
            structure_progress_state = ?,
            break_confirmation_status = ?,
            break_confirmation_ref = ?,
            stats_snapshot_nk = ?,
            exhaustion_risk_bucket = ?,
            reversal_probability_bucket = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE structure_snapshot_nk = ?
        """,
        [*new_fingerprint, first_seen_run_id, snapshot_row.last_materialized_run_id, snapshot_row.structure_snapshot_nk],
    )
    return "reused" if tuple(existing_row[:-1]) == new_fingerprint else "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: StructureSnapshotBuildSummary,
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
        UPDATE {STRUCTURE_RUN_TABLE}
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
