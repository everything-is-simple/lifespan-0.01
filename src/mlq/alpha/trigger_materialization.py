"""封装 `alpha trigger` runner 的事件物化与 run 审计写回。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from mlq.alpha.bootstrap import (
    ALPHA_TRIGGER_EVENT_TABLE,
    ALPHA_TRIGGER_RUN_EVENT_TABLE,
    ALPHA_TRIGGER_RUN_TABLE,
)
from mlq.alpha.trigger_shared import (
    AlphaTriggerBuildSummary,
    _OfficialContextRow,
    _TriggerEventRow,
    _TriggerInputRow,
    _normalize_optional_nullable_str,
    _normalize_optional_str,
)


def _materialize_trigger_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    input_rows: list[_TriggerInputRow],
    context_map: dict[tuple[str, date, date], _OfficialContextRow],
    trigger_contract_version: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    alpha_path: Path,
    filter_path: Path,
    structure_path: Path,
    source_trigger_input_table: str,
    source_filter_table: str,
    source_structure_table: str,
    batch_size: int,
) -> AlphaTriggerBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    missing_context_count = 0

    for input_batch in _bounded_by_instrument_batches(input_rows, batch_size=batch_size):
        for input_row in input_batch:
            context_row = context_map.get((input_row.instrument, input_row.signal_date, input_row.asof_date))
            if context_row is None:
                missing_context_count += 1
                continue
            event_row = _build_trigger_event_row(
                run_id=run_id,
                input_row=input_row,
                context_row=context_row,
                trigger_contract_version=trigger_contract_version,
            )
            materialization_action = _upsert_trigger_event(connection, event_row=event_row)
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {ALPHA_TRIGGER_RUN_EVENT_TABLE} (
                    run_id,
                    trigger_event_nk,
                    materialization_action,
                    trigger_type
                )
                VALUES (?, ?, ?, ?)
                """,
                [
                    run_id,
                    event_row.trigger_event_nk,
                    materialization_action,
                    event_row.trigger_type,
                ],
            )
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

    materialized_trigger_count = inserted_count + reused_count + rematerialized_count
    return AlphaTriggerBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        execution_mode="bounded",
        trigger_contract_version=trigger_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in input_rows}),
        claimed_scope_count=len({row.instrument for row in input_rows}),
        candidate_trigger_count=len(input_rows),
        materialized_trigger_count=materialized_trigger_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        missing_context_count=missing_context_count,
        queue_enqueued_count=0,
        queue_claimed_count=len({row.instrument for row in input_rows}),
        checkpoint_upserted_count=0,
        alpha_ledger_path=str(alpha_path),
        filter_ledger_path=str(filter_path),
        structure_ledger_path=str(structure_path),
        source_trigger_input_table=source_trigger_input_table,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
    )


def _bounded_by_instrument_batches(
    input_rows: list[_TriggerInputRow],
    *,
    batch_size: int,
) -> list[list[_TriggerInputRow]]:
    if not input_rows:
        return []
    normalized_batch_size = max(int(batch_size), 1)
    batches: list[list[_TriggerInputRow]] = []
    current_batch: list[_TriggerInputRow] = []
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
    candidate_trigger_count: int,
    source_trigger_input_table: str,
    source_filter_table: str,
    source_structure_table: str,
    trigger_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {ALPHA_TRIGGER_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            candidate_trigger_count,
            source_trigger_input_table,
            source_filter_table,
            source_structure_table,
            trigger_contract_version,
            notes
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            candidate_trigger_count,
            source_trigger_input_table,
            source_filter_table,
            source_structure_table,
            trigger_contract_version,
            "bounded alpha trigger ledger materialization",
        ],
    )


def _build_trigger_event_row(
    *,
    run_id: str,
    input_row: _TriggerInputRow,
    context_row: _OfficialContextRow,
    trigger_contract_version: str,
) -> _TriggerEventRow:
    trigger_event_nk = _build_trigger_event_nk(
        instrument=input_row.instrument,
        signal_date=input_row.signal_date,
        asof_date=input_row.asof_date,
        trigger_family=input_row.trigger_family,
        trigger_type=input_row.trigger_type,
        pattern_code=input_row.pattern_code,
        trigger_contract_version=trigger_contract_version,
    )
    return _TriggerEventRow(
        trigger_event_nk=trigger_event_nk,
        instrument=input_row.instrument,
        signal_date=input_row.signal_date,
        asof_date=input_row.asof_date,
        trigger_family=input_row.trigger_family,
        trigger_type=input_row.trigger_type,
        pattern_code=input_row.pattern_code,
        source_filter_snapshot_nk=context_row.filter_snapshot_nk,
        source_structure_snapshot_nk=context_row.structure_snapshot_nk,
        daily_source_context_nk=context_row.daily_source_context_nk,
        weekly_major_state=context_row.weekly_major_state,
        weekly_trend_direction=context_row.weekly_trend_direction,
        weekly_reversal_stage=context_row.weekly_reversal_stage,
        weekly_source_context_nk=context_row.weekly_source_context_nk,
        monthly_major_state=context_row.monthly_major_state,
        monthly_trend_direction=context_row.monthly_trend_direction,
        monthly_reversal_stage=context_row.monthly_reversal_stage,
        monthly_source_context_nk=context_row.monthly_source_context_nk,
        upstream_context_fingerprint=context_row.upstream_context_fingerprint,
        trigger_contract_version=trigger_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _build_trigger_event_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    trigger_family: str,
    trigger_type: str,
    pattern_code: str,
    trigger_contract_version: str,
) -> str:
    return "|".join(
        [
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            trigger_family,
            trigger_type,
            pattern_code,
            trigger_contract_version,
        ]
    )


def _upsert_trigger_event(
    connection: duckdb.DuckDBPyConnection,
    *,
    event_row: _TriggerEventRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            source_filter_snapshot_nk,
            source_structure_snapshot_nk,
            daily_source_context_nk,
            weekly_major_state,
            weekly_trend_direction,
            weekly_reversal_stage,
            weekly_source_context_nk,
            monthly_major_state,
            monthly_trend_direction,
            monthly_reversal_stage,
            monthly_source_context_nk,
            upstream_context_fingerprint,
            first_seen_run_id
        FROM {ALPHA_TRIGGER_EVENT_TABLE}
        WHERE trigger_event_nk = ?
        """,
        [event_row.trigger_event_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {ALPHA_TRIGGER_EVENT_TABLE} (
                trigger_event_nk,
                instrument,
                signal_date,
                asof_date,
                trigger_family,
                trigger_type,
                pattern_code,
                source_filter_snapshot_nk,
                source_structure_snapshot_nk,
                daily_source_context_nk,
                weekly_major_state,
                weekly_trend_direction,
                weekly_reversal_stage,
                weekly_source_context_nk,
                monthly_major_state,
                monthly_trend_direction,
                monthly_reversal_stage,
                monthly_source_context_nk,
                upstream_context_fingerprint,
                trigger_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event_row.trigger_event_nk,
                event_row.instrument,
                event_row.signal_date,
                event_row.asof_date,
                event_row.trigger_family,
                event_row.trigger_type,
                event_row.pattern_code,
                event_row.source_filter_snapshot_nk,
                event_row.source_structure_snapshot_nk,
                event_row.daily_source_context_nk,
                event_row.weekly_major_state,
                event_row.weekly_trend_direction,
                event_row.weekly_reversal_stage,
                event_row.weekly_source_context_nk,
                event_row.monthly_major_state,
                event_row.monthly_trend_direction,
                event_row.monthly_reversal_stage,
                event_row.monthly_source_context_nk,
                event_row.upstream_context_fingerprint,
                event_row.trigger_contract_version,
                event_row.first_seen_run_id,
                event_row.last_materialized_run_id,
            ],
        )
        return "inserted"

    existing_fingerprint = (
        _normalize_optional_str(existing_row[0]),
        _normalize_optional_str(existing_row[1]),
        _normalize_optional_nullable_str(existing_row[2]),
        _normalize_optional_nullable_str(existing_row[3]),
        _normalize_optional_nullable_str(existing_row[4]),
        _normalize_optional_nullable_str(existing_row[5]),
        _normalize_optional_nullable_str(existing_row[6]),
        _normalize_optional_nullable_str(existing_row[7]),
        _normalize_optional_nullable_str(existing_row[8]),
        _normalize_optional_nullable_str(existing_row[9]),
        _normalize_optional_nullable_str(existing_row[10]),
        _normalize_optional_str(existing_row[11]),
    )
    new_fingerprint = (
        event_row.source_filter_snapshot_nk,
        event_row.source_structure_snapshot_nk,
        event_row.daily_source_context_nk,
        event_row.weekly_major_state,
        event_row.weekly_trend_direction,
        event_row.weekly_reversal_stage,
        event_row.weekly_source_context_nk,
        event_row.monthly_major_state,
        event_row.monthly_trend_direction,
        event_row.monthly_reversal_stage,
        event_row.monthly_source_context_nk,
        event_row.upstream_context_fingerprint,
    )
    first_seen_run_id = str(existing_row[12]) if existing_row[12] is not None else event_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {ALPHA_TRIGGER_EVENT_TABLE}
        SET
            source_filter_snapshot_nk = ?,
            source_structure_snapshot_nk = ?,
            daily_source_context_nk = ?,
            weekly_major_state = ?,
            weekly_trend_direction = ?,
            weekly_reversal_stage = ?,
            weekly_source_context_nk = ?,
            monthly_major_state = ?,
            monthly_trend_direction = ?,
            monthly_reversal_stage = ?,
            monthly_source_context_nk = ?,
            upstream_context_fingerprint = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE trigger_event_nk = ?
        """,
        [
            event_row.source_filter_snapshot_nk,
            event_row.source_structure_snapshot_nk,
            event_row.daily_source_context_nk,
            event_row.weekly_major_state,
            event_row.weekly_trend_direction,
            event_row.weekly_reversal_stage,
            event_row.weekly_source_context_nk,
            event_row.monthly_major_state,
            event_row.monthly_trend_direction,
            event_row.monthly_reversal_stage,
            event_row.monthly_source_context_nk,
            event_row.upstream_context_fingerprint,
            first_seen_run_id,
            event_row.last_materialized_run_id,
            event_row.trigger_event_nk,
        ],
    )
    if existing_fingerprint == new_fingerprint:
        return "reused"
    return "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: AlphaTriggerBuildSummary,
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
        UPDATE {ALPHA_TRIGGER_RUN_TABLE}
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


def _write_summary(summary: AlphaTriggerBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
