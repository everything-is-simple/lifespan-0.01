"""alpha PAS detector 的物化逻辑。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from mlq.alpha.bootstrap import (
    ALPHA_PAS_TRIGGER_RUN_CANDIDATE_TABLE,
    ALPHA_PAS_TRIGGER_RUN_TABLE,
    ALPHA_TRIGGER_CANDIDATE_TABLE,
)
from mlq.alpha.pas_detectors import evaluate_pas_triggers
from mlq.alpha.pas_shared import (
    DEFAULT_ALPHA_PAS_FAMILY_CODE_BY_TRIGGER,
    AlphaPasTriggerBuildSummary,
    _CandidateRow,
    _DetectorScopeRow,
    _build_candidate_nk,
)


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    candidate_scope_count: int,
    source_filter_table: str,
    source_structure_table: str,
    source_price_table: str,
    source_adjust_method: str,
    detector_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {ALPHA_PAS_TRIGGER_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            candidate_scope_count,
            materialized_candidate_count,
            source_filter_table,
            source_structure_table,
            source_price_table,
            source_adjust_method,
            detector_contract_version,
            notes
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            candidate_scope_count,
            source_filter_table,
            source_structure_table,
            source_price_table,
            source_adjust_method,
            detector_contract_version,
            "official alpha PAS five-trigger detector materialization",
        ],
    )


def _materialize_pas_detector_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    scope_rows: list[_DetectorScopeRow],
    price_history: pd.DataFrame,
    detector_contract_version: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    alpha_path: Path,
    filter_path: Path,
    structure_path: Path,
    market_base_path: Path,
    source_filter_table: str,
    source_structure_table: str,
    source_price_table: str,
    source_adjust_method: str,
) -> AlphaPasTriggerBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    materialized_candidate_count = 0
    skipped_pattern_count = 0
    family_counts = {name: 0 for name in DEFAULT_ALPHA_PAS_FAMILY_CODE_BY_TRIGGER}
    price_history_by_instrument = {
        instrument: frame.sort_values("date").reset_index(drop=True)
        for instrument, frame in price_history.groupby("instrument")
    }

    for scope_row in scope_rows:
        history = _build_history_for_scope(
            price_history_by_instrument=price_history_by_instrument,
            instrument=scope_row.instrument,
            signal_date=scope_row.signal_date,
        )
        evaluations = evaluate_pas_triggers(scope_row=scope_row, history=history)
        for evaluation in evaluations:
            if not bool(evaluation.get("triggered")):
                skipped_pattern_count += 1
                continue
            trigger_type = str(evaluation["trigger_type"])
            candidate_row = _build_candidate_row(
                run_id=run_id,
                scope_row=scope_row,
                evaluation=evaluation,
                history=history,
                detector_contract_version=detector_contract_version,
            )
            materialization_action = _upsert_candidate_row(connection, candidate_row=candidate_row)
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {ALPHA_PAS_TRIGGER_RUN_CANDIDATE_TABLE} (
                    run_id,
                    candidate_nk,
                    materialization_action,
                    trigger_type,
                    family_code
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    candidate_row.candidate_nk,
                    materialization_action,
                    candidate_row.trigger_type,
                    candidate_row.family_code,
                ],
            )
            materialized_candidate_count += 1
            family_counts[trigger_type] = family_counts.get(trigger_type, 0) + 1
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

    return AlphaPasTriggerBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        execution_mode="bounded",
        detector_contract_version=detector_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in scope_rows}),
        claimed_scope_count=0,
        evaluated_snapshot_count=len(scope_rows),
        materialized_candidate_count=materialized_candidate_count,
        skipped_pattern_count=skipped_pattern_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        queue_enqueued_count=0,
        queue_claimed_count=0,
        checkpoint_upserted_count=0,
        family_counts=family_counts,
        alpha_ledger_path=str(alpha_path),
        filter_ledger_path=str(filter_path),
        structure_ledger_path=str(structure_path),
        market_base_ledger_path=str(market_base_path),
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        source_price_table=source_price_table,
        source_adjust_method=source_adjust_method,
    )


def _build_history_for_scope(
    *,
    price_history_by_instrument: dict[str, pd.DataFrame],
    instrument: str,
    signal_date: date,
) -> pd.DataFrame:
    history = price_history_by_instrument.get(instrument)
    if history is None or history.empty:
        return pd.DataFrame(
            columns=["date", "adj_open", "adj_high", "adj_low", "adj_close", "volume", "volume_ma20"]
        )
    return history.loc[history["date"] <= pd.Timestamp(signal_date)].tail(61).reset_index(drop=True)


def _build_candidate_row(
    *,
    run_id: str,
    scope_row: _DetectorScopeRow,
    evaluation: dict[str, object],
    history: pd.DataFrame,
    detector_contract_version: str,
) -> _CandidateRow:
    trigger_type = str(evaluation["trigger_type"])
    pattern_code = str(evaluation["pattern_code"])
    candidate_nk = _build_candidate_nk(
        instrument=scope_row.instrument,
        signal_date=scope_row.signal_date,
        asof_date=scope_row.asof_date,
        trigger_family="PAS",
        trigger_type=trigger_type,
        pattern_code=pattern_code,
        detector_contract_version=detector_contract_version,
    )
    trace_payload = {
        key: _normalize_json_value(value)
        for key, value in evaluation.items()
        if key != "family_code"
    }
    return _CandidateRow(
        candidate_nk=candidate_nk,
        instrument=scope_row.instrument,
        signal_date=scope_row.signal_date,
        asof_date=scope_row.asof_date,
        trigger_family="PAS",
        trigger_type=trigger_type,
        pattern_code=pattern_code,
        family_code=str(evaluation.get("family_code") or DEFAULT_ALPHA_PAS_FAMILY_CODE_BY_TRIGGER[trigger_type]),
        trigger_strength=float(evaluation.get("strength") or 0.0),
        detect_reason=str(evaluation.get("detect_reason") or "TRIGGERED"),
        skip_reason=None if evaluation.get("skip_reason") is None else str(evaluation["skip_reason"]),
        price_context_json=json.dumps(_build_price_context_payload(history=history), ensure_ascii=False, sort_keys=True),
        structure_context_json=json.dumps(_build_structure_context_payload(scope_row=scope_row), ensure_ascii=False, sort_keys=True),
        detector_trace_json=json.dumps(trace_payload, ensure_ascii=False, sort_keys=True),
        source_filter_snapshot_nk=scope_row.filter_snapshot_nk,
        source_structure_snapshot_nk=scope_row.structure_snapshot_nk,
        source_price_fingerprint=json.dumps(
            {
                "instrument": scope_row.instrument,
                "history_days": int(len(history)),
                "history_start_date": None if history.empty else history["date"].iloc[0].date().isoformat(),
                "history_end_date": None if history.empty else history["date"].iloc[-1].date().isoformat(),
                "last_close": None if history.empty else float(history["adj_close"].iloc[-1]),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        detector_contract_version=detector_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _build_price_context_payload(*, history: pd.DataFrame) -> dict[str, object]:
    if history.empty:
        return {"history_days": 0}
    latest = history.iloc[-1]
    return {
        "history_days": int(len(history)),
        "history_start_date": history["date"].iloc[0].date().isoformat(),
        "history_end_date": history["date"].iloc[-1].date().isoformat(),
        "signal_open": float(latest["adj_open"]),
        "signal_high": float(latest["adj_high"]),
        "signal_low": float(latest["adj_low"]),
        "signal_close": float(latest["adj_close"]),
        "signal_volume": float(latest["volume"]),
        "signal_volume_ma20": float(latest["volume_ma20"]),
    }


def _build_structure_context_payload(*, scope_row: _DetectorScopeRow) -> dict[str, object]:
    return {
        "filter_snapshot_nk": scope_row.filter_snapshot_nk,
        "structure_snapshot_nk": scope_row.structure_snapshot_nk,
        "trigger_admissible": scope_row.trigger_admissible,
        "primary_blocking_condition": scope_row.primary_blocking_condition,
        "major_state": scope_row.major_state,
        "trend_direction": scope_row.trend_direction,
        "reversal_stage": scope_row.reversal_stage,
        "wave_id": scope_row.wave_id,
        "current_hh_count": scope_row.current_hh_count,
        "current_ll_count": scope_row.current_ll_count,
        "structure_progress_state": scope_row.structure_progress_state,
        "break_confirmation_status": scope_row.break_confirmation_status,
        "break_confirmation_ref": scope_row.break_confirmation_ref,
        "exhaustion_risk_bucket": scope_row.exhaustion_risk_bucket,
        "reversal_probability_bucket": scope_row.reversal_probability_bucket,
        "daily_source_context_nk": scope_row.daily_source_context_nk,
        "weekly_major_state": scope_row.weekly_major_state,
        "weekly_trend_direction": scope_row.weekly_trend_direction,
        "weekly_reversal_stage": scope_row.weekly_reversal_stage,
        "weekly_source_context_nk": scope_row.weekly_source_context_nk,
        "monthly_major_state": scope_row.monthly_major_state,
        "monthly_trend_direction": scope_row.monthly_trend_direction,
        "monthly_reversal_stage": scope_row.monthly_reversal_stage,
        "monthly_source_context_nk": scope_row.monthly_source_context_nk,
    }


def _normalize_json_value(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _upsert_candidate_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_row: _CandidateRow,
) -> str:
    existing = connection.execute(
        f"""
        SELECT
            family_code,
            trigger_strength,
            detect_reason,
            skip_reason,
            price_context_json,
            structure_context_json,
            detector_trace_json,
            source_filter_snapshot_nk,
            source_structure_snapshot_nk,
            source_price_fingerprint,
            first_seen_run_id
        FROM {ALPHA_TRIGGER_CANDIDATE_TABLE}
        WHERE candidate_nk = ?
        """,
        [candidate_row.candidate_nk],
    ).fetchone()
    payload_tuple = (
        candidate_row.family_code,
        float(candidate_row.trigger_strength),
        candidate_row.detect_reason,
        candidate_row.skip_reason,
        candidate_row.price_context_json,
        candidate_row.structure_context_json,
        candidate_row.detector_trace_json,
        candidate_row.source_filter_snapshot_nk,
        candidate_row.source_structure_snapshot_nk,
        candidate_row.source_price_fingerprint,
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {ALPHA_TRIGGER_CANDIDATE_TABLE} (
                candidate_nk, instrument, signal_date, asof_date, trigger_family, trigger_type, pattern_code,
                family_code, trigger_strength, detect_reason, skip_reason,
                price_context_json, structure_context_json, detector_trace_json,
                source_filter_snapshot_nk, source_structure_snapshot_nk, source_price_fingerprint,
                detector_contract_version, first_seen_run_id, last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                candidate_row.candidate_nk,
                candidate_row.instrument,
                candidate_row.signal_date,
                candidate_row.asof_date,
                candidate_row.trigger_family,
                candidate_row.trigger_type,
                candidate_row.pattern_code,
                candidate_row.family_code,
                candidate_row.trigger_strength,
                candidate_row.detect_reason,
                candidate_row.skip_reason,
                candidate_row.price_context_json,
                candidate_row.structure_context_json,
                candidate_row.detector_trace_json,
                candidate_row.source_filter_snapshot_nk,
                candidate_row.source_structure_snapshot_nk,
                candidate_row.source_price_fingerprint,
                candidate_row.detector_contract_version,
                candidate_row.first_seen_run_id,
                candidate_row.last_materialized_run_id,
            ],
        )
        return "inserted"
    first_seen_run_id = str(existing[10]) if existing[10] is not None else candidate_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {ALPHA_TRIGGER_CANDIDATE_TABLE}
        SET
            family_code = ?,
            trigger_strength = ?,
            detect_reason = ?,
            skip_reason = ?,
            price_context_json = ?,
            structure_context_json = ?,
            detector_trace_json = ?,
            source_filter_snapshot_nk = ?,
            source_structure_snapshot_nk = ?,
            source_price_fingerprint = ?,
            detector_contract_version = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE candidate_nk = ?
        """,
        [
            candidate_row.family_code,
            candidate_row.trigger_strength,
            candidate_row.detect_reason,
            candidate_row.skip_reason,
            candidate_row.price_context_json,
            candidate_row.structure_context_json,
            candidate_row.detector_trace_json,
            candidate_row.source_filter_snapshot_nk,
            candidate_row.source_structure_snapshot_nk,
            candidate_row.source_price_fingerprint,
            candidate_row.detector_contract_version,
            first_seen_run_id,
            candidate_row.last_materialized_run_id,
            candidate_row.candidate_nk,
        ],
    )
    existing_tuple = (
        str(existing[0]),
        float(existing[1]),
        str(existing[2]),
        None if existing[3] is None else str(existing[3]),
        str(existing[4]),
        str(existing[5]),
        str(existing[6]),
        str(existing[7]),
        str(existing[8]),
        str(existing[9]),
    )
    return "reused" if existing_tuple == payload_tuple else "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: AlphaPasTriggerBuildSummary,
) -> None:
    _update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        materialized_candidate_count=summary.materialized_candidate_count,
        summary_payload=summary.as_dict(),
    )


def _update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    materialized_candidate_count: int,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_PAS_TRIGGER_RUN_TABLE}
        SET
            run_status = ?,
            materialized_candidate_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            materialized_candidate_count,
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )
