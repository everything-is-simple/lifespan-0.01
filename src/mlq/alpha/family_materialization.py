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
    _DEFAULT_FAMILY_CODE_BY_TYPE,
    _FamilyEventRow,
    _TriggerRow,
    _normalize_optional_str,
)


def _materialize_family_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    trigger_rows: list[_TriggerRow],
    candidate_map: dict[tuple[str, date, date, str, str], dict[str, object]],
    family_contract_version: str,
    family_scope: tuple[str, ...],
    producer_name: str,
    producer_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    alpha_path: Path,
    source_trigger_table: str,
    source_candidate_table: str,
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
        source_trigger_table=source_trigger_table,
        source_candidate_table=source_candidate_table,
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
    family_contract_version: str,
) -> _FamilyEventRow:
    family_code = _resolve_family_code(trigger_row.trigger_type, candidate_payload)
    payload_json = json.dumps(
        _build_payload(
            trigger_row=trigger_row,
            family_code=family_code,
            candidate_payload=candidate_payload,
        ),
        ensure_ascii=False,
        sort_keys=True,
    )
    return _FamilyEventRow(
        family_event_nk=_build_family_event_nk(
            trigger_event_nk=trigger_row.trigger_event_nk,
            trigger_family=trigger_row.trigger_family,
            trigger_type=trigger_row.trigger_type,
            pattern_code=trigger_row.pattern_code,
            family_code=family_code,
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
    trigger_family: str,
    trigger_type: str,
    pattern_code: str,
    family_code: str,
    family_contract_version: str,
) -> str:
    return "|".join(
        [
            trigger_event_nk,
            trigger_family,
            trigger_type,
            pattern_code,
            family_code,
            family_contract_version,
        ]
    )


def _build_payload(
    *,
    trigger_row: _TriggerRow,
    family_code: str,
    candidate_payload: dict[str, object] | None,
) -> dict[str, object]:
    # payload_json 同时承载 family 最小解释层和 trigger 上游指纹，
    # 这样官方 trigger 语义变化时，family ledger 才能稳定记账 rematerialized。
    return {
        "family_code": family_code,
        "pattern_code": trigger_row.pattern_code,
        "trigger_type": trigger_row.trigger_type,
        "source_trigger": {
            "trigger_event_nk": trigger_row.trigger_event_nk,
            "source_filter_snapshot_nk": trigger_row.source_filter_snapshot_nk,
            "source_structure_snapshot_nk": trigger_row.source_structure_snapshot_nk,
            "upstream_context_fingerprint": _normalize_upstream_context_fingerprint(
                trigger_row.upstream_context_fingerprint
            ),
        },
        "candidate_payload": _normalize_candidate_payload(candidate_payload),
    }


def _normalize_candidate_payload(candidate_payload: dict[str, object] | None) -> dict[str, object]:
    if candidate_payload is None:
        return {}
    normalized_payload: dict[str, object] = {}
    for key, value in candidate_payload.items():
        if key in {"instrument", "signal_date", "asof_date", "trigger_family", "trigger_type", "pattern_code"}:
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
