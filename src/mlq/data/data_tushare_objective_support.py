"""Tushare objective source / materialization 的 checkpoint 与 audit 辅助。"""

from __future__ import annotations

from mlq.data.data_common import *
from mlq.data.data_shared import *


def build_tushare_request_nk(*, run_id: str, source_api: str, cursor_type: str, cursor_value: str) -> str:
    return "|".join([run_id, source_api, cursor_type, cursor_value])


def build_tushare_checkpoint_nk(*, source_api: str, cursor_type: str, cursor_value: str) -> str:
    return "|".join([source_api, cursor_type, cursor_value])


def build_tushare_event_nk(
    *,
    asset_type: str,
    code: str,
    source_api: str,
    objective_dimension: str,
    effective_start_date: date,
    source_record_hash: str,
) -> str:
    return "|".join(
        [asset_type, code, source_api, objective_dimension, effective_start_date.isoformat(), source_record_hash]
    )


def build_objective_profile_checkpoint_nk(*, asset_type: str, code: str, observed_trade_date: date) -> str:
    return "|".join([asset_type, code, observed_trade_date.isoformat()])


def build_objective_profile_run_profile_nk(*, run_id: str, asset_type: str, code: str, observed_trade_date: date) -> str:
    return "|".join([run_id, asset_type, code, observed_trade_date.isoformat()])


def build_records_digest(*, payload: list[dict[str, object]]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def fetch_tushare_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_api: str,
    cursor_type: str,
    cursor_value: str,
) -> tuple[object, ...] | None:
    return connection.execute(
        f"""
        SELECT checkpoint_nk, last_response_digest, last_observed_max_date, last_status
        FROM {TUSHARE_OBJECTIVE_CHECKPOINT_TABLE}
        WHERE checkpoint_nk = ?
        """,
        [build_tushare_checkpoint_nk(source_api=source_api, cursor_type=cursor_type, cursor_value=cursor_value)],
    ).fetchone()


def should_skip_tushare_request(
    *,
    checkpoint_row: tuple[object, ...] | None,
    response_digest: str,
    last_observed_max_date: date | None,
) -> bool:
    if checkpoint_row is None:
        return False
    recorded_digest = checkpoint_row[1]
    recorded_max_date = _coerce_date(checkpoint_row[2])
    if recorded_digest is None:
        return False
    return str(recorded_digest) == response_digest and recorded_max_date == last_observed_max_date


def insert_tushare_objective_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    source_api_scope: tuple[str, ...],
    candidate_cursor_count: int,
) -> None:
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {TUSHARE_OBJECTIVE_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            source_api_scope,
            signal_start_date,
            signal_end_date,
            candidate_cursor_count,
            processed_request_count,
            successful_request_count,
            failed_request_count,
            inserted_event_count,
            reused_event_count,
            rematerialized_event_count,
            run_status,
            started_at_utc,
            finished_at_utc,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 'running', CURRENT_TIMESTAMP, NULL, NULL)
        """,
        [
            run_id,
            TUSHARE_OBJECTIVE_SOURCE_SYNC_RUNNER_NAME,
            TUSHARE_OBJECTIVE_SOURCE_SYNC_RUNNER_VERSION,
            json.dumps(list(source_api_scope), ensure_ascii=False),
            signal_start_date,
            signal_end_date,
            candidate_cursor_count,
        ],
    )


def update_tushare_objective_run_success(
    connection: duckdb.DuckDBPyConnection,
    *,
    summary: TushareObjectiveSourceSyncSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {TUSHARE_OBJECTIVE_RUN_TABLE}
        SET
            processed_request_count = ?,
            successful_request_count = ?,
            failed_request_count = ?,
            inserted_event_count = ?,
            reused_event_count = ?,
            rematerialized_event_count = ?,
            run_status = 'completed',
            finished_at_utc = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            summary.processed_request_count,
            summary.successful_request_count,
            summary.failed_request_count,
            summary.inserted_event_count,
            summary.reused_event_count,
            summary.rematerialized_event_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            summary.run_id,
        ],
    )


def update_tushare_objective_run_failure(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {TUSHARE_OBJECTIVE_RUN_TABLE}
        SET
            run_status = 'failed',
            finished_at_utc = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [json.dumps(summary_payload, ensure_ascii=False, sort_keys=True), run_id],
    )


def record_tushare_objective_request(
    connection: duckdb.DuckDBPyConnection,
    *,
    request_nk: str,
    run_id: str,
    source_api: str,
    cursor_type: str,
    cursor_value: str,
    response_row_count: int,
    inserted_event_count: int,
    reused_event_count: int,
    rematerialized_event_count: int,
    response_digest: str | None,
    request_status: str,
    error_message: str | None,
) -> None:
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {TUSHARE_OBJECTIVE_REQUEST_TABLE} (
            request_nk,
            run_id,
            source_api,
            cursor_type,
            cursor_value,
            response_row_count,
            inserted_event_count,
            reused_event_count,
            rematerialized_event_count,
            response_digest,
            request_status,
            error_message,
            recorded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            request_nk,
            run_id,
            source_api,
            cursor_type,
            cursor_value,
            response_row_count,
            inserted_event_count,
            reused_event_count,
            rematerialized_event_count,
            response_digest,
            request_status,
            error_message,
        ],
    )


def upsert_tushare_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_api: str,
    cursor_type: str,
    cursor_value: str,
    last_success_run_id: str,
    last_response_digest: str | None,
    last_observed_max_date: date | None,
    last_status: str,
) -> None:
    checkpoint_nk = build_tushare_checkpoint_nk(
        source_api=source_api,
        cursor_type=cursor_type,
        cursor_value=cursor_value,
    )
    existing = connection.execute(
        f"SELECT checkpoint_nk FROM {TUSHARE_OBJECTIVE_CHECKPOINT_TABLE} WHERE checkpoint_nk = ?",
        [checkpoint_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {TUSHARE_OBJECTIVE_CHECKPOINT_TABLE} (
                checkpoint_nk,
                source_api,
                cursor_type,
                cursor_value,
                last_success_run_id,
                last_response_digest,
                last_observed_max_date,
                last_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                checkpoint_nk,
                source_api,
                cursor_type,
                cursor_value,
                last_success_run_id,
                last_response_digest,
                last_observed_max_date,
                last_status,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {TUSHARE_OBJECTIVE_CHECKPOINT_TABLE}
        SET
            last_success_run_id = ?,
            last_response_digest = ?,
            last_observed_max_date = ?,
            last_status = ?,
            updated_at_utc = CURRENT_TIMESTAMP
        WHERE checkpoint_nk = ?
        """,
        [last_success_run_id, last_response_digest, last_observed_max_date, last_status, checkpoint_nk],
    )


def upsert_tushare_events(
    connection: duckdb.DuckDBPyConnection,
    *,
    records: list[dict[str, object]],
) -> tuple[int, int, int]:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    for record in records:
        event_nk = str(record["event_nk"])
        existing = connection.execute(
            f"""
            SELECT payload_json, status_value_code, status_value_text, effective_end_date
            FROM {TUSHARE_OBJECTIVE_EVENT_TABLE}
            WHERE event_nk = ?
            """,
            [event_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {TUSHARE_OBJECTIVE_EVENT_TABLE} (
                    event_nk,
                    asset_type,
                    code,
                    source_api,
                    objective_dimension,
                    effective_start_date,
                    effective_end_date,
                    status_value_code,
                    status_value_text,
                    source_record_hash,
                    source_trade_date,
                    source_ann_date,
                    payload_json,
                    first_seen_run_id,
                    last_seen_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    record["event_nk"],
                    record["asset_type"],
                    record["code"],
                    record["source_api"],
                    record["objective_dimension"],
                    record["effective_start_date"],
                    record["effective_end_date"],
                    record["status_value_code"],
                    record["status_value_text"],
                    record["source_record_hash"],
                    record["source_trade_date"],
                    record["source_ann_date"],
                    record["payload_json"],
                    record["first_seen_run_id"],
                    record["last_seen_run_id"],
                ],
            )
            inserted_count += 1
            continue
        new_fingerprint = (
            record["payload_json"],
            record["status_value_code"],
            record["status_value_text"],
            record["effective_end_date"],
        )
        old_fingerprint = (existing[0], existing[1], existing[2], existing[3])
        connection.execute(
            f"""
            UPDATE {TUSHARE_OBJECTIVE_EVENT_TABLE}
            SET
                effective_end_date = ?,
                status_value_code = ?,
                status_value_text = ?,
                source_trade_date = ?,
                source_ann_date = ?,
                payload_json = ?,
                last_seen_run_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_nk = ?
            """,
            [
                record["effective_end_date"],
                record["status_value_code"],
                record["status_value_text"],
                record["source_trade_date"],
                record["source_ann_date"],
                record["payload_json"],
                record["last_seen_run_id"],
                event_nk,
            ],
        )
        if new_fingerprint == old_fingerprint:
            reused_count += 1
        else:
            rematerialized_count += 1
    return inserted_count, reused_count, rematerialized_count


def insert_objective_profile_materialization_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    candidate_profile_count: int,
) -> None:
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {OBJECTIVE_PROFILE_MATERIALIZATION_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            signal_start_date,
            signal_end_date,
            candidate_profile_count,
            processed_profile_count,
            inserted_profile_count,
            reused_profile_count,
            rematerialized_profile_count,
            run_status,
            started_at_utc,
            finished_at_utc,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 'running', CURRENT_TIMESTAMP, NULL, NULL)
        """,
        [
            run_id,
            OBJECTIVE_PROFILE_MATERIALIZATION_RUNNER_NAME,
            OBJECTIVE_PROFILE_MATERIALIZATION_RUNNER_VERSION,
            signal_start_date,
            signal_end_date,
            candidate_profile_count,
        ],
    )


def update_objective_profile_materialization_run_success(
    connection: duckdb.DuckDBPyConnection,
    *,
    summary: ObjectiveProfileMaterializationSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {OBJECTIVE_PROFILE_MATERIALIZATION_RUN_TABLE}
        SET
            processed_profile_count = ?,
            inserted_profile_count = ?,
            reused_profile_count = ?,
            rematerialized_profile_count = ?,
            run_status = 'completed',
            finished_at_utc = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            summary.processed_profile_count,
            summary.inserted_profile_count,
            summary.reused_profile_count,
            summary.rematerialized_profile_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            summary.run_id,
        ],
    )


def update_objective_profile_materialization_run_failure(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {OBJECTIVE_PROFILE_MATERIALIZATION_RUN_TABLE}
        SET
            run_status = 'failed',
            finished_at_utc = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [json.dumps(summary_payload, ensure_ascii=False, sort_keys=True), run_id],
    )


def upsert_objective_profile_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    code: str,
    observed_trade_date: date,
    source_digest: str,
    last_materialized_run_id: str,
) -> None:
    checkpoint_nk = build_objective_profile_checkpoint_nk(
        asset_type=asset_type,
        code=code,
        observed_trade_date=observed_trade_date,
    )
    existing = connection.execute(
        f"""
        SELECT checkpoint_nk
        FROM {OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE}
        WHERE checkpoint_nk = ?
        """,
        [checkpoint_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE} (
                checkpoint_nk,
                asset_type,
                code,
                observed_trade_date,
                source_digest,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [checkpoint_nk, asset_type, code, observed_trade_date, source_digest, last_materialized_run_id],
        )
        return
    connection.execute(
        f"""
        UPDATE {OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE}
        SET
            source_digest = ?,
            last_materialized_run_id = ?,
            updated_at_utc = CURRENT_TIMESTAMP
        WHERE checkpoint_nk = ?
        """,
        [source_digest, last_materialized_run_id, checkpoint_nk],
    )


def record_objective_profile_run_profile(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    code: str,
    observed_trade_date: date,
    materialization_action: str,
    source_digest: str,
) -> None:
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {OBJECTIVE_PROFILE_MATERIALIZATION_RUN_PROFILE_TABLE} (
            run_profile_nk,
            run_id,
            asset_type,
            code,
            observed_trade_date,
            materialization_action,
            source_digest,
            recorded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            build_objective_profile_run_profile_nk(
                run_id=run_id,
                asset_type=asset_type,
                code=code,
                observed_trade_date=observed_trade_date,
            ),
            run_id,
            asset_type,
            code,
            observed_trade_date,
            materialization_action,
            source_digest,
        ],
    )
