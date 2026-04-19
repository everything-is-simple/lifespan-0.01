"""`data bootstrap` 中 objective source/materialization 表族常量与 DDL。"""

from __future__ import annotations

from typing import Final


TUSHARE_OBJECTIVE_RUN_TABLE: Final[str] = "tushare_objective_run"
TUSHARE_OBJECTIVE_REQUEST_TABLE: Final[str] = "tushare_objective_request"
TUSHARE_OBJECTIVE_CHECKPOINT_TABLE: Final[str] = "tushare_objective_checkpoint"
TUSHARE_OBJECTIVE_EVENT_TABLE: Final[str] = "tushare_objective_event"
OBJECTIVE_PROFILE_MATERIALIZATION_RUN_TABLE: Final[str] = "objective_profile_materialization_run"
OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE: Final[str] = "objective_profile_materialization_checkpoint"
OBJECTIVE_PROFILE_MATERIALIZATION_RUN_PROFILE_TABLE: Final[str] = "objective_profile_materialization_run_profile"

OBJECTIVE_LEDGER_TABLES: Final[dict[str, str]] = {
    TUSHARE_OBJECTIVE_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS tushare_objective_run (
            run_id TEXT,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            source_api_scope TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            candidate_cursor_count BIGINT NOT NULL DEFAULT 0,
            processed_request_count BIGINT NOT NULL DEFAULT 0,
            successful_request_count BIGINT NOT NULL DEFAULT 0,
            failed_request_count BIGINT NOT NULL DEFAULT 0,
            inserted_event_count BIGINT NOT NULL DEFAULT 0,
            reused_event_count BIGINT NOT NULL DEFAULT 0,
            rematerialized_event_count BIGINT NOT NULL DEFAULT 0,
            run_status TEXT NOT NULL,
            started_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at_utc TIMESTAMP,
            summary_json TEXT
        )
    """,
    TUSHARE_OBJECTIVE_REQUEST_TABLE: """
        CREATE TABLE IF NOT EXISTS tushare_objective_request (
            request_nk TEXT,
            run_id TEXT NOT NULL,
            source_api TEXT NOT NULL,
            cursor_type TEXT NOT NULL,
            cursor_value TEXT NOT NULL,
            response_row_count BIGINT NOT NULL DEFAULT 0,
            inserted_event_count BIGINT NOT NULL DEFAULT 0,
            reused_event_count BIGINT NOT NULL DEFAULT 0,
            rematerialized_event_count BIGINT NOT NULL DEFAULT 0,
            response_digest TEXT,
            request_status TEXT NOT NULL,
            error_message TEXT,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    TUSHARE_OBJECTIVE_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS tushare_objective_checkpoint (
            checkpoint_nk TEXT,
            source_api TEXT NOT NULL,
            cursor_type TEXT NOT NULL,
            cursor_value TEXT NOT NULL,
            last_success_run_id TEXT,
            last_response_digest TEXT,
            last_observed_max_date DATE,
            last_status TEXT,
            updated_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    TUSHARE_OBJECTIVE_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS tushare_objective_event (
            event_nk TEXT,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            source_api TEXT NOT NULL,
            objective_dimension TEXT NOT NULL,
            effective_start_date DATE NOT NULL,
            effective_end_date DATE,
            status_value_code TEXT,
            status_value_text TEXT,
            source_record_hash TEXT NOT NULL,
            source_trade_date DATE,
            source_ann_date DATE,
            payload_json TEXT,
            first_seen_run_id TEXT NOT NULL,
            last_seen_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS objective_profile_materialization_run (
            run_id TEXT,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            candidate_profile_count BIGINT NOT NULL DEFAULT 0,
            processed_profile_count BIGINT NOT NULL DEFAULT 0,
            inserted_profile_count BIGINT NOT NULL DEFAULT 0,
            reused_profile_count BIGINT NOT NULL DEFAULT 0,
            rematerialized_profile_count BIGINT NOT NULL DEFAULT 0,
            run_status TEXT NOT NULL,
            started_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at_utc TIMESTAMP,
            summary_json TEXT
        )
    """,
    OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS objective_profile_materialization_checkpoint (
            checkpoint_nk TEXT,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            observed_trade_date DATE NOT NULL,
            source_digest TEXT,
            last_materialized_run_id TEXT,
            updated_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_PROFILE_TABLE: """
        CREATE TABLE IF NOT EXISTS objective_profile_materialization_run_profile (
            run_profile_nk TEXT,
            run_id TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            observed_trade_date DATE NOT NULL,
            materialization_action TEXT NOT NULL,
            source_digest TEXT,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
}

OBJECTIVE_REQUIRED_COLUMNS: Final[dict[str, dict[str, str]]] = {
    TUSHARE_OBJECTIVE_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "source_api_scope": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "candidate_cursor_count": "BIGINT",
        "processed_request_count": "BIGINT",
        "successful_request_count": "BIGINT",
        "failed_request_count": "BIGINT",
        "inserted_event_count": "BIGINT",
        "reused_event_count": "BIGINT",
        "rematerialized_event_count": "BIGINT",
        "run_status": "TEXT",
        "started_at_utc": "TIMESTAMP",
        "finished_at_utc": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    TUSHARE_OBJECTIVE_REQUEST_TABLE: {
        "request_nk": "TEXT",
        "run_id": "TEXT",
        "source_api": "TEXT",
        "cursor_type": "TEXT",
        "cursor_value": "TEXT",
        "response_row_count": "BIGINT",
        "inserted_event_count": "BIGINT",
        "reused_event_count": "BIGINT",
        "rematerialized_event_count": "BIGINT",
        "response_digest": "TEXT",
        "request_status": "TEXT",
        "error_message": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
    TUSHARE_OBJECTIVE_CHECKPOINT_TABLE: {
        "checkpoint_nk": "TEXT",
        "source_api": "TEXT",
        "cursor_type": "TEXT",
        "cursor_value": "TEXT",
        "last_success_run_id": "TEXT",
        "last_response_digest": "TEXT",
        "last_observed_max_date": "DATE",
        "last_status": "TEXT",
        "updated_at_utc": "TIMESTAMP",
    },
    TUSHARE_OBJECTIVE_EVENT_TABLE: {
        "event_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "source_api": "TEXT",
        "objective_dimension": "TEXT",
        "effective_start_date": "DATE",
        "effective_end_date": "DATE",
        "status_value_code": "TEXT",
        "status_value_text": "TEXT",
        "source_record_hash": "TEXT",
        "source_trade_date": "DATE",
        "source_ann_date": "DATE",
        "payload_json": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_seen_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "candidate_profile_count": "BIGINT",
        "processed_profile_count": "BIGINT",
        "inserted_profile_count": "BIGINT",
        "reused_profile_count": "BIGINT",
        "rematerialized_profile_count": "BIGINT",
        "run_status": "TEXT",
        "started_at_utc": "TIMESTAMP",
        "finished_at_utc": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE: {
        "checkpoint_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "observed_trade_date": "DATE",
        "source_digest": "TEXT",
        "last_materialized_run_id": "TEXT",
        "updated_at_utc": "TIMESTAMP",
    },
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_PROFILE_TABLE: {
        "run_profile_nk": "TEXT",
        "run_id": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "observed_trade_date": "DATE",
        "materialization_action": "TEXT",
        "source_digest": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
}

OBJECTIVE_NOT_NULL_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    TUSHARE_OBJECTIVE_RUN_TABLE: (
        "run_id",
        "runner_name",
        "runner_version",
        "source_api_scope",
        "candidate_cursor_count",
        "processed_request_count",
        "successful_request_count",
        "failed_request_count",
        "inserted_event_count",
        "reused_event_count",
        "rematerialized_event_count",
        "run_status",
        "started_at_utc",
    ),
    TUSHARE_OBJECTIVE_REQUEST_TABLE: (
        "request_nk",
        "run_id",
        "source_api",
        "cursor_type",
        "cursor_value",
        "response_row_count",
        "inserted_event_count",
        "reused_event_count",
        "rematerialized_event_count",
        "request_status",
        "recorded_at",
    ),
    TUSHARE_OBJECTIVE_CHECKPOINT_TABLE: (
        "checkpoint_nk",
        "source_api",
        "cursor_type",
        "cursor_value",
        "updated_at_utc",
    ),
    TUSHARE_OBJECTIVE_EVENT_TABLE: (
        "event_nk",
        "asset_type",
        "code",
        "source_api",
        "objective_dimension",
        "effective_start_date",
        "source_record_hash",
        "first_seen_run_id",
        "last_seen_run_id",
        "created_at",
        "updated_at",
    ),
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_TABLE: (
        "run_id",
        "runner_name",
        "runner_version",
        "candidate_profile_count",
        "processed_profile_count",
        "inserted_profile_count",
        "reused_profile_count",
        "rematerialized_profile_count",
        "run_status",
        "started_at_utc",
    ),
    OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE: (
        "checkpoint_nk",
        "asset_type",
        "code",
        "observed_trade_date",
        "updated_at_utc",
    ),
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_PROFILE_TABLE: (
        "run_profile_nk",
        "run_id",
        "asset_type",
        "code",
        "observed_trade_date",
        "materialization_action",
        "recorded_at",
    ),
}

OBJECTIVE_UNIQUE_INDEXES: Final[dict[str, tuple[tuple[str, tuple[str, ...]], ...]]] = {
    TUSHARE_OBJECTIVE_RUN_TABLE: (("ux_tushare_objective_run_run_id", ("run_id",)),),
    TUSHARE_OBJECTIVE_REQUEST_TABLE: (("ux_tushare_objective_request_request_nk", ("request_nk",)),),
    TUSHARE_OBJECTIVE_CHECKPOINT_TABLE: (("ux_tushare_objective_checkpoint_checkpoint_nk", ("checkpoint_nk",)),),
    TUSHARE_OBJECTIVE_EVENT_TABLE: (("ux_tushare_objective_event_event_nk", ("event_nk",)),),
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_TABLE: (
        ("ux_objective_profile_materialization_run_run_id", ("run_id",)),
    ),
    OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE: (
        ("ux_objective_profile_materialization_checkpoint_checkpoint_nk", ("checkpoint_nk",)),
    ),
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_PROFILE_TABLE: (
        ("ux_objective_profile_materialization_run_profile_nk", ("run_profile_nk",)),
    ),
}
