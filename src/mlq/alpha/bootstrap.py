"""冻结 `alpha` 模块正式账本表族的 bootstrap。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


ALPHA_TRIGGER_RUN_TABLE: Final[str] = "alpha_trigger_run"
ALPHA_TRIGGER_WORK_QUEUE_TABLE: Final[str] = "alpha_trigger_work_queue"
ALPHA_TRIGGER_CHECKPOINT_TABLE: Final[str] = "alpha_trigger_checkpoint"
ALPHA_PAS_TRIGGER_RUN_TABLE: Final[str] = "alpha_pas_trigger_run"
ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE: Final[str] = "alpha_pas_trigger_work_queue"
ALPHA_PAS_TRIGGER_CHECKPOINT_TABLE: Final[str] = "alpha_pas_trigger_checkpoint"
ALPHA_TRIGGER_CANDIDATE_TABLE: Final[str] = "alpha_trigger_candidate"
ALPHA_PAS_TRIGGER_RUN_CANDIDATE_TABLE: Final[str] = "alpha_pas_trigger_run_candidate"
ALPHA_TRIGGER_EVENT_TABLE: Final[str] = "alpha_trigger_event"
ALPHA_TRIGGER_RUN_EVENT_TABLE: Final[str] = "alpha_trigger_run_event"

ALPHA_FORMAL_SIGNAL_RUN_TABLE: Final[str] = "alpha_formal_signal_run"
ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE: Final[str] = "alpha_formal_signal_work_queue"
ALPHA_FORMAL_SIGNAL_CHECKPOINT_TABLE: Final[str] = "alpha_formal_signal_checkpoint"
ALPHA_FORMAL_SIGNAL_EVENT_TABLE: Final[str] = "alpha_formal_signal_event"
ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE: Final[str] = "alpha_formal_signal_run_event"
ALPHA_FAMILY_RUN_TABLE: Final[str] = "alpha_family_run"
ALPHA_FAMILY_EVENT_TABLE: Final[str] = "alpha_family_event"
ALPHA_FAMILY_RUN_EVENT_TABLE: Final[str] = "alpha_family_run_event"


ALPHA_TRIGGER_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    ALPHA_PAS_TRIGGER_RUN_TABLE,
    ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE,
    ALPHA_PAS_TRIGGER_CHECKPOINT_TABLE,
    ALPHA_TRIGGER_CANDIDATE_TABLE,
    ALPHA_PAS_TRIGGER_RUN_CANDIDATE_TABLE,
    ALPHA_TRIGGER_RUN_TABLE,
    ALPHA_TRIGGER_WORK_QUEUE_TABLE,
    ALPHA_TRIGGER_CHECKPOINT_TABLE,
    ALPHA_TRIGGER_EVENT_TABLE,
    ALPHA_TRIGGER_RUN_EVENT_TABLE,
)


ALPHA_FORMAL_SIGNAL_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    ALPHA_FORMAL_SIGNAL_RUN_TABLE,
    ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE,
    ALPHA_FORMAL_SIGNAL_CHECKPOINT_TABLE,
    ALPHA_FORMAL_SIGNAL_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE,
)


ALPHA_FAMILY_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    ALPHA_FAMILY_RUN_TABLE,
    ALPHA_FAMILY_EVENT_TABLE,
    ALPHA_FAMILY_RUN_EVENT_TABLE,
)


ALPHA_LEDGER_TABLE_NAMES: Final[tuple[str, ...]] = (
    *ALPHA_TRIGGER_LEDGER_TABLE_NAMES,
    *ALPHA_FORMAL_SIGNAL_LEDGER_TABLE_NAMES,
    *ALPHA_FAMILY_LEDGER_TABLE_NAMES,
)


ALPHA_LEDGER_DDL: Final[dict[str, str]] = {
    ALPHA_PAS_TRIGGER_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_pas_trigger_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            candidate_scope_count BIGINT NOT NULL DEFAULT 0,
            materialized_candidate_count BIGINT NOT NULL DEFAULT 0,
            source_filter_table TEXT NOT NULL,
            source_structure_table TEXT NOT NULL,
            source_price_table TEXT NOT NULL,
            source_adjust_method TEXT NOT NULL,
            detector_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT,
            notes TEXT
        )
    """,
    ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_pas_trigger_work_queue (
            queue_nk TEXT PRIMARY KEY,
            scope_nk TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            dirty_reason TEXT NOT NULL,
            replay_start_bar_dt DATE,
            replay_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            queue_status TEXT NOT NULL,
            enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            claimed_at TIMESTAMP,
            completed_at TIMESTAMP,
            first_seen_run_id TEXT,
            last_claimed_run_id TEXT,
            last_materialized_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    ALPHA_PAS_TRIGGER_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_pas_trigger_checkpoint (
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            last_completed_bar_dt DATE,
            tail_start_bar_dt DATE,
            tail_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (asset_type, code, timeframe)
        )
    """,
    ALPHA_TRIGGER_CANDIDATE_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_trigger_candidate (
            candidate_nk TEXT PRIMARY KEY,
            instrument TEXT NOT NULL,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            trigger_family TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            pattern_code TEXT NOT NULL,
            family_code TEXT NOT NULL,
            trigger_strength DOUBLE NOT NULL DEFAULT 0,
            detect_reason TEXT NOT NULL,
            skip_reason TEXT,
            price_context_json TEXT NOT NULL DEFAULT '{}',
            structure_context_json TEXT NOT NULL DEFAULT '{}',
            detector_trace_json TEXT NOT NULL DEFAULT '{}',
            source_filter_snapshot_nk TEXT,
            source_structure_snapshot_nk TEXT,
            source_price_fingerprint TEXT NOT NULL DEFAULT '{}',
            detector_contract_version TEXT,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    ALPHA_PAS_TRIGGER_RUN_CANDIDATE_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_pas_trigger_run_candidate (
            run_id TEXT NOT NULL,
            candidate_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            family_code TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, candidate_nk)
        )
    """,
    ALPHA_TRIGGER_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_trigger_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            candidate_trigger_count BIGINT NOT NULL DEFAULT 0,
            source_trigger_input_table TEXT NOT NULL,
            source_filter_table TEXT NOT NULL,
            source_structure_table TEXT NOT NULL,
            trigger_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT,
            notes TEXT
        )
    """,
    ALPHA_TRIGGER_WORK_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_trigger_work_queue (
            queue_nk TEXT PRIMARY KEY,
            scope_nk TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            dirty_reason TEXT NOT NULL,
            replay_start_bar_dt DATE,
            replay_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            queue_status TEXT NOT NULL,
            enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            claimed_at TIMESTAMP,
            completed_at TIMESTAMP,
            first_seen_run_id TEXT,
            last_claimed_run_id TEXT,
            last_materialized_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    ALPHA_TRIGGER_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_trigger_checkpoint (
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            last_completed_bar_dt DATE,
            tail_start_bar_dt DATE,
            tail_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (asset_type, code, timeframe)
        )
    """,
    ALPHA_TRIGGER_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_trigger_event (
            trigger_event_nk TEXT PRIMARY KEY,
            instrument TEXT NOT NULL,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            trigger_family TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            pattern_code TEXT NOT NULL,
            source_filter_snapshot_nk TEXT NOT NULL,
            source_structure_snapshot_nk TEXT NOT NULL,
            daily_source_context_nk TEXT,
            weekly_major_state TEXT,
            weekly_trend_direction TEXT,
            weekly_reversal_stage TEXT,
            weekly_source_context_nk TEXT,
            monthly_major_state TEXT,
            monthly_trend_direction TEXT,
            monthly_reversal_stage TEXT,
            monthly_source_context_nk TEXT,
            upstream_context_fingerprint TEXT NOT NULL,
            trigger_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    ALPHA_TRIGGER_RUN_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_trigger_run_event (
            run_id TEXT NOT NULL,
            trigger_event_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, trigger_event_nk)
        )
    """,
    ALPHA_FORMAL_SIGNAL_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_formal_signal_run (
            run_id TEXT PRIMARY KEY,
            producer_name TEXT NOT NULL,
            producer_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            source_trigger_table TEXT NOT NULL,
            source_family_table TEXT,
            source_context_table TEXT NOT NULL,
            signal_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT,
            notes TEXT
        )
    """,
    ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_formal_signal_work_queue (
            queue_nk TEXT PRIMARY KEY,
            scope_nk TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            dirty_reason TEXT NOT NULL,
            replay_start_bar_dt DATE,
            replay_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            queue_status TEXT NOT NULL,
            enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            claimed_at TIMESTAMP,
            completed_at TIMESTAMP,
            first_seen_run_id TEXT,
            last_claimed_run_id TEXT,
            last_materialized_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    ALPHA_FORMAL_SIGNAL_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_formal_signal_checkpoint (
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            last_completed_bar_dt DATE,
            tail_start_bar_dt DATE,
            tail_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (asset_type, code, timeframe)
        )
    """,
    ALPHA_FORMAL_SIGNAL_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_formal_signal_event (
            signal_nk TEXT PRIMARY KEY,
            instrument TEXT NOT NULL,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            trigger_family TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            pattern_code TEXT NOT NULL,
            formal_signal_status TEXT NOT NULL,
            trigger_admissible BOOLEAN NOT NULL,
            major_state TEXT NOT NULL,
            trend_direction TEXT NOT NULL,
            reversal_stage TEXT NOT NULL,
            wave_id BIGINT NOT NULL,
            current_hh_count BIGINT NOT NULL,
            current_ll_count BIGINT NOT NULL,
            malf_context_4 TEXT NOT NULL,
            lifecycle_rank_high BIGINT NOT NULL,
            lifecycle_rank_total BIGINT NOT NULL,
            daily_source_context_nk TEXT,
            weekly_major_state TEXT,
            weekly_trend_direction TEXT,
            weekly_reversal_stage TEXT,
            weekly_source_context_nk TEXT,
            monthly_major_state TEXT,
            monthly_trend_direction TEXT,
            monthly_reversal_stage TEXT,
            monthly_source_context_nk TEXT,
            source_family_event_nk TEXT,
            family_code TEXT,
            source_family_contract_version TEXT,
            family_role TEXT,
            family_bias TEXT,
            malf_alignment TEXT,
            malf_phase_bucket TEXT,
            family_source_context_fingerprint TEXT,
            wave_life_percentile DOUBLE,
            remaining_life_bars_p50 DOUBLE,
            remaining_life_bars_p75 DOUBLE,
            termination_risk_bucket TEXT,
            stage_percentile_decision_code TEXT,
            stage_percentile_action_owner TEXT,
            stage_percentile_note TEXT,
            stage_percentile_contract_version TEXT,
            source_trigger_event_nk TEXT NOT NULL,
            signal_contract_version TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_formal_signal_run_event (
            run_id TEXT NOT NULL,
            signal_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            formal_signal_status TEXT NOT NULL,
            source_family_event_nk TEXT,
            family_code TEXT,
            family_role TEXT,
            malf_alignment TEXT,
            family_source_context_fingerprint TEXT,
            stage_percentile_decision_code TEXT,
            stage_percentile_action_owner TEXT,
            source_trigger_event_nk TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, signal_nk)
        )
    """,
    ALPHA_FAMILY_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_family_run (
            run_id TEXT PRIMARY KEY,
            producer_name TEXT NOT NULL,
            producer_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            family_scope TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            materialized_family_event_count BIGINT NOT NULL DEFAULT 0,
            source_trigger_table TEXT NOT NULL,
            source_candidate_table TEXT NOT NULL,
            family_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT,
            notes TEXT
        )
    """,
    ALPHA_FAMILY_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_family_event (
            family_event_nk TEXT PRIMARY KEY,
            trigger_event_nk TEXT NOT NULL,
            instrument TEXT NOT NULL,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            trigger_family TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            pattern_code TEXT NOT NULL,
            family_code TEXT NOT NULL,
            family_contract_version TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    ALPHA_FAMILY_RUN_EVENT_TABLE: """
        CREATE TABLE IF NOT EXISTS alpha_family_run_event (
            run_id TEXT NOT NULL,
            family_event_nk TEXT NOT NULL,
            trigger_event_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            family_code TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, family_event_nk)
        )
    """,
}


ALPHA_FORMAL_SIGNAL_REQUIRED_COLUMNS: Final[dict[str, dict[str, str]]] = {
    ALPHA_PAS_TRIGGER_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "run_status": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_instrument_count": "BIGINT",
        "candidate_scope_count": "BIGINT",
        "materialized_candidate_count": "BIGINT",
        "source_filter_table": "TEXT",
        "source_structure_table": "TEXT",
        "source_price_table": "TEXT",
        "source_adjust_method": "TEXT",
        "detector_contract_version": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
        "notes": "TEXT",
    },
    ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE: {
        "queue_nk": "TEXT",
        "scope_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "dirty_reason": "TEXT",
        "replay_start_bar_dt": "DATE",
        "replay_confirm_until_dt": "DATE",
        "source_fingerprint": "TEXT",
        "queue_status": "TEXT",
        "enqueued_at": "TIMESTAMP",
        "claimed_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "first_seen_run_id": "TEXT",
        "last_claimed_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    ALPHA_PAS_TRIGGER_CHECKPOINT_TABLE: {
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "last_completed_bar_dt": "DATE",
        "tail_start_bar_dt": "DATE",
        "tail_confirm_until_dt": "DATE",
        "source_fingerprint": "TEXT",
        "last_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    ALPHA_TRIGGER_CANDIDATE_TABLE: {
        "candidate_nk": "TEXT",
        "instrument": "TEXT",
        "signal_date": "DATE",
        "asof_date": "DATE",
        "trigger_family": "TEXT",
        "trigger_type": "TEXT",
        "pattern_code": "TEXT",
        "family_code": "TEXT",
        "trigger_strength": "DOUBLE",
        "detect_reason": "TEXT",
        "skip_reason": "TEXT",
        "price_context_json": "TEXT",
        "structure_context_json": "TEXT",
        "detector_trace_json": "TEXT",
        "source_filter_snapshot_nk": "TEXT",
        "source_structure_snapshot_nk": "TEXT",
        "source_price_fingerprint": "TEXT",
        "detector_contract_version": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    ALPHA_PAS_TRIGGER_RUN_CANDIDATE_TABLE: {
        "run_id": "TEXT",
        "candidate_nk": "TEXT",
        "materialization_action": "TEXT",
        "trigger_type": "TEXT",
        "family_code": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
    ALPHA_TRIGGER_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "run_status": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_instrument_count": "BIGINT",
        "candidate_trigger_count": "BIGINT",
        "source_trigger_input_table": "TEXT",
        "source_filter_table": "TEXT",
        "source_structure_table": "TEXT",
        "trigger_contract_version": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
        "notes": "TEXT",
    },
    ALPHA_TRIGGER_WORK_QUEUE_TABLE: {
        "queue_nk": "TEXT",
        "scope_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "dirty_reason": "TEXT",
        "replay_start_bar_dt": "DATE",
        "replay_confirm_until_dt": "DATE",
        "source_fingerprint": "TEXT",
        "queue_status": "TEXT",
        "enqueued_at": "TIMESTAMP",
        "claimed_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "first_seen_run_id": "TEXT",
        "last_claimed_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    ALPHA_TRIGGER_CHECKPOINT_TABLE: {
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "last_completed_bar_dt": "DATE",
        "tail_start_bar_dt": "DATE",
        "tail_confirm_until_dt": "DATE",
        "source_fingerprint": "TEXT",
        "last_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    ALPHA_TRIGGER_EVENT_TABLE: {
        "trigger_event_nk": "TEXT",
        "instrument": "TEXT",
        "signal_date": "DATE",
        "asof_date": "DATE",
        "trigger_family": "TEXT",
        "trigger_type": "TEXT",
        "pattern_code": "TEXT",
        "source_filter_snapshot_nk": "TEXT",
        "source_structure_snapshot_nk": "TEXT",
        "daily_source_context_nk": "TEXT",
        "weekly_major_state": "TEXT",
        "weekly_trend_direction": "TEXT",
        "weekly_reversal_stage": "TEXT",
        "weekly_source_context_nk": "TEXT",
        "monthly_major_state": "TEXT",
        "monthly_trend_direction": "TEXT",
        "monthly_reversal_stage": "TEXT",
        "monthly_source_context_nk": "TEXT",
        "upstream_context_fingerprint": "TEXT",
        "trigger_contract_version": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    ALPHA_TRIGGER_RUN_EVENT_TABLE: {
        "run_id": "TEXT",
        "trigger_event_nk": "TEXT",
        "materialization_action": "TEXT",
        "trigger_type": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
    ALPHA_FORMAL_SIGNAL_RUN_TABLE: {
        "run_id": "TEXT",
        "producer_name": "TEXT",
        "producer_version": "TEXT",
        "run_status": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_instrument_count": "BIGINT",
        "source_trigger_table": "TEXT",
        "source_family_table": "TEXT",
        "source_context_table": "TEXT",
        "signal_contract_version": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
        "notes": "TEXT",
    },
    ALPHA_FORMAL_SIGNAL_WORK_QUEUE_TABLE: {
        "queue_nk": "TEXT",
        "scope_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "dirty_reason": "TEXT",
        "replay_start_bar_dt": "DATE",
        "replay_confirm_until_dt": "DATE",
        "source_fingerprint": "TEXT",
        "queue_status": "TEXT",
        "enqueued_at": "TIMESTAMP",
        "claimed_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "first_seen_run_id": "TEXT",
        "last_claimed_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    ALPHA_FORMAL_SIGNAL_CHECKPOINT_TABLE: {
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "last_completed_bar_dt": "DATE",
        "tail_start_bar_dt": "DATE",
        "tail_confirm_until_dt": "DATE",
        "source_fingerprint": "TEXT",
        "last_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    ALPHA_FORMAL_SIGNAL_EVENT_TABLE: {
        "signal_nk": "TEXT",
        "instrument": "TEXT",
        "signal_date": "DATE",
        "asof_date": "DATE",
        "trigger_family": "TEXT",
        "trigger_type": "TEXT",
        "pattern_code": "TEXT",
        "formal_signal_status": "TEXT",
        "trigger_admissible": "BOOLEAN",
        "major_state": "TEXT",
        "trend_direction": "TEXT",
        "reversal_stage": "TEXT",
        "wave_id": "BIGINT",
        "current_hh_count": "BIGINT",
        "current_ll_count": "BIGINT",
        "malf_context_4": "TEXT",
        "lifecycle_rank_high": "BIGINT",
        "lifecycle_rank_total": "BIGINT",
        "daily_source_context_nk": "TEXT",
        "weekly_major_state": "TEXT",
        "weekly_trend_direction": "TEXT",
        "weekly_reversal_stage": "TEXT",
        "weekly_source_context_nk": "TEXT",
        "monthly_major_state": "TEXT",
        "monthly_trend_direction": "TEXT",
        "monthly_reversal_stage": "TEXT",
        "monthly_source_context_nk": "TEXT",
        "source_family_event_nk": "TEXT",
        "family_code": "TEXT",
        "source_family_contract_version": "TEXT",
        "family_role": "TEXT",
        "family_bias": "TEXT",
        "malf_alignment": "TEXT",
        "malf_phase_bucket": "TEXT",
        "family_source_context_fingerprint": "TEXT",
        "wave_life_percentile": "DOUBLE",
        "remaining_life_bars_p50": "DOUBLE",
        "remaining_life_bars_p75": "DOUBLE",
        "termination_risk_bucket": "TEXT",
        "stage_percentile_decision_code": "TEXT",
        "stage_percentile_action_owner": "TEXT",
        "stage_percentile_note": "TEXT",
        "stage_percentile_contract_version": "TEXT",
        "source_trigger_event_nk": "TEXT",
        "signal_contract_version": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE: {
        "run_id": "TEXT",
        "signal_nk": "TEXT",
        "materialization_action": "TEXT",
        "formal_signal_status": "TEXT",
        "source_family_event_nk": "TEXT",
        "family_code": "TEXT",
        "family_role": "TEXT",
        "malf_alignment": "TEXT",
        "family_source_context_fingerprint": "TEXT",
        "stage_percentile_decision_code": "TEXT",
        "stage_percentile_action_owner": "TEXT",
        "source_trigger_event_nk": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
}


def connect_alpha_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `alpha` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(workspace.databases.alpha), read_only=read_only)


def bootstrap_alpha_trigger_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建 `alpha trigger ledger` 最小三表。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_alpha_ledger(workspace)
    try:
        for table_name in ALPHA_TRIGGER_LEDGER_TABLE_NAMES:
            conn.execute(ALPHA_LEDGER_DDL[table_name])
        for table_name, column_map in ALPHA_FORMAL_SIGNAL_REQUIRED_COLUMNS.items():
            if table_name not in ALPHA_TRIGGER_LEDGER_TABLE_NAMES:
                continue
            _ensure_columns(conn, table_name=table_name, required_columns=column_map)
        return ALPHA_TRIGGER_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def bootstrap_alpha_pas_trigger_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建 `alpha PAS detector` 表族与官方 candidate 输出表。"""

    return bootstrap_alpha_trigger_ledger(settings, connection=connection)


def bootstrap_alpha_formal_signal_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建 `alpha formal signal` 三表。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_alpha_ledger(workspace)
    try:
        for table_name in ALPHA_FORMAL_SIGNAL_LEDGER_TABLE_NAMES:
            conn.execute(ALPHA_LEDGER_DDL[table_name])
        for table_name, column_map in ALPHA_FORMAL_SIGNAL_REQUIRED_COLUMNS.items():
            _ensure_columns(conn, table_name=table_name, required_columns=column_map)
        return ALPHA_FORMAL_SIGNAL_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def bootstrap_alpha_family_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    """创建 `alpha family ledger` 最小三表。"""

    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_alpha_ledger(workspace)
    try:
        for table_name in ALPHA_FAMILY_LEDGER_TABLE_NAMES:
            conn.execute(ALPHA_LEDGER_DDL[table_name])
        return ALPHA_FAMILY_LEDGER_TABLE_NAMES
    finally:
        if owns_connection:
            conn.close()


def alpha_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `alpha` 账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.alpha


def _ensure_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    required_columns: dict[str, str],
) -> None:
    existing_rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchall()
    existing_columns = {str(row[0]) for row in existing_rows}
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
