"""冻结 `malf` 模块最小正式快照表族。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings


MALF_RUN_TABLE: Final[str] = "malf_run"
PAS_CONTEXT_SNAPSHOT_TABLE: Final[str] = "pas_context_snapshot"
STRUCTURE_CANDIDATE_SNAPSHOT_TABLE: Final[str] = "structure_candidate_snapshot"
MALF_RUN_CONTEXT_SNAPSHOT_TABLE: Final[str] = "malf_run_context_snapshot"
MALF_RUN_STRUCTURE_SNAPSHOT_TABLE: Final[str] = "malf_run_structure_snapshot"
MALF_MECHANISM_RUN_TABLE: Final[str] = "malf_mechanism_run"
MALF_MECHANISM_CHECKPOINT_TABLE: Final[str] = "malf_mechanism_checkpoint"
PIVOT_CONFIRMED_BREAK_LEDGER_TABLE: Final[str] = "pivot_confirmed_break_ledger"
SAME_TIMEFRAME_STATS_PROFILE_TABLE: Final[str] = "same_timeframe_stats_profile"
SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE: Final[str] = "same_timeframe_stats_snapshot"
MALF_CANONICAL_RUN_TABLE: Final[str] = "malf_canonical_run"
MALF_CANONICAL_WORK_QUEUE_TABLE: Final[str] = "malf_canonical_work_queue"
MALF_CANONICAL_CHECKPOINT_TABLE: Final[str] = "malf_canonical_checkpoint"
MALF_PIVOT_LEDGER_TABLE: Final[str] = "malf_pivot_ledger"
MALF_WAVE_LEDGER_TABLE: Final[str] = "malf_wave_ledger"
MALF_EXTREME_PROGRESS_LEDGER_TABLE: Final[str] = "malf_extreme_progress_ledger"
MALF_STATE_SNAPSHOT_TABLE: Final[str] = "malf_state_snapshot"
MALF_SAME_LEVEL_STATS_TABLE: Final[str] = "malf_same_level_stats"
MALF_WAVE_LIFE_RUN_TABLE: Final[str] = "malf_wave_life_run"
MALF_WAVE_LIFE_WORK_QUEUE_TABLE: Final[str] = "malf_wave_life_work_queue"
MALF_WAVE_LIFE_CHECKPOINT_TABLE: Final[str] = "malf_wave_life_checkpoint"
MALF_WAVE_LIFE_SNAPSHOT_TABLE: Final[str] = "malf_wave_life_snapshot"
MALF_WAVE_LIFE_PROFILE_TABLE: Final[str] = "malf_wave_life_profile"


MALF_LEDGER_TABLES: Final[dict[str, str]] = {
    MALF_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_run (
            run_id TEXT,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            source_price_table TEXT NOT NULL,
            adjust_method TEXT NOT NULL,
            malf_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    PAS_CONTEXT_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS pas_context_snapshot (
            context_nk TEXT,
            entity_code TEXT NOT NULL,
            entity_name TEXT,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            source_context_nk TEXT,
            malf_context_4 TEXT NOT NULL,
            lifecycle_rank_high BIGINT NOT NULL,
            lifecycle_rank_total BIGINT NOT NULL,
            calc_date DATE,
            adjust_method TEXT,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    STRUCTURE_CANDIDATE_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS structure_candidate_snapshot (
            candidate_nk TEXT,
            instrument TEXT NOT NULL,
            instrument_name TEXT,
            signal_date DATE NOT NULL,
            asof_date DATE NOT NULL,
            new_high_count BIGINT NOT NULL,
            new_low_count BIGINT NOT NULL,
            refresh_density DOUBLE NOT NULL,
            advancement_density DOUBLE NOT NULL,
            is_failed_extreme BOOLEAN NOT NULL,
            failure_type TEXT,
            adjust_method TEXT,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_RUN_CONTEXT_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_run_context_snapshot (
            run_id TEXT NOT NULL,
            context_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_RUN_STRUCTURE_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_run_structure_snapshot (
            run_id TEXT NOT NULL,
            candidate_nk TEXT NOT NULL,
            materialization_action TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_MECHANISM_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_mechanism_run (
            run_id TEXT,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_instrument_count BIGINT NOT NULL DEFAULT 0,
            source_context_table TEXT NOT NULL,
            source_structure_input_table TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            stats_sample_version TEXT NOT NULL,
            mechanism_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    MALF_MECHANISM_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_mechanism_checkpoint (
            instrument TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            last_signal_date DATE,
            last_asof_date DATE,
            last_run_id TEXT,
            source_context_nk TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (instrument, timeframe)
        )
    """,
    PIVOT_CONFIRMED_BREAK_LEDGER_TABLE: """
        CREATE TABLE IF NOT EXISTS pivot_confirmed_break_ledger (
            break_event_nk TEXT,
            instrument TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            guard_pivot_id TEXT NOT NULL,
            guard_pivot_role TEXT NOT NULL,
            origin_context TEXT NOT NULL,
            trigger_bar_dt DATE NOT NULL,
            trigger_price_proxy DOUBLE,
            break_direction TEXT NOT NULL,
            confirmation_status TEXT NOT NULL,
            confirmation_bar_dt DATE,
            confirmation_pivot_id TEXT,
            confirmation_pivot_role TEXT,
            source_context_nk TEXT,
            source_candidate_nk TEXT,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    SAME_TIMEFRAME_STATS_PROFILE_TABLE: """
        CREATE TABLE IF NOT EXISTS same_timeframe_stats_profile (
            stats_profile_nk TEXT,
            universe TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            regime_family TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            sample_version TEXT NOT NULL,
            sample_size BIGINT NOT NULL,
            p10 DOUBLE,
            p25 DOUBLE,
            p50 DOUBLE,
            p75 DOUBLE,
            p90 DOUBLE,
            mean DOUBLE,
            std DOUBLE,
            bucket_definition_json TEXT NOT NULL,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS same_timeframe_stats_snapshot (
            stats_snapshot_nk TEXT,
            instrument TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            signal_date DATE NOT NULL,
            asof_bar_dt DATE NOT NULL,
            regime_family TEXT NOT NULL,
            sample_version TEXT NOT NULL,
            stats_contract_version TEXT NOT NULL,
            source_context_nk TEXT,
            source_candidate_nk TEXT,
            new_high_count_percentile DOUBLE,
            new_low_count_percentile DOUBLE,
            refresh_density_percentile DOUBLE,
            advancement_density_percentile DOUBLE,
            exhaustion_risk_bucket TEXT,
            reversal_probability_bucket TEXT,
            source_profile_refs_json TEXT NOT NULL,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_CANONICAL_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_canonical_run (
            run_id TEXT,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_scope_count BIGINT NOT NULL DEFAULT 0,
            pivot_confirmation_window BIGINT NOT NULL,
            source_price_table TEXT NOT NULL,
            source_adjust_method TEXT NOT NULL,
            canonical_contract_version TEXT NOT NULL,
            timeframe_list_json TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    MALF_CANONICAL_WORK_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_canonical_work_queue (
            queue_nk TEXT PRIMARY KEY,
            scope_nk TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            dirty_reason TEXT NOT NULL,
            source_last_trade_date DATE,
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
    MALF_CANONICAL_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_canonical_checkpoint (
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            last_completed_bar_dt DATE,
            tail_start_bar_dt DATE,
            tail_confirm_until_dt DATE,
            last_wave_id BIGINT NOT NULL DEFAULT 0,
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (asset_type, code, timeframe)
        )
    """,
    MALF_PIVOT_LEDGER_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_pivot_ledger (
            pivot_nk TEXT PRIMARY KEY,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            pivot_type TEXT NOT NULL,
            pivot_bar_dt DATE NOT NULL,
            confirmed_at DATE NOT NULL,
            pivot_price DOUBLE NOT NULL,
            prior_pivot_nk TEXT,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_WAVE_LEDGER_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_wave_ledger (
            wave_nk TEXT PRIMARY KEY,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            wave_id BIGINT NOT NULL,
            direction TEXT NOT NULL,
            major_state TEXT NOT NULL,
            reversal_stage TEXT NOT NULL,
            start_bar_dt DATE NOT NULL,
            end_bar_dt DATE,
            active_flag BOOLEAN NOT NULL,
            start_pivot_nk TEXT,
            end_pivot_nk TEXT,
            hh_count BIGINT NOT NULL DEFAULT 0,
            ll_count BIGINT NOT NULL DEFAULT 0,
            bar_count BIGINT NOT NULL DEFAULT 0,
            wave_high DOUBLE,
            wave_low DOUBLE,
            range_ratio DOUBLE,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_EXTREME_PROGRESS_LEDGER_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_extreme_progress_ledger (
            extreme_nk TEXT PRIMARY KEY,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            wave_id BIGINT NOT NULL,
            extreme_seq BIGINT NOT NULL,
            extreme_type TEXT NOT NULL,
            break_base_extreme_nk TEXT,
            record_bar_dt DATE NOT NULL,
            record_price DOUBLE NOT NULL,
            cumulative_count BIGINT NOT NULL,
            major_state TEXT NOT NULL,
            trend_direction TEXT NOT NULL,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_STATE_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_state_snapshot (
            snapshot_nk TEXT PRIMARY KEY,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            asof_bar_dt DATE NOT NULL,
            major_state TEXT NOT NULL,
            trend_direction TEXT NOT NULL,
            reversal_stage TEXT NOT NULL,
            wave_id BIGINT NOT NULL DEFAULT 0,
            last_confirmed_h_bar_dt DATE,
            last_confirmed_h_price DOUBLE,
            last_confirmed_l_bar_dt DATE,
            last_confirmed_l_price DOUBLE,
            last_valid_hl_bar_dt DATE,
            last_valid_hl_price DOUBLE,
            last_valid_lh_bar_dt DATE,
            last_valid_lh_price DOUBLE,
            current_hh_count BIGINT NOT NULL DEFAULT 0,
            current_ll_count BIGINT NOT NULL DEFAULT 0,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_SAME_LEVEL_STATS_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_same_level_stats (
            stats_nk TEXT PRIMARY KEY,
            universe TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            major_state TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            sample_version TEXT NOT NULL,
            sample_size BIGINT NOT NULL,
            p10 DOUBLE,
            p25 DOUBLE,
            p50 DOUBLE,
            p75 DOUBLE,
            p90 DOUBLE,
            mean DOUBLE,
            std DOUBLE,
            first_seen_run_id TEXT,
            last_materialized_run_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_WAVE_LIFE_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_wave_life_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            execution_mode TEXT NOT NULL,
            signal_start_date DATE,
            signal_end_date DATE,
            bounded_scope_count BIGINT NOT NULL DEFAULT 0,
            claimed_scope_count BIGINT NOT NULL DEFAULT 0,
            source_wave_table TEXT NOT NULL,
            source_state_table TEXT NOT NULL,
            source_stats_table TEXT NOT NULL,
            sample_version TEXT NOT NULL,
            life_contract_version TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    MALF_WAVE_LIFE_WORK_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_wave_life_work_queue (
            queue_nk TEXT PRIMARY KEY,
            scope_nk TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            dirty_reason TEXT NOT NULL,
            replay_start_bar_dt DATE,
            replay_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            sample_version TEXT NOT NULL,
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
    MALF_WAVE_LIFE_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_wave_life_checkpoint (
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            last_completed_bar_dt DATE,
            tail_start_bar_dt DATE,
            tail_confirm_until_dt DATE,
            last_sample_version TEXT NOT NULL,
            source_fingerprint TEXT NOT NULL,
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (asset_type, code, timeframe)
        )
    """,
    MALF_WAVE_LIFE_SNAPSHOT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_wave_life_snapshot (
            snapshot_nk TEXT PRIMARY KEY,
            asset_type TEXT NOT NULL,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            asof_bar_dt DATE NOT NULL,
            wave_id BIGINT NOT NULL,
            source_wave_nk TEXT NOT NULL,
            source_state_snapshot_nk TEXT NOT NULL,
            major_state TEXT NOT NULL,
            reversal_stage TEXT NOT NULL,
            active_wave_bar_age BIGINT NOT NULL,
            wave_life_percentile DOUBLE,
            remaining_life_bars_p50 DOUBLE,
            remaining_life_bars_p75 DOUBLE,
            termination_risk_bucket TEXT,
            sample_size BIGINT NOT NULL DEFAULT 0,
            sample_version TEXT NOT NULL,
            source_profile_nk TEXT,
            profile_origin TEXT,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MALF_WAVE_LIFE_PROFILE_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_wave_life_profile (
            profile_nk TEXT PRIMARY KEY,
            timeframe TEXT NOT NULL,
            major_state TEXT NOT NULL,
            reversal_stage TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            sample_version TEXT NOT NULL,
            sample_size BIGINT NOT NULL,
            profile_origin TEXT NOT NULL,
            p10 DOUBLE,
            p25 DOUBLE,
            p50 DOUBLE,
            p75 DOUBLE,
            p90 DOUBLE,
            mean DOUBLE,
            std DOUBLE,
            source_stats_nk TEXT,
            first_seen_run_id TEXT NOT NULL,
            last_materialized_run_id TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
}


MALF_REQUIRED_COLUMNS: Final[dict[str, dict[str, str]]] = {
    MALF_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "run_status": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_instrument_count": "BIGINT",
        "source_price_table": "TEXT",
        "adjust_method": "TEXT",
        "malf_contract_version": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    PAS_CONTEXT_SNAPSHOT_TABLE: {
        "context_nk": "TEXT",
        "entity_code": "TEXT",
        "entity_name": "TEXT",
        "signal_date": "DATE",
        "asof_date": "DATE",
        "source_context_nk": "TEXT",
        "malf_context_4": "TEXT",
        "lifecycle_rank_high": "BIGINT",
        "lifecycle_rank_total": "BIGINT",
        "calc_date": "DATE",
        "adjust_method": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    STRUCTURE_CANDIDATE_SNAPSHOT_TABLE: {
        "candidate_nk": "TEXT",
        "instrument": "TEXT",
        "instrument_name": "TEXT",
        "signal_date": "DATE",
        "asof_date": "DATE",
        "new_high_count": "BIGINT",
        "new_low_count": "BIGINT",
        "refresh_density": "DOUBLE",
        "advancement_density": "DOUBLE",
        "is_failed_extreme": "BOOLEAN",
        "failure_type": "TEXT",
        "adjust_method": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    MALF_RUN_CONTEXT_SNAPSHOT_TABLE: {
        "run_id": "TEXT",
        "context_nk": "TEXT",
        "materialization_action": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
    MALF_RUN_STRUCTURE_SNAPSHOT_TABLE: {
        "run_id": "TEXT",
        "candidate_nk": "TEXT",
        "materialization_action": "TEXT",
        "recorded_at": "TIMESTAMP",
    },
    MALF_MECHANISM_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "run_status": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_instrument_count": "BIGINT",
        "source_context_table": "TEXT",
        "source_structure_input_table": "TEXT",
        "timeframe": "TEXT",
        "stats_sample_version": "TEXT",
        "mechanism_contract_version": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    MALF_MECHANISM_CHECKPOINT_TABLE: {
        "instrument": "TEXT",
        "timeframe": "TEXT",
        "last_signal_date": "DATE",
        "last_asof_date": "DATE",
        "last_run_id": "TEXT",
        "source_context_nk": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    PIVOT_CONFIRMED_BREAK_LEDGER_TABLE: {
        "break_event_nk": "TEXT",
        "instrument": "TEXT",
        "timeframe": "TEXT",
        "guard_pivot_id": "TEXT",
        "guard_pivot_role": "TEXT",
        "origin_context": "TEXT",
        "trigger_bar_dt": "DATE",
        "trigger_price_proxy": "DOUBLE",
        "break_direction": "TEXT",
        "confirmation_status": "TEXT",
        "confirmation_bar_dt": "DATE",
        "confirmation_pivot_id": "TEXT",
        "confirmation_pivot_role": "TEXT",
        "source_context_nk": "TEXT",
        "source_candidate_nk": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    SAME_TIMEFRAME_STATS_PROFILE_TABLE: {
        "stats_profile_nk": "TEXT",
        "universe": "TEXT",
        "timeframe": "TEXT",
        "regime_family": "TEXT",
        "metric_name": "TEXT",
        "sample_version": "TEXT",
        "sample_size": "BIGINT",
        "p10": "DOUBLE",
        "p25": "DOUBLE",
        "p50": "DOUBLE",
        "p75": "DOUBLE",
        "p90": "DOUBLE",
        "mean": "DOUBLE",
        "std": "DOUBLE",
        "bucket_definition_json": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE: {
        "stats_snapshot_nk": "TEXT",
        "instrument": "TEXT",
        "timeframe": "TEXT",
        "signal_date": "DATE",
        "asof_bar_dt": "DATE",
        "regime_family": "TEXT",
        "sample_version": "TEXT",
        "stats_contract_version": "TEXT",
        "source_context_nk": "TEXT",
        "source_candidate_nk": "TEXT",
        "new_high_count_percentile": "DOUBLE",
        "new_low_count_percentile": "DOUBLE",
        "refresh_density_percentile": "DOUBLE",
        "advancement_density_percentile": "DOUBLE",
        "exhaustion_risk_bucket": "TEXT",
        "reversal_probability_bucket": "TEXT",
        "source_profile_refs_json": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    MALF_CANONICAL_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "run_status": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_scope_count": "BIGINT",
        "pivot_confirmation_window": "BIGINT",
        "source_price_table": "TEXT",
        "source_adjust_method": "TEXT",
        "canonical_contract_version": "TEXT",
        "timeframe_list_json": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    MALF_CANONICAL_WORK_QUEUE_TABLE: {
        "queue_nk": "TEXT",
        "scope_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "dirty_reason": "TEXT",
        "source_last_trade_date": "DATE",
        "queue_status": "TEXT",
        "enqueued_at": "TIMESTAMP",
        "claimed_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "first_seen_run_id": "TEXT",
        "last_claimed_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    MALF_CANONICAL_CHECKPOINT_TABLE: {
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "last_completed_bar_dt": "DATE",
        "tail_start_bar_dt": "DATE",
        "tail_confirm_until_dt": "DATE",
        "last_wave_id": "BIGINT",
        "last_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    MALF_PIVOT_LEDGER_TABLE: {
        "pivot_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "pivot_type": "TEXT",
        "pivot_bar_dt": "DATE",
        "confirmed_at": "DATE",
        "pivot_price": "DOUBLE",
        "prior_pivot_nk": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    MALF_WAVE_LEDGER_TABLE: {
        "wave_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "wave_id": "BIGINT",
        "direction": "TEXT",
        "major_state": "TEXT",
        "reversal_stage": "TEXT",
        "start_bar_dt": "DATE",
        "end_bar_dt": "DATE",
        "active_flag": "BOOLEAN",
        "start_pivot_nk": "TEXT",
        "end_pivot_nk": "TEXT",
        "hh_count": "BIGINT",
        "ll_count": "BIGINT",
        "bar_count": "BIGINT",
        "wave_high": "DOUBLE",
        "wave_low": "DOUBLE",
        "range_ratio": "DOUBLE",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    MALF_EXTREME_PROGRESS_LEDGER_TABLE: {
        "extreme_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "wave_id": "BIGINT",
        "extreme_seq": "BIGINT",
        "extreme_type": "TEXT",
        "break_base_extreme_nk": "TEXT",
        "record_bar_dt": "DATE",
        "record_price": "DOUBLE",
        "cumulative_count": "BIGINT",
        "major_state": "TEXT",
        "trend_direction": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    MALF_STATE_SNAPSHOT_TABLE: {
        "snapshot_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "asof_bar_dt": "DATE",
        "major_state": "TEXT",
        "trend_direction": "TEXT",
        "reversal_stage": "TEXT",
        "wave_id": "BIGINT",
        "last_confirmed_h_bar_dt": "DATE",
        "last_confirmed_h_price": "DOUBLE",
        "last_confirmed_l_bar_dt": "DATE",
        "last_confirmed_l_price": "DOUBLE",
        "last_valid_hl_bar_dt": "DATE",
        "last_valid_hl_price": "DOUBLE",
        "last_valid_lh_bar_dt": "DATE",
        "last_valid_lh_price": "DOUBLE",
        "current_hh_count": "BIGINT",
        "current_ll_count": "BIGINT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    MALF_SAME_LEVEL_STATS_TABLE: {
        "stats_nk": "TEXT",
        "universe": "TEXT",
        "timeframe": "TEXT",
        "major_state": "TEXT",
        "metric_name": "TEXT",
        "sample_version": "TEXT",
        "sample_size": "BIGINT",
        "p10": "DOUBLE",
        "p25": "DOUBLE",
        "p50": "DOUBLE",
        "p75": "DOUBLE",
        "p90": "DOUBLE",
        "mean": "DOUBLE",
        "std": "DOUBLE",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    MALF_WAVE_LIFE_RUN_TABLE: {
        "run_id": "TEXT",
        "runner_name": "TEXT",
        "runner_version": "TEXT",
        "run_status": "TEXT",
        "execution_mode": "TEXT",
        "signal_start_date": "DATE",
        "signal_end_date": "DATE",
        "bounded_scope_count": "BIGINT",
        "claimed_scope_count": "BIGINT",
        "source_wave_table": "TEXT",
        "source_state_table": "TEXT",
        "source_stats_table": "TEXT",
        "sample_version": "TEXT",
        "life_contract_version": "TEXT",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "summary_json": "TEXT",
    },
    MALF_WAVE_LIFE_WORK_QUEUE_TABLE: {
        "queue_nk": "TEXT",
        "scope_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "dirty_reason": "TEXT",
        "replay_start_bar_dt": "DATE",
        "replay_confirm_until_dt": "DATE",
        "source_fingerprint": "TEXT",
        "sample_version": "TEXT",
        "queue_status": "TEXT",
        "enqueued_at": "TIMESTAMP",
        "claimed_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "first_seen_run_id": "TEXT",
        "last_claimed_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    MALF_WAVE_LIFE_CHECKPOINT_TABLE: {
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "last_completed_bar_dt": "DATE",
        "tail_start_bar_dt": "DATE",
        "tail_confirm_until_dt": "DATE",
        "last_sample_version": "TEXT",
        "source_fingerprint": "TEXT",
        "last_run_id": "TEXT",
        "updated_at": "TIMESTAMP",
    },
    MALF_WAVE_LIFE_SNAPSHOT_TABLE: {
        "snapshot_nk": "TEXT",
        "asset_type": "TEXT",
        "code": "TEXT",
        "timeframe": "TEXT",
        "asof_bar_dt": "DATE",
        "wave_id": "BIGINT",
        "source_wave_nk": "TEXT",
        "source_state_snapshot_nk": "TEXT",
        "major_state": "TEXT",
        "reversal_stage": "TEXT",
        "active_wave_bar_age": "BIGINT",
        "wave_life_percentile": "DOUBLE",
        "remaining_life_bars_p50": "DOUBLE",
        "remaining_life_bars_p75": "DOUBLE",
        "termination_risk_bucket": "TEXT",
        "sample_size": "BIGINT",
        "sample_version": "TEXT",
        "source_profile_nk": "TEXT",
        "profile_origin": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    MALF_WAVE_LIFE_PROFILE_TABLE: {
        "profile_nk": "TEXT",
        "timeframe": "TEXT",
        "major_state": "TEXT",
        "reversal_stage": "TEXT",
        "metric_name": "TEXT",
        "sample_version": "TEXT",
        "sample_size": "BIGINT",
        "profile_origin": "TEXT",
        "p10": "DOUBLE",
        "p25": "DOUBLE",
        "p50": "DOUBLE",
        "p75": "DOUBLE",
        "p90": "DOUBLE",
        "mean": "DOUBLE",
        "std": "DOUBLE",
        "source_stats_nk": "TEXT",
        "first_seen_run_id": "TEXT",
        "last_materialized_run_id": "TEXT",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
}


def malf_ledger_path(settings: WorkspaceRoots | None = None) -> Path:
    """返回正式 `malf` 历史账本路径。"""

    workspace = settings or default_settings()
    return workspace.databases.malf


def connect_malf_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `malf` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(str(malf_ledger_path(workspace)), read_only=read_only)


def bootstrap_malf_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> Path:
    """创建或补齐 `malf` 最小正式表族。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    owns_connection = connection is None
    conn = connection or connect_malf_ledger(workspace)
    try:
        for ddl in MALF_LEDGER_TABLES.values():
            conn.execute(ddl)
        for table_name, required_columns in MALF_REQUIRED_COLUMNS.items():
            _ensure_columns(conn, table_name=table_name, required_columns=required_columns)
        return malf_ledger_path(workspace)
    finally:
        if owns_connection:
            conn.close()


def _ensure_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    required_columns: dict[str, str],
) -> None:
    existing_rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    existing_columns = {str(row[1]) for row in existing_rows}
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
