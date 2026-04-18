"""冻结 `malf` 模块最小正式快照表族。"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.malf.bootstrap_columns import MALF_REQUIRED_COLUMNS
from mlq.malf.bootstrap_tables import (
    MALF_CANONICAL_CHECKPOINT_TABLE,
    MALF_CANONICAL_RUN_TABLE,
    MALF_CANONICAL_WORK_QUEUE_TABLE,
    MALF_EXTREME_PROGRESS_LEDGER_TABLE,
    MALF_LEDGER_CONTRACT_TABLE,
    MALF_MECHANISM_CHECKPOINT_TABLE,
    MALF_MECHANISM_RUN_TABLE,
    MALF_PIVOT_LEDGER_TABLE,
    MALF_RUN_CONTEXT_SNAPSHOT_TABLE,
    MALF_RUN_STRUCTURE_SNAPSHOT_TABLE,
    MALF_RUN_TABLE,
    MALF_SAME_LEVEL_STATS_TABLE,
    MALF_STATE_SNAPSHOT_TABLE,
    MALF_WAVE_LEDGER_TABLE,
    MALF_WAVE_LIFE_CHECKPOINT_TABLE,
    MALF_WAVE_LIFE_PROFILE_TABLE,
    MALF_WAVE_LIFE_RUN_TABLE,
    MALF_WAVE_LIFE_SNAPSHOT_TABLE,
    MALF_WAVE_LIFE_WORK_QUEUE_TABLE,
    PAS_CONTEXT_SNAPSHOT_TABLE,
    PIVOT_CONFIRMED_BREAK_LEDGER_TABLE,
    SAME_TIMEFRAME_STATS_PROFILE_TABLE,
    SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE,
    STRUCTURE_CANDIDATE_SNAPSHOT_TABLE,
)


MALF_NATIVE_TIMEFRAME_MAP: Final[dict[str, str]] = {
    "D": "malf_day",
    "W": "malf_week",
    "M": "malf_month",
}
MALF_LEDGER_CONTRACT_VERSION: Final[str] = "malf-ledger-path-v1"
MALF_LEDGER_STORAGE_MODE_NATIVE: Final[str] = "official_native"
MALF_LEDGER_STORAGE_MODE_LEGACY: Final[str] = "legacy_compat"


MALF_LEDGER_TABLES: Final[dict[str, str]] = {
    MALF_LEDGER_CONTRACT_TABLE: """
        CREATE TABLE IF NOT EXISTS malf_ledger_contract (
            contract_key TEXT PRIMARY KEY,
            storage_mode TEXT NOT NULL,
            native_timeframe TEXT,
            contract_version TEXT NOT NULL,
            declared_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
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


def malf_ledger_path(
    settings: WorkspaceRoots | None = None,
    *,
    timeframe: str | None = None,
    use_legacy: bool = False,
) -> Path:
    """返回正式 `malf` 历史账本路径。"""

    workspace = settings or default_settings()
    if use_legacy:
        if timeframe is not None:
            raise ValueError("Legacy malf ledger path cannot be combined with a native timeframe.")
        return workspace.databases.malf_legacy
    if timeframe is None:
        return workspace.databases.malf_legacy
    normalized_timeframe = _normalize_native_timeframe(timeframe)
    return getattr(workspace.databases, MALF_NATIVE_TIMEFRAME_MAP[normalized_timeframe])


def connect_malf_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
    timeframe: str | None = None,
    use_legacy: bool = False,
) -> duckdb.DuckDBPyConnection:
    """连接正式 `malf` 历史账本。"""

    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
    return duckdb.connect(
        str(malf_ledger_path(workspace, timeframe=timeframe, use_legacy=use_legacy)),
        read_only=read_only,
    )


def bootstrap_malf_ledger(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
    timeframe: str | None = None,
    use_legacy: bool = False,
) -> Path:
    """创建或补齐 `malf` 最小正式表族。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    native_timeframe = None if use_legacy or timeframe is None else _normalize_native_timeframe(timeframe)
    owns_connection = connection is None
    ledger_path = malf_ledger_path(workspace, timeframe=native_timeframe, use_legacy=use_legacy)
    conn = connection or connect_malf_ledger(
        workspace,
        timeframe=native_timeframe,
        use_legacy=use_legacy,
    )
    try:
        for ddl in _render_malf_ledger_ddls(native_timeframe=native_timeframe):
            conn.execute(ddl)
        for table_name, required_columns in MALF_REQUIRED_COLUMNS.items():
            _ensure_columns(conn, table_name=table_name, required_columns=required_columns)
        _upsert_ledger_contract(
            conn,
            storage_mode=(
                MALF_LEDGER_STORAGE_MODE_LEGACY
                if native_timeframe is None
                else MALF_LEDGER_STORAGE_MODE_NATIVE
            ),
            native_timeframe=native_timeframe,
        )
        if native_timeframe is not None:
            _validate_native_timeframe_rows(conn, native_timeframe=native_timeframe)
        return ledger_path
    finally:
        if owns_connection:
            conn.close()


def _normalize_native_timeframe(timeframe: str | None) -> str:
    if timeframe is None:
        raise ValueError("Native malf ledger requires an explicit timeframe in {'D', 'W', 'M'}.")
    normalized_timeframe = str(timeframe).strip().upper()
    if normalized_timeframe not in MALF_NATIVE_TIMEFRAME_MAP:
        raise ValueError(f"Unsupported malf native timeframe: {timeframe}")
    return normalized_timeframe


def _render_malf_ledger_ddls(*, native_timeframe: str | None) -> tuple[str, ...]:
    timeframe_sql = "timeframe TEXT NOT NULL"
    if native_timeframe is not None:
        timeframe_sql = f"timeframe TEXT NOT NULL CHECK (timeframe = '{native_timeframe}')"
    return tuple(
        ddl.replace("timeframe TEXT NOT NULL", timeframe_sql) for ddl in MALF_LEDGER_TABLES.values()
    )


def _upsert_ledger_contract(
    connection: duckdb.DuckDBPyConnection,
    *,
    storage_mode: str,
    native_timeframe: str | None,
) -> None:
    existing_row = connection.execute(
        f"""
        SELECT storage_mode, native_timeframe, contract_version
        FROM {MALF_LEDGER_CONTRACT_TABLE}
        WHERE contract_key = 'malf'
        """
    ).fetchone()
    expected_row = (storage_mode, native_timeframe, MALF_LEDGER_CONTRACT_VERSION)
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {MALF_LEDGER_CONTRACT_TABLE} (
                contract_key,
                storage_mode,
                native_timeframe,
                contract_version
            )
            VALUES ('malf', ?, ?, ?)
            """,
            [storage_mode, native_timeframe, MALF_LEDGER_CONTRACT_VERSION],
        )
        return
    if tuple(existing_row) != expected_row:
        raise ValueError("Existing malf ledger contract does not match the requested bootstrap mode.")
    connection.execute(
        f"""
        UPDATE {MALF_LEDGER_CONTRACT_TABLE}
        SET updated_at = CURRENT_TIMESTAMP
        WHERE contract_key = 'malf'
        """
    )


def _validate_native_timeframe_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    native_timeframe: str,
) -> None:
    timeframe_tables = [
        table_name
        for table_name, required_columns in MALF_REQUIRED_COLUMNS.items()
        if "timeframe" in required_columns and table_name != MALF_LEDGER_CONTRACT_TABLE
    ]
    for table_name in timeframe_tables:
        mismatch_row = connection.execute(
            f"""
            SELECT timeframe
            FROM {table_name}
            WHERE timeframe <> ?
            LIMIT 1
            """,
            [native_timeframe],
        ).fetchone()
        if mismatch_row is not None:
            raise ValueError(
                f"{table_name} contains timeframe={mismatch_row[0]!r}, expected only {native_timeframe!r}."
            )


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
