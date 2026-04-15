"""覆盖 `position` 官方 formal signal runner 与 reference price enrichment。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.position import (
    POSITION_CHECKPOINT_TABLE,
    POSITION_RUN_SNAPSHOT_TABLE,
    POSITION_RUN_TABLE,
    POSITION_WORK_QUEUE_TABLE,
    position_ledger_path,
    run_position_formal_signal_materialization,
)


def _clear_workspace_env(monkeypatch) -> None:
    for env_name in (
        "LIFESPAN_REPO_ROOT",
        "LIFESPAN_DATA_ROOT",
        "LIFESPAN_TEMP_ROOT",
        "LIFESPAN_REPORT_ROOT",
        "LIFESPAN_VALIDATED_ROOT",
    ):
        monkeypatch.delenv(env_name, raising=False)


def _bootstrap_repo_root(tmp_path: Path) -> Path:
    repo_root = tmp_path / "lifespan-0.01"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text("[project]\nname='lifespan-0.01'\n", encoding="utf-8")
    return repo_root


def _seed_alpha_formal_signal_table(alpha_path: Path, rows: list[tuple[object, ...]]) -> None:
    alpha_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(alpha_path))
    try:
        conn.execute(
            """
            CREATE TABLE alpha_formal_signal_event (
                signal_nk TEXT NOT NULL,
                instrument TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_date DATE NOT NULL,
                trigger_family TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                pattern_code TEXT NOT NULL,
                formal_signal_status TEXT NOT NULL,
                trigger_admissible BOOLEAN NOT NULL,
                malf_context_4 TEXT NOT NULL,
                lifecycle_rank_high BIGINT NOT NULL,
                lifecycle_rank_total BIGINT NOT NULL,
                source_trigger_event_nk TEXT NOT NULL,
                signal_contract_version TEXT NOT NULL,
                source_family_event_nk TEXT,
                source_family_contract_version TEXT,
                family_code TEXT,
                family_role TEXT,
                family_bias TEXT,
                malf_alignment TEXT,
                malf_phase_bucket TEXT,
                family_source_context_fingerprint TEXT,
                last_materialized_run_id TEXT
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO alpha_formal_signal_event (
                    signal_nk,
                    instrument,
                    signal_date,
                    asof_date,
                    trigger_family,
                    trigger_type,
                    pattern_code,
                    formal_signal_status,
                    trigger_admissible,
                    malf_context_4,
                    lifecycle_rank_high,
                    lifecycle_rank_total,
                    source_trigger_event_nk,
                    signal_contract_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
    finally:
        conn.close()


def _seed_market_base_prices(market_base_path: Path, rows: list[tuple[object, ...]]) -> None:
    market_base_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(market_base_path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                code TEXT NOT NULL,
                trade_date DATE NOT NULL,
                adjust_method TEXT NOT NULL,
                close DOUBLE
            )
            """
        )
        for row in rows:
            conn.execute("INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, ?)", row)
    finally:
        conn.close()


def test_run_position_formal_signal_materialization_reads_official_alpha_and_enriches_reference_price(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_alpha_formal_signal_table(
        settings.databases.alpha,
        [
            (
                "sig-101",
                "000001.SZ",
                "2026-04-08",
                "2026-04-08",
                "PAS",
                "bof",
                "BOF",
                "admitted",
                True,
                "BULL_MAINSTREAM",
                1,
                4,
                "evt-101",
                "alpha-formal-signal-v1",
            )
        ],
    )
    _seed_market_base_prices(
        settings.databases.market_base,
        [
            ("000001.SZ", "2026-04-08", "none", 10.1),
            ("000001.SZ", "2026-04-09", "none", 10.5),
        ],
    )

    summary = run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        run_id="position-runner-test-001",
    )

    assert summary.position_run_id == "position-runner-test-001"
    assert summary.execution_mode == "bounded"
    assert summary.alpha_signal_count == 1
    assert summary.enriched_signal_count == 1
    assert summary.missing_reference_price_count == 0
    assert summary.candidate_count == 1
    assert summary.inserted_count == 1
    assert summary.reused_count == 0
    assert summary.rematerialized_count == 0
    assert summary.risk_budget_count == 1
    assert summary.entry_leg_count == 3
    assert summary.exit_plan_count == 0
    assert summary.exit_leg_count == 0

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        candidate_row = conn.execute(
            """
            SELECT candidate_status, context_behavior_profile, deployment_stage
            FROM position_candidate_audit
            WHERE candidate_nk = 'sig-101|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        sizing_row = conn.execute(
            """
            SELECT reference_trade_date, reference_price, position_action_decision, target_shares,
                   schedule_stage, entry_leg_count
            FROM position_sizing_snapshot
            WHERE candidate_nk = 'sig-101|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        entry_leg_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM position_entry_leg_plan
            WHERE candidate_nk = 'sig-101|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()[0]
    finally:
        conn.close()

    assert candidate_row == ("admitted", "trend_following_expansion", "initial_entry_window")
    assert sizing_row == (date(2026, 4, 9), 10.5, "open_up_to_context_cap", 17800, "t+1", 3)
    assert entry_leg_count == 3


def test_run_position_formal_signal_materialization_accepts_family_aware_alpha_columns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_alpha_formal_signal_table(
        settings.databases.alpha,
        [
            (
                "sig-111",
                "000001.SZ",
                "2026-04-08",
                "2026-04-08",
                "PAS",
                "bof",
                "BOF",
                "admitted",
                True,
                "BULL_MAINSTREAM",
                1,
                4,
                "evt-111",
                "alpha-formal-signal-v4",
            )
        ],
    )
    conn = duckdb.connect(str(settings.databases.alpha))
    try:
        conn.execute(
            """
            UPDATE alpha_formal_signal_event
            SET
                source_family_event_nk = 'evt-111|alpha-family-v2',
                source_family_contract_version = 'alpha-family-v2',
                family_code = 'bof_core',
                family_role = 'mainline',
                family_bias = 'reversal_attempt',
                malf_alignment = 'cautious',
                malf_phase_bucket = 'early',
                family_source_context_fingerprint = 'family-fingerprint-111',
                last_materialized_run_id = 'alpha-formal-run-111'
            WHERE signal_nk = 'sig-111'
            """
        )
    finally:
        conn.close()
    _seed_market_base_prices(
        settings.databases.market_base,
        [("000001.SZ", "2026-04-09", "none", 10.5)],
    )

    summary = run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        run_id="position-runner-test-family-aware-001",
    )

    assert summary.position_run_id == "position-runner-test-family-aware-001"
    assert summary.execution_mode == "bounded"
    assert summary.alpha_signal_count == 1
    assert summary.enriched_signal_count == 1
    assert summary.risk_budget_count == 1
    assert summary.entry_leg_count == 3

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        candidate_row = conn.execute(
            """
            SELECT candidate_status, source_signal_run_id, candidate_contract_version
            FROM position_candidate_audit
            WHERE candidate_nk = 'sig-111|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
    finally:
        conn.close()

    assert candidate_row == (
        "admitted",
        "alpha-formal-run-111",
        "position-malf-batched-entry-exit-v2",
    )


def test_run_position_formal_signal_materialization_skips_signals_without_reference_price(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_alpha_formal_signal_table(
        settings.databases.alpha,
        [
            (
                "sig-201",
                "000001.SZ",
                "2026-04-08",
                "2026-04-08",
                "PAS",
                "bof",
                "BOF",
                "admitted",
                True,
                "BULL_MAINSTREAM",
                1,
                4,
                "evt-201",
                "alpha-formal-signal-v1",
            ),
            (
                "sig-202",
                "000002.SZ",
                "2026-04-08",
                "2026-04-08",
                "PAS",
                "pb",
                "PB",
                "blocked",
                False,
                "BEAR_MAINSTREAM",
                0,
                4,
                "evt-202",
                "alpha-formal-signal-v1",
            ),
        ],
    )
    _seed_market_base_prices(
        settings.databases.market_base,
        [
            ("000001.SZ", "2026-04-09", "none", 10.5),
        ],
    )

    summary = run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        run_id="position-runner-test-002",
    )

    assert summary.alpha_signal_count == 2
    assert summary.enriched_signal_count == 1
    assert summary.missing_reference_price_count == 1
    assert summary.candidate_count == 1
    assert summary.risk_budget_count == 1

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        candidate_count = conn.execute(
            "SELECT COUNT(*) FROM position_candidate_audit"
        ).fetchone()[0]
    finally:
        conn.close()

    assert candidate_count == 1


def test_run_position_formal_signal_materialization_accepts_legacy_alpha_column_names(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    alpha_path = settings.databases.alpha
    alpha_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(alpha_path))
    try:
        conn.execute(
            """
            CREATE TABLE alpha_formal_signal_event (
                signal_id TEXT NOT NULL,
                code TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_date DATE NOT NULL,
                trigger_type TEXT NOT NULL,
                pattern TEXT NOT NULL,
                admission_status TEXT NOT NULL,
                filter_trigger_admissible BOOLEAN NOT NULL,
                malf_context_4 TEXT NOT NULL,
                lifecycle_rank_high BIGINT NOT NULL,
                lifecycle_rank_total BIGINT NOT NULL,
                source_pas_signal_id TEXT NOT NULL,
                source_pas_contract_version TEXT NOT NULL,
                last_materialized_run_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO alpha_formal_signal_event VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "sig-301",
                "000003.SZ",
                "2026-04-08",
                "2026-04-08",
                "pb",
                "PB",
                "ADMITTED",
                True,
                "BULL_COUNTERTREND",
                2,
                4,
                "evt-301",
                "legacy-alpha-v1",
                "alpha-run-legacy-001",
            ],
        )
    finally:
        conn.close()

    _seed_market_base_prices(
        settings.databases.market_base,
        [
            ("000003.SZ", "2026-04-09", "none", 20.0),
        ],
    )

    summary = run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        limit=10,
        run_id="position-runner-test-003",
    )

    assert summary.position_run_id == "position-runner-test-003"
    assert summary.execution_mode == "checkpoint_queue"
    assert summary.alpha_signal_count == 1
    assert summary.enriched_signal_count == 1
    assert summary.risk_budget_count == 1

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        candidate_row = conn.execute(
            """
            SELECT signal_nk, instrument, source_signal_run_id
            FROM position_candidate_audit
            WHERE candidate_nk = 'sig-301|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
    finally:
        conn.close()

    assert candidate_row == ("sig-301", "000003.SZ", "alpha-run-legacy-001")


def test_run_position_formal_signal_materialization_checkpoint_queue_bootstraps_queue_and_checkpoint(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_alpha_formal_signal_table(
        settings.databases.alpha,
        [
            (
                "sig-401",
                "000001.SZ",
                "2026-04-08",
                "2026-04-08",
                "PAS",
                "bof",
                "BOF",
                "admitted",
                True,
                "BULL_MAINSTREAM",
                1,
                4,
                "evt-401",
                "alpha-formal-signal-v1",
            )
        ],
    )
    _seed_market_base_prices(
        settings.databases.market_base,
        [("000001.SZ", "2026-04-09", "none", 10.5)],
    )

    summary = run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        limit=10,
        run_id="position-runner-queue-001",
    )

    assert summary.execution_mode == "checkpoint_queue"
    assert summary.inserted_count == 1
    assert summary.queue_enqueued_count == 1
    assert summary.queue_claimed_count == 1
    assert summary.checkpoint_upserted_count == 1

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        queue_row = conn.execute(
            f"""
            SELECT queue_status, queue_reason
            FROM {POSITION_WORK_QUEUE_TABLE}
            WHERE signal_nk = 'sig-401'
            """
        ).fetchone()
        checkpoint_row = conn.execute(
            f"""
            SELECT checkpoint_scope, last_signal_nk
            FROM {POSITION_CHECKPOINT_TABLE}
            WHERE candidate_nk = 'sig-401|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        run_snapshot_row = conn.execute(
            f"""
            SELECT materialization_action, candidate_status
            FROM {POSITION_RUN_SNAPSHOT_TABLE}
            WHERE run_id = 'position-runner-queue-001'
              AND candidate_nk = 'sig-401|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        run_row = conn.execute(
            f"""
            SELECT execution_mode, inserted_count, queue_enqueued_count, checkpoint_upserted_count
            FROM {POSITION_RUN_TABLE}
            WHERE run_id = 'position-runner-queue-001'
            """
        ).fetchone()
    finally:
        conn.close()

    assert queue_row == ("completed", "bootstrap_missing_checkpoint")
    assert checkpoint_row == ("fixed_notional_full_exit_v1", "sig-401")
    assert run_snapshot_row == ("inserted", "admitted")
    assert run_row == ("checkpoint_queue", 1, 1, 1)


def test_run_position_formal_signal_materialization_reuses_unchanged_candidate_on_bounded_replay(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_alpha_formal_signal_table(
        settings.databases.alpha,
        [
            (
                "sig-402",
                "000001.SZ",
                "2026-04-08",
                "2026-04-08",
                "PAS",
                "bof",
                "BOF",
                "admitted",
                True,
                "BULL_MAINSTREAM",
                1,
                4,
                "evt-402",
                "alpha-formal-signal-v1",
            )
        ],
    )
    _seed_market_base_prices(
        settings.databases.market_base,
        [("000001.SZ", "2026-04-09", "none", 10.5)],
    )

    first_summary = run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        run_id="position-runner-bounded-001",
    )
    second_summary = run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        run_id="position-runner-bounded-002",
    )

    assert first_summary.inserted_count == 1
    assert second_summary.reused_count == 1
    assert second_summary.inserted_count == 0
    assert second_summary.rematerialized_count == 0


def test_run_position_formal_signal_materialization_rematerializes_when_reference_price_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_alpha_formal_signal_table(
        settings.databases.alpha,
        [
            (
                "sig-403",
                "000001.SZ",
                "2026-04-08",
                "2026-04-08",
                "PAS",
                "bof",
                "BOF",
                "admitted",
                True,
                "BULL_MAINSTREAM",
                1,
                4,
                "evt-403",
                "alpha-formal-signal-v1",
            )
        ],
    )
    _seed_market_base_prices(
        settings.databases.market_base,
        [("000001.SZ", "2026-04-09", "none", 10.5)],
    )

    run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        run_id="position-runner-remat-001",
    )

    conn = duckdb.connect(str(settings.databases.market_base))
    try:
        conn.execute(
            """
            UPDATE stock_daily_adjusted
            SET close = 11.0
            WHERE code = '000001.SZ'
              AND trade_date = '2026-04-09'
              AND adjust_method = 'none'
            """
        )
    finally:
        conn.close()

    summary = run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        run_id="position-runner-remat-002",
    )

    assert summary.rematerialized_count == 1
    assert summary.reused_count == 0

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        sizing_row = conn.execute(
            """
            SELECT reference_price, target_shares
            FROM position_sizing_snapshot
            WHERE candidate_nk = 'sig-403|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        run_snapshot_row = conn.execute(
            f"""
            SELECT materialization_action
            FROM {POSITION_RUN_SNAPSHOT_TABLE}
            WHERE run_id = 'position-runner-remat-002'
              AND candidate_nk = 'sig-403|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
    finally:
        conn.close()

    assert sizing_row == (11.0, 17000)
    assert run_snapshot_row == ("rematerialized",)
