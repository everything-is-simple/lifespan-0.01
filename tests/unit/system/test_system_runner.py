"""覆盖 `system` 主链 bounded acceptance readout / audit bootstrap。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.alpha import run_alpha_formal_signal_build, run_alpha_trigger_build
from mlq.core.paths import default_settings
from mlq.filter import run_filter_snapshot_build
from mlq.portfolio_plan import run_portfolio_plan_build
from mlq.position import run_position_formal_signal_materialization
from mlq.structure import run_structure_snapshot_build
from mlq.system import run_system_mainline_readout_build, system_ledger_path
from mlq.trade import run_trade_runtime_build


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


def _seed_malf_sources(settings) -> None:
    malf_path = settings.databases.malf
    malf_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute(
            """
            CREATE TABLE pas_context_snapshot (
                entity_code TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_date DATE NOT NULL,
                source_context_nk TEXT NOT NULL,
                malf_context_4 TEXT NOT NULL,
                lifecycle_rank_high BIGINT NOT NULL,
                lifecycle_rank_total BIGINT NOT NULL,
                calc_date DATE NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE structure_candidate_snapshot (
                instrument TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_date DATE NOT NULL,
                new_high_count BIGINT NOT NULL,
                new_low_count BIGINT NOT NULL,
                refresh_density DOUBLE NOT NULL,
                advancement_density DOUBLE NOT NULL,
                is_failed_extreme BOOLEAN NOT NULL,
                failure_type TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE pivot_confirmed_break_ledger (
                break_event_nk TEXT NOT NULL,
                instrument TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                trigger_bar_dt DATE NOT NULL,
                confirmation_status TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE same_timeframe_stats_snapshot (
                stats_snapshot_nk TEXT NOT NULL,
                instrument TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_bar_dt DATE NOT NULL,
                exhaustion_risk_bucket TEXT,
                reversal_probability_bucket TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO pas_context_snapshot VALUES
            ('000001.SZ', '2026-04-08', '2026-04-08', 'ctx-001', 'BULL_MAINSTREAM', 1, 4, '2026-04-08')
            """
        )
        conn.execute(
            """
            INSERT INTO structure_candidate_snapshot VALUES
            ('000001.SZ', '2026-04-08', '2026-04-08', 2, 0, 0.8, 0.7, FALSE, NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO pivot_confirmed_break_ledger VALUES
            ('break-001', '000001.SZ', 'D', '2026-04-08', 'confirmed')
            """
        )
        conn.execute(
            """
            INSERT INTO same_timeframe_stats_snapshot VALUES
            ('stats-001', '000001.SZ', '2026-04-08', '2026-04-08', 'high', 'elevated')
            """
        )
    finally:
        conn.close()


def _seed_alpha_trigger_candidates(settings) -> None:
    alpha_path = settings.databases.alpha
    alpha_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(alpha_path))
    try:
        conn.execute(
            """
            CREATE TABLE alpha_trigger_candidate (
                instrument TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_date DATE NOT NULL,
                trigger_family TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                pattern_code TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO alpha_trigger_candidate VALUES
            ('000001.SZ', '2026-04-08', '2026-04-08', 'PAS', 'bof', 'BOF')
            """
        )
    finally:
        conn.close()


def _seed_market_base_prices(settings) -> None:
    market_base_path = settings.databases.market_base
    market_base_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(market_base_path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                code TEXT NOT NULL,
                trade_date DATE NOT NULL,
                adjust_method TEXT NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stock_daily_adjusted VALUES
            ('000001.SZ', '2026-04-09', 'none', 10.4, 10.8, 10.1, 10.6),
            ('000001.SZ', '2026-04-09', 'backward', 88.1, 88.9, 87.7, 88.5),
            ('000001.SZ', '2026-04-10', 'backward', 89.0, 89.8, 88.4, 89.4),
            ('000001.SZ', '2026-04-11', 'none', 10.7, 11.1, 10.5, 10.9)
            """
        )
    finally:
        conn.close()


def _prepare_mainline(settings) -> None:
    settings.ensure_directories()
    _seed_malf_sources(settings)
    _seed_alpha_trigger_candidates(settings)
    _seed_market_base_prices(settings)

    run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-mainline-system-001",
    )
    run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="filter-mainline-system-001",
    )
    run_alpha_trigger_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-trigger-mainline-system-001",
    )
    run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-formal-mainline-system-001",
    )
    run_position_formal_signal_materialization(
        settings=settings,
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="position-mainline-system-001",
    )
    run_portfolio_plan_build(
        settings=settings,
        portfolio_id="main_book",
        portfolio_gross_cap_weight=0.20,
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="portfolio-mainline-system-001",
    )
    run_trade_runtime_build(
        settings=settings,
        portfolio_id="main_book",
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="trade-mainline-system-001",
    )


def test_run_system_mainline_readout_build_materializes_and_reuses_readout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _prepare_mainline(settings)

    first_summary = run_system_mainline_readout_build(
        settings=settings,
        portfolio_id="main_book",
        snapshot_date="2026-04-09",
        run_id="system-mainline-test-001a",
    )
    second_summary = run_system_mainline_readout_build(
        settings=settings,
        portfolio_id="main_book",
        snapshot_date="2026-04-09",
        run_id="system-mainline-test-001b",
    )

    assert first_summary.system_materialization_action == "inserted"
    assert first_summary.child_readout_inserted_count == 7
    assert first_summary.snapshot_inserted_count == 1
    assert first_summary.acceptance_status == "planned_entry_ready"
    assert first_summary.planned_entry_count == 1
    assert first_summary.blocked_upstream_count == 0
    assert first_summary.planned_carry_count == 0
    assert first_summary.carried_open_leg_count == 1
    assert first_summary.current_carry_weight == 0.1875

    assert second_summary.system_materialization_action == "reused"
    assert second_summary.child_readout_reused_count == 7
    assert second_summary.snapshot_reused_count == 1

    conn = duckdb.connect(str(system_ledger_path(settings)), read_only=True)
    try:
        run_rows = conn.execute(
            """
            SELECT run_id, run_status, system_materialization_action, bounded_child_run_count,
                   planned_entry_count, carried_open_leg_count
            FROM system_run
            ORDER BY run_id
            """
        ).fetchall()
        child_rows = conn.execute(
            """
            SELECT child_module, child_run_id, last_materialized_run_id
            FROM system_child_run_readout
            ORDER BY child_module
            """
        ).fetchall()
        snapshot_row = conn.execute(
            """
            SELECT snapshot_date, acceptance_status, planned_entry_count, carried_open_leg_count,
                   current_carry_weight, source_trade_run_id
            FROM system_mainline_snapshot
            WHERE portfolio_id = 'main_book'
            """
        ).fetchone()
        run_snapshot_actions = conn.execute(
            """
            SELECT run_id, materialization_action
            FROM system_run_snapshot
            ORDER BY run_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_rows == [
        ("system-mainline-test-001a", "completed", "inserted", 7, 1, 1),
        ("system-mainline-test-001b", "completed", "reused", 7, 1, 1),
    ]
    assert len(child_rows) == 7
    assert snapshot_row == (
        date(2026, 4, 9),
        "planned_entry_ready",
        1,
        1,
        0.1875,
        "trade-mainline-system-001",
    )
    assert run_snapshot_actions == [
        ("system-mainline-test-001a", "inserted"),
        ("system-mainline-test-001b", "reused"),
    ]


def test_run_system_mainline_readout_build_marks_rematerialized_when_trade_run_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _prepare_mainline(settings)

    first_summary = run_system_mainline_readout_build(
        settings=settings,
        portfolio_id="main_book",
        snapshot_date="2026-04-09",
        run_id="system-mainline-test-002a",
    )
    assert first_summary.system_materialization_action == "inserted"

    run_trade_runtime_build(
        settings=settings,
        portfolio_id="main_book",
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="trade-mainline-system-002",
    )

    second_summary = run_system_mainline_readout_build(
        settings=settings,
        portfolio_id="main_book",
        snapshot_date="2026-04-09",
        run_id="system-mainline-test-002b",
    )

    assert second_summary.system_materialization_action == "rematerialized"
    assert second_summary.child_readout_inserted_count == 1
    assert second_summary.snapshot_rematerialized_count == 1

    conn = duckdb.connect(str(system_ledger_path(settings)), read_only=True)
    try:
        trade_child_rows = conn.execute(
            """
            SELECT child_run_id, last_materialized_run_id
            FROM system_child_run_readout
            WHERE child_module = 'trade'
            ORDER BY child_run_id
            """
        ).fetchall()
        snapshot_row = conn.execute(
            """
            SELECT source_trade_run_id, last_materialized_run_id
            FROM system_mainline_snapshot
            WHERE portfolio_id = 'main_book'
            """
        ).fetchone()
        run_snapshot_actions = conn.execute(
            """
            SELECT run_id, materialization_action
            FROM system_run_snapshot
            ORDER BY run_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert trade_child_rows == [
        ("trade-mainline-system-001", "system-mainline-test-002a"),
        ("trade-mainline-system-002", "system-mainline-test-002b"),
    ]
    assert snapshot_row == ("trade-mainline-system-002", "system-mainline-test-002b")
    assert run_snapshot_actions == [
        ("system-mainline-test-002a", "inserted"),
        ("system-mainline-test-002b", "rematerialized"),
    ]
