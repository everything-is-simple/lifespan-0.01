"""覆盖 26 号卡要求的整链 truthfulness 复核。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.alpha import alpha_ledger_path, run_alpha_formal_signal_build, run_alpha_trigger_build
from mlq.core.paths import default_settings
from mlq.filter import filter_ledger_path, run_filter_snapshot_build
from mlq.portfolio_plan import portfolio_plan_ledger_path, run_portfolio_plan_build
from mlq.position import position_ledger_path, run_position_formal_signal_materialization
from mlq.structure import run_structure_snapshot_build, structure_ledger_path
from mlq.trade import run_trade_runtime_build, trade_runtime_ledger_path


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
            CREATE TABLE malf_state_snapshot (
                snapshot_nk TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                code TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                asof_bar_dt DATE NOT NULL,
                major_state TEXT NOT NULL,
                trend_direction TEXT NOT NULL,
                reversal_stage TEXT NOT NULL,
                wave_id BIGINT NOT NULL,
                current_hh_count BIGINT NOT NULL,
                current_ll_count BIGINT NOT NULL
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
            INSERT INTO malf_state_snapshot VALUES
            ('state-001', 'stock', '000001.SZ', 'D', '2026-04-08', '牛顺', 'up', 'none', 0, 1, 0)
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


def test_mainline_truthfulness_revalidation_runs_to_trade_with_sidecar_read_only_and_none_prices(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    settings.ensure_directories()

    _seed_malf_sources(settings)
    _seed_alpha_trigger_candidates(settings)
    _seed_market_base_prices(settings)

    structure_summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-mainline-truthfulness-001",
        source_structure_input_table="structure_candidate_snapshot",
    )
    filter_summary = run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="filter-mainline-truthfulness-001",
    )
    trigger_summary = run_alpha_trigger_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-trigger-mainline-truthfulness-001",
    )
    alpha_summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-formal-mainline-truthfulness-001",
    )
    position_summary = run_position_formal_signal_materialization(
        settings=settings,
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="position-mainline-truthfulness-001",
    )
    portfolio_summary = run_portfolio_plan_build(
        settings=settings,
        portfolio_id="main_book",
        portfolio_gross_cap_weight=0.20,
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="portfolio-mainline-truthfulness-001",
    )
    trade_summary = run_trade_runtime_build(
        settings=settings,
        portfolio_id="main_book",
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="trade-mainline-truthfulness-001",
    )

    assert structure_summary.materialized_snapshot_count == 1
    assert filter_summary.materialized_snapshot_count == 1
    assert trigger_summary.materialized_trigger_count == 1
    assert alpha_summary.materialized_signal_count == 1
    assert position_summary.enriched_signal_count == 1
    assert position_summary.adjust_method == "none"
    assert portfolio_summary.admitted_count == 1
    assert trade_summary.planned_entry_count == 1

    structure_conn = duckdb.connect(str(structure_ledger_path(settings)), read_only=True)
    try:
        structure_row = structure_conn.execute(
            """
            SELECT
                structure_progress_state,
                break_confirmation_status,
                break_confirmation_ref,
                stats_snapshot_nk,
                exhaustion_risk_bucket,
                reversal_probability_bucket
            FROM structure_snapshot
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        structure_conn.close()

    filter_conn = duckdb.connect(str(filter_ledger_path(settings)), read_only=True)
    try:
        filter_row = filter_conn.execute(
            """
            SELECT
                filter_snapshot_nk,
                structure_snapshot_nk,
                trigger_admissible,
                primary_blocking_condition,
                admission_notes,
                break_confirmation_status,
                exhaustion_risk_bucket
            FROM filter_snapshot
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        filter_conn.close()

    alpha_conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        trigger_row = alpha_conn.execute(
            """
            SELECT source_filter_snapshot_nk, source_structure_snapshot_nk
            FROM alpha_trigger_event
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
        signal_row = alpha_conn.execute(
            """
            SELECT formal_signal_status, trigger_admissible, source_trigger_event_nk
            FROM alpha_formal_signal_event
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        alpha_conn.close()

    position_conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        position_row = position_conn.execute(
            """
            SELECT a.candidate_status, s.reference_trade_date, s.reference_price, s.target_shares
            FROM position_candidate_audit AS a
            INNER JOIN position_sizing_snapshot AS s
                ON s.candidate_nk = a.candidate_nk
            WHERE a.instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        position_conn.close()

    portfolio_conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        portfolio_row = portfolio_conn.execute(
            """
            SELECT plan_status, admitted_weight
            FROM portfolio_plan_snapshot
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        portfolio_conn.close()

    trade_conn = duckdb.connect(str(trade_runtime_ledger_path(settings)), read_only=True)
    try:
        trade_row = trade_conn.execute(
            """
            SELECT execution_status, planned_entry_trade_date, planned_entry_weight
            FROM trade_execution_plan
            WHERE instrument = '000001.SZ' AND execution_action = 'enter'
            """
        ).fetchone()
    finally:
        trade_conn.close()

    assert structure_row == (
        "advancing",
        "confirmed",
        "break-001",
        "stats-001",
        "high",
        "elevated",
    )
    assert filter_row[2] is True
    assert filter_row[3] is None
    assert filter_row[5] == "confirmed"
    assert filter_row[6] == "high"
    assert "break_confirmation=confirmed 仅 sidecar 提示" in str(filter_row[4])
    assert "exhaustion_risk=high" in str(filter_row[4])
    assert trigger_row == (filter_row[0], filter_row[1])
    assert signal_row[0:2] == ("admitted", True)
    assert signal_row[2]
    assert position_row == ("admitted", date(2026, 4, 9), 10.6, 17600)
    assert portfolio_row == ("admitted", 0.1875)
    assert trade_row == ("planned_entry", date(2026, 4, 11), 0.1875)
