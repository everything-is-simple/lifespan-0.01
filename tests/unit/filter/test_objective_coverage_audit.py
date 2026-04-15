"""覆盖 `filter objective coverage audit`。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.data import RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE, bootstrap_raw_market_ledger
from mlq.filter import run_filter_objective_coverage_audit, run_filter_snapshot_build
from mlq.structure import bootstrap_structure_snapshot_ledger, connect_structure_ledger


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


def _seed_structure_snapshots(settings) -> None:
    conn = connect_structure_ledger(settings)
    try:
        bootstrap_structure_snapshot_ledger(settings, connection=conn)
        conn.execute(
            """
            INSERT INTO structure_snapshot (
                structure_snapshot_nk,
                instrument,
                signal_date,
                asof_date,
                major_state,
                trend_direction,
                reversal_stage,
                wave_id,
                current_hh_count,
                current_ll_count,
                daily_major_state,
                daily_trend_direction,
                daily_reversal_stage,
                daily_wave_id,
                daily_current_hh_count,
                daily_current_ll_count,
                daily_source_context_nk,
                weekly_major_state,
                weekly_trend_direction,
                weekly_reversal_stage,
                weekly_wave_id,
                weekly_current_hh_count,
                weekly_current_ll_count,
                weekly_source_context_nk,
                monthly_major_state,
                monthly_trend_direction,
                monthly_reversal_stage,
                monthly_wave_id,
                monthly_current_hh_count,
                monthly_current_ll_count,
                monthly_source_context_nk,
                structure_progress_state,
                break_confirmation_status,
                break_confirmation_ref,
                stats_snapshot_nk,
                exhaustion_risk_bucket,
                reversal_probability_bucket,
                source_context_nk,
                structure_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES
                (
                    'ss-001', '000001.SZ', '2026-04-08', '2026-04-08', '牛顺', 'up', 'expand', 7, 2, 0,
                    '牛顺', 'up', 'expand', 7, 2, 0, 'ctx-d-001',
                    '牛顺', 'up', 'none', 3, 1, 0, 'ctx-w-001',
                    '牛逆', 'down', 'trigger', 1, 0, 1, 'ctx-m-001',
                    'advancing', 'confirmed', 'break-001', 'stats-001', 'high', 'elevated', 'ctx-001',
                    'structure-snapshot-v2', 'run-a', 'run-a'
                ),
                (
                    'ss-002', '000002.SZ', '2026-04-08', '2026-04-08', '熊顺', 'down', 'expand', 9, 0, 1,
                    '熊顺', 'down', 'expand', 9, 0, 1, 'ctx-d-002',
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    'failed', NULL, NULL, NULL, NULL, NULL, 'ctx-002',
                    'structure-snapshot-v2', 'run-a', 'run-a'
                )
            """
        )
    finally:
        conn.close()


def _seed_context_rows(malf_path: Path) -> None:
    malf_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(malf_path))
    try:
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
            INSERT INTO malf_state_snapshot VALUES
            ('state-001', 'stock', '000001.SZ', 'D', '2026-04-08', '牛顺', 'up', 'expand', 7, 2, 0)
            """
        )
    finally:
        conn.close()


def _seed_partial_objective_profiles(settings) -> None:
    bootstrap_raw_market_ledger(settings)
    conn = duckdb.connect(str(settings.databases.raw_market))
    try:
        conn.execute(
            f"""
            INSERT INTO {RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE} (
                profile_nk,
                code,
                asset_type,
                observed_trade_date,
                name,
                market_type,
                security_type,
                suspension_status,
                risk_warning_status,
                delisting_status,
                is_suspended_or_unresumed,
                is_risk_warning_excluded,
                is_delisting_arrangement,
                source_run_id,
                source_request_nk,
                raw_payload_json
            )
            VALUES (
                '000001.SZ|stock|2026-04-07',
                '000001.SZ',
                'stock',
                '2026-04-07',
                '平安银行',
                'sz',
                'stock',
                'trading',
                NULL,
                NULL,
                FALSE,
                FALSE,
                FALSE,
                'tq-run-001',
                'req-001',
                '{{}}'
            )
            """
        )
    finally:
        conn.close()


def _seed_filter_snapshots(settings) -> None:
    _seed_structure_snapshots(settings)
    _seed_context_rows(settings.databases.malf)
    run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="filter-audit-test-run-001",
    )


def test_filter_objective_coverage_audit_marks_all_rows_missing_when_objective_table_absent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_filter_snapshots(settings)

    summary = run_filter_objective_coverage_audit(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        group_limit=10,
    )

    assert summary.filter_snapshot_count == 2
    assert summary.objective_profile_table_present is False
    assert summary.objective_profile_row_count == 0
    assert summary.covered_objective_count == 0
    assert summary.missing_objective_count == 2
    assert summary.suggested_backfill_start_date == "2026-04-08"
    assert summary.suggested_backfill_end_date == "2026-04-08"
    assert summary.top_missing_by_signal_date == (
        summary.top_missing_by_signal_date[0].__class__(bucket_key="2026-04-08", missing_count=2),
    )
    assert summary.top_missing_by_market_type == (
        summary.top_missing_by_market_type[0].__class__(bucket_key="sz", missing_count=2),
    )


def test_filter_objective_coverage_audit_resolves_latest_profile_on_or_before_signal_date(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_filter_snapshots(settings)
    _seed_partial_objective_profiles(settings)

    summary = run_filter_objective_coverage_audit(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        group_limit=10,
    )

    assert summary.filter_snapshot_count == 2
    assert summary.objective_profile_table_present is True
    assert summary.objective_profile_row_count == 1
    assert summary.objective_profile_instrument_count == 1
    assert summary.covered_objective_count == 1
    assert summary.missing_objective_count == 1
    assert summary.missing_ratio == 0.5
    assert summary.suggested_backfill_start_date == "2026-04-08"
    assert summary.suggested_backfill_end_date == "2026-04-08"
    assert summary.top_missing_by_instrument == (
        summary.top_missing_by_instrument[0].__class__(bucket_key="000002.SZ", missing_count=1),
    )
    assert summary.top_missing_by_market_type == (
        summary.top_missing_by_market_type[0].__class__(bucket_key="sz", missing_count=1),
    )
