"""alpha PAS 五触发 detector runner 测试。"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from mlq.alpha import (
    alpha_ledger_path,
    run_alpha_family_build,
    run_alpha_pas_five_trigger_build,
    run_alpha_trigger_build,
)
from mlq.core.paths import default_settings
from mlq.filter.bootstrap import bootstrap_filter_snapshot_ledger, connect_filter_ledger
from mlq.structure.bootstrap import bootstrap_structure_snapshot_ledger, connect_structure_ledger


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


def _seed_structure_and_filter(settings) -> None:
    structure_conn = connect_structure_ledger(settings)
    filter_conn = connect_filter_ledger(settings)
    settings.databases.malf.parent.mkdir(parents=True, exist_ok=True)
    malf_conn = duckdb.connect(str(settings.databases.malf))
    try:
        bootstrap_structure_snapshot_ledger(settings, connection=structure_conn)
        bootstrap_filter_snapshot_ledger(settings, connection=filter_conn)
        malf_conn.execute(
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
        structure_conn.execute(
            """
            INSERT INTO structure_snapshot (
                structure_snapshot_nk, instrument, signal_date, asof_date,
                major_state, trend_direction, reversal_stage, wave_id, current_hh_count, current_ll_count,
                daily_major_state, daily_trend_direction, daily_reversal_stage, daily_wave_id, daily_current_hh_count, daily_current_ll_count, daily_source_context_nk,
                weekly_major_state, weekly_trend_direction, weekly_reversal_stage, weekly_wave_id, weekly_current_hh_count, weekly_current_ll_count, weekly_source_context_nk,
                monthly_major_state, monthly_trend_direction, monthly_reversal_stage, monthly_wave_id, monthly_current_hh_count, monthly_current_ll_count, monthly_source_context_nk,
                structure_progress_state, break_confirmation_status, break_confirmation_ref, stats_snapshot_nk,
                exhaustion_risk_bucket, reversal_probability_bucket, source_context_nk,
                structure_contract_version, first_seen_run_id, last_materialized_run_id
            )
            VALUES
                ('ss-bof', '000001.SZ', '2026-04-30', '2026-04-30', '牛逆', 'down', 'trigger', 1, 0, 2,
                 '牛逆', 'down', 'trigger', 1, 0, 2, 'ctx-d-bof',
                 '牛顺', 'up', 'none', 3, 2, 0, 'ctx-w-bof',
                 '熊逆', 'down', 'trigger', 1, 0, 1, 'ctx-m-bof',
                 'advancing', 'confirmed', 'break-bof', 'stats-bof',
                 'elevated', 'high', 'ctx-bof', 'structure-snapshot-v2', 'run-a', 'run-a'),
                ('ss-tst', '000002.SZ', '2026-04-30', '2026-04-30', '牛顺', 'up', 'expand', 7, 3, 0,
                 '牛顺', 'up', 'expand', 7, 3, 0, 'ctx-d-tst',
                 '牛顺', 'up', 'none', 4, 2, 0, 'ctx-w-tst',
                 '牛逆', 'down', 'trigger', 1, 0, 1, 'ctx-m-tst',
                 'advancing', 'confirmed', 'break-tst', 'stats-tst',
                 'normal', 'medium', 'ctx-tst', 'structure-snapshot-v2', 'run-a', 'run-a'),
                ('ss-pb', '000003.SZ', '2026-04-30', '2026-04-30', '牛顺', 'up', 'expand', 9, 4, 0,
                 '牛顺', 'up', 'expand', 9, 4, 0, 'ctx-d-pb',
                 '牛顺', 'up', 'none', 4, 2, 0, 'ctx-w-pb',
                 '牛顺', 'up', 'none', 2, 1, 0, 'ctx-m-pb',
                 'advancing', 'confirmed', 'break-pb', 'stats-pb',
                 'normal', 'medium', 'ctx-pb', 'structure-snapshot-v2', 'run-a', 'run-a'),
                ('ss-cpb', '000004.SZ', '2026-04-30', '2026-04-30', '牛顺', 'up', 'expand', 11, 5, 0,
                 '牛顺', 'up', 'expand', 11, 5, 0, 'ctx-d-cpb',
                 '牛顺', 'up', 'none', 5, 3, 0, 'ctx-w-cpb',
                 '牛顺', 'up', 'none', 2, 1, 0, 'ctx-m-cpb',
                 'advancing', 'confirmed', 'break-cpb', 'stats-cpb',
                 'normal', 'medium', 'ctx-cpb', 'structure-snapshot-v2', 'run-a', 'run-a'),
                ('ss-bpb', '000005.SZ', '2026-04-30', '2026-04-30', '牛顺', 'up', 'expand', 8, 3, 0,
                 '牛顺', 'up', 'expand', 8, 3, 0, 'ctx-d-bpb',
                 '牛顺', 'up', 'none', 4, 2, 0, 'ctx-w-bpb',
                 '牛顺', 'up', 'none', 2, 1, 0, 'ctx-m-bpb',
                 'advancing', 'confirmed', 'break-bpb', 'stats-bpb',
                 'normal', 'medium', 'ctx-bpb', 'structure-snapshot-v2', 'run-a', 'run-a')
            """
        )
        filter_conn.execute(
            """
            INSERT INTO filter_snapshot (
                filter_snapshot_nk, structure_snapshot_nk, instrument, signal_date, asof_date,
                major_state, trend_direction, reversal_stage, wave_id, current_hh_count, current_ll_count,
                daily_major_state, daily_trend_direction, daily_reversal_stage, daily_wave_id, daily_current_hh_count, daily_current_ll_count, daily_source_context_nk,
                weekly_major_state, weekly_trend_direction, weekly_reversal_stage, weekly_wave_id, weekly_current_hh_count, weekly_current_ll_count, weekly_source_context_nk,
                monthly_major_state, monthly_trend_direction, monthly_reversal_stage, monthly_wave_id, monthly_current_hh_count, monthly_current_ll_count, monthly_source_context_nk,
                trigger_admissible, filter_gate_code, filter_reject_reason_code, primary_blocking_condition, blocking_conditions_json, admission_notes,
                break_confirmation_status, break_confirmation_ref, stats_snapshot_nk, exhaustion_risk_bucket, reversal_probability_bucket,
                source_context_nk, filter_contract_version, first_seen_run_id, last_materialized_run_id
            )
            VALUES
                ('fs-000001.SZ', 'ss-bof', '000001.SZ', '2026-04-30', '2026-04-30', '牛逆', 'down', 'trigger', 1, 0, 2,
                 '牛逆', 'down', 'trigger', 1, 0, 2, 'ctx-d-bof',
                 '牛顺', 'up', 'none', 3, 2, 0, 'ctx-w-bof',
                 '熊逆', 'down', 'trigger', 1, 0, 1, 'ctx-m-bof',
                 TRUE, 'pre_trigger_passed', NULL, NULL, '[]', 'filter-admitted',
                 'confirmed', 'break-bof', 'stats-bof', 'elevated', 'high',
                 'ctx-bof', 'filter-snapshot-v2', 'run-a', 'run-a'),
                ('fs-000002.SZ', 'ss-tst', '000002.SZ', '2026-04-30', '2026-04-30', '牛顺', 'up', 'expand', 7, 3, 0,
                 '牛顺', 'up', 'expand', 7, 3, 0, 'ctx-d-tst',
                 '牛顺', 'up', 'none', 4, 2, 0, 'ctx-w-tst',
                 '牛逆', 'down', 'trigger', 1, 0, 1, 'ctx-m-tst',
                 TRUE, 'pre_trigger_passed', NULL, NULL, '[]', 'filter-admitted',
                 'confirmed', 'break-tst', 'stats-tst', 'normal', 'medium',
                 'ctx-tst', 'filter-snapshot-v2', 'run-a', 'run-a'),
                ('fs-000003.SZ', 'ss-pb', '000003.SZ', '2026-04-30', '2026-04-30', '牛顺', 'up', 'expand', 9, 4, 0,
                 '牛顺', 'up', 'expand', 9, 4, 0, 'ctx-d-pb',
                 '牛顺', 'up', 'none', 4, 2, 0, 'ctx-w-pb',
                 '牛顺', 'up', 'none', 2, 1, 0, 'ctx-m-pb',
                 TRUE, 'pre_trigger_passed', NULL, NULL, '[]', 'filter-admitted',
                 'confirmed', 'break-pb', 'stats-pb', 'normal', 'medium',
                 'ctx-pb', 'filter-snapshot-v2', 'run-a', 'run-a'),
                ('fs-000004.SZ', 'ss-cpb', '000004.SZ', '2026-04-30', '2026-04-30', '牛顺', 'up', 'expand', 11, 5, 0,
                 '牛顺', 'up', 'expand', 11, 5, 0, 'ctx-d-cpb',
                 '牛顺', 'up', 'none', 5, 3, 0, 'ctx-w-cpb',
                 '牛顺', 'up', 'none', 2, 1, 0, 'ctx-m-cpb',
                 TRUE, 'pre_trigger_passed', NULL, NULL, '[]', 'filter-admitted',
                 'confirmed', 'break-cpb', 'stats-cpb', 'normal', 'medium',
                 'ctx-cpb', 'filter-snapshot-v2', 'run-a', 'run-a'),
                ('fs-000005.SZ', 'ss-bpb', '000005.SZ', '2026-04-30', '2026-04-30', '牛顺', 'up', 'expand', 8, 3, 0,
                 '牛顺', 'up', 'expand', 8, 3, 0, 'ctx-d-bpb',
                 '牛顺', 'up', 'none', 4, 2, 0, 'ctx-w-bpb',
                 '牛顺', 'up', 'none', 2, 1, 0, 'ctx-m-bpb',
                 TRUE, 'pre_trigger_passed', NULL, NULL, '[]', 'filter-admitted',
                 'confirmed', 'break-bpb', 'stats-bpb', 'normal', 'medium',
                 'ctx-bpb', 'filter-snapshot-v2', 'run-a', 'run-a')
            """
        )
        filter_conn.execute(
            """
            INSERT INTO filter_checkpoint (
                asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id
            )
            VALUES
                ('stock', '000001.SZ', 'D', '2026-04-30', '2026-04-30', '2026-04-30', 'filter-source-a', 'filter-run-a'),
                ('stock', '000002.SZ', 'D', '2026-04-30', '2026-04-30', '2026-04-30', 'filter-source-a', 'filter-run-a'),
                ('stock', '000003.SZ', 'D', '2026-04-30', '2026-04-30', '2026-04-30', 'filter-source-a', 'filter-run-a'),
                ('stock', '000004.SZ', 'D', '2026-04-30', '2026-04-30', '2026-04-30', 'filter-source-a', 'filter-run-a'),
                ('stock', '000005.SZ', 'D', '2026-04-30', '2026-04-30', '2026-04-30', 'filter-source-a', 'filter-run-a')
            """
        )
        malf_conn.execute(
            """
            INSERT INTO malf_state_snapshot VALUES
                ('ctx-bof', 'stock', '000001.SZ', 'D', '2026-04-30', '牛逆', 'down', 'trigger', 1, 0, 2),
                ('ctx-w-bof', 'stock', '000001.SZ', 'W', '2026-04-24', '牛顺', 'up', 'none', 3, 2, 0),
                ('ctx-m-bof', 'stock', '000001.SZ', 'M', '2026-03-31', '熊逆', 'down', 'trigger', 1, 0, 1),
                ('ctx-tst', 'stock', '000002.SZ', 'D', '2026-04-30', '牛顺', 'up', 'expand', 7, 3, 0),
                ('ctx-w-tst', 'stock', '000002.SZ', 'W', '2026-04-24', '牛顺', 'up', 'none', 4, 2, 0),
                ('ctx-m-tst', 'stock', '000002.SZ', 'M', '2026-03-31', '牛逆', 'down', 'trigger', 1, 0, 1),
                ('ctx-pb', 'stock', '000003.SZ', 'D', '2026-04-30', '牛顺', 'up', 'expand', 9, 4, 0),
                ('ctx-w-pb', 'stock', '000003.SZ', 'W', '2026-04-24', '牛顺', 'up', 'none', 4, 2, 0),
                ('ctx-m-pb', 'stock', '000003.SZ', 'M', '2026-03-31', '牛顺', 'up', 'none', 2, 1, 0),
                ('ctx-cpb', 'stock', '000004.SZ', 'D', '2026-04-30', '牛顺', 'up', 'expand', 11, 5, 0),
                ('ctx-w-cpb', 'stock', '000004.SZ', 'W', '2026-04-24', '牛顺', 'up', 'none', 5, 3, 0),
                ('ctx-m-cpb', 'stock', '000004.SZ', 'M', '2026-03-31', '牛顺', 'up', 'none', 2, 1, 0),
                ('ctx-bpb', 'stock', '000005.SZ', 'D', '2026-04-30', '牛顺', 'up', 'expand', 8, 3, 0),
                ('ctx-w-bpb', 'stock', '000005.SZ', 'W', '2026-04-24', '牛顺', 'up', 'none', 4, 2, 0),
                ('ctx-m-bpb', 'stock', '000005.SZ', 'M', '2026-03-31', '牛顺', 'up', 'none', 2, 1, 0)
            """
        )
    finally:
        structure_conn.close()
        filter_conn.close()
        malf_conn.close()


def _seed_market_base(settings) -> None:
    settings.databases.market_base.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(settings.databases.market_base))
    try:
        conn.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                trade_date DATE NOT NULL,
                adjust_method TEXT NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE
            )
            """
        )
        for code, bars in _build_all_histories().items():
            for trade_date, open_price, high_price, low_price, close_price, volume in bars:
                conn.execute(
                    "INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, 'backward', ?, ?, ?, ?, ?, ?)",
                    [code, code, trade_date, open_price, high_price, low_price, close_price, volume, volume * close_price],
                )
    finally:
        conn.close()


def _build_all_histories() -> dict[str, list[tuple[date, float, float, float, float, float]]]:
    return {
        "000001.SZ": _build_bof_history(),
        "000002.SZ": _build_tst_history(),
        "000003.SZ": _build_pb_history(),
        "000004.SZ": _build_cpb_history(),
        "000005.SZ": _build_bpb_history(),
    }


def _build_bof_history() -> list[tuple[date, float, float, float, float, float]]:
    start_date = date(2026, 4, 10)
    bars = []
    for offset in range(20):
        trade_date = start_date + timedelta(days=offset)
        bars.append((trade_date, 10.2, 10.6, 10.0, 10.3, 100.0))
    bars.append((date(2026, 4, 30), 9.8, 10.5, 9.7, 10.3, 140.0))
    return bars


def _build_tst_history() -> list[tuple[date, float, float, float, float, float]]:
    start_date = date(2026, 3, 1)
    bars = []
    for offset in range(55):
        trade_date = start_date + timedelta(days=offset)
        close_price = 10.6 + offset * 0.03
        bars.append((trade_date, close_price - 0.05, close_price + 0.2, 10.0 if offset == 0 else close_price - 0.15, close_price, 100.0))
    test_closes = [10.25, 10.3, 10.28, 10.35, 10.4]
    for index, close_price in enumerate(test_closes):
        trade_date = date(2026, 4, 25) + timedelta(days=index)
        bars.append((trade_date, close_price - 0.05, close_price + 0.2, 10.1, close_price, 100.0))
    bars.append((date(2026, 4, 30), 10.35, 10.95, 10.0, 10.85, 130.0))
    return bars


def _build_pb_history() -> list[tuple[date, float, float, float, float, float]]:
    start_date = date(2026, 3, 21)
    closes = [9.6 + i * 0.04 for i in range(20)]
    closes += [10.8 + i * 0.15 for i in range(15)]
    closes += [12.0, 11.8, 11.6, 11.5, 11.7]
    bars = []
    for offset, close_price in enumerate(closes):
        trade_date = start_date + timedelta(days=offset)
        bars.append((trade_date, close_price - 0.05, close_price + 0.2, close_price - 0.2, close_price, 100.0))
    bars.append((date(2026, 4, 30), 12.1, 12.6, 12.0, 12.45, 135.0))
    return bars


def _build_cpb_history() -> list[tuple[date, float, float, float, float, float]]:
    start_date = date(2026, 3, 20)
    closes = [9.8 + i * 0.12 for i in range(23)]
    closes += [12.2, 12.0, 12.15, 11.9, 12.0, 11.75, 11.9, 11.65, 11.8, 11.55, 11.7, 11.45, 11.6, 11.35, 11.5, 11.25, 11.4, 11.45]
    bars = []
    for offset, close_price in enumerate(closes):
        trade_date = start_date + timedelta(days=offset)
        bars.append((trade_date, close_price - 0.05, close_price + 0.2, close_price - 0.2, close_price, 100.0))
    bars.append((date(2026, 4, 30), 11.5, 11.9, 11.45, 11.8, 140.0))
    return bars


def _build_bpb_history() -> list[tuple[date, float, float, float, float, float]]:
    start_date = date(2026, 4, 5)
    closes = [9.2 + i * 0.03 for i in range(20)]
    pullback = [
        (10.15, 10.4, 10.3, 10.3, 130.0),
        (10.35, 11.2, 10.35, 10.9, 130.0),
        (10.85, 10.95, 10.45, 10.7, 100.0),
        (10.72, 10.82, 10.5, 10.65, 100.0),
        (10.68, 10.78, 10.55, 10.7, 100.0),
    ]
    bars = []
    for offset, close_price in enumerate(closes):
        trade_date = start_date + timedelta(days=offset)
        bars.append((trade_date, close_price - 0.03, close_price + 0.15, close_price - 0.15, close_price, 100.0))
    for index, item in enumerate(pullback):
        trade_date = start_date + timedelta(days=20 + index)
        bars.append((trade_date, item[0], item[1], item[2], item[3], item[4]))
    bars.append((date(2026, 4, 30), 11.0, 11.35, 10.95, 11.25, 145.0))
    return bars


def test_run_alpha_pas_five_trigger_build_materializes_official_candidates_and_downstream_family(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_structure_and_filter(settings)
    _seed_market_base(settings)

    pas_summary = run_alpha_pas_five_trigger_build(
        settings=settings,
        signal_start_date="2026-04-30",
        signal_end_date="2026-04-30",
        run_id="alpha-pas-test-001",
    )
    trigger_summary = run_alpha_trigger_build(
        settings=settings,
        signal_start_date="2026-04-30",
        signal_end_date="2026-04-30",
        run_id="alpha-trigger-test-pas-001",
    )
    family_summary = run_alpha_family_build(
        settings=settings,
        signal_start_date="2026-04-30",
        signal_end_date="2026-04-30",
        run_id="alpha-family-test-pas-001",
    )

    assert pas_summary.materialized_candidate_count == 5
    assert pas_summary.inserted_count == 5
    assert trigger_summary.materialized_trigger_count == 5
    assert family_summary.materialized_family_event_count == 5

    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        candidate_rows = conn.execute(
            """
            SELECT trigger_type, pattern_code, family_code
            FROM alpha_trigger_candidate
            ORDER BY trigger_type
            """
        ).fetchall()
        family_payload = conn.execute(
            """
            SELECT payload_json
            FROM alpha_family_event
            WHERE trigger_type = 'bof'
            """
        ).fetchone()
    finally:
        conn.close()

    assert candidate_rows == [
        ("bof", "BOF", "bof_core"),
        ("bpb", "BPB", "bpb_core"),
        ("cpb", "CPB", "cpb_core"),
        ("pb", "PB", "pb_core"),
        ("tst", "TST", "tst_core"),
    ]
    assert '"family_role"' in family_payload[0]
    assert '"malf_alignment"' in family_payload[0]
    assert '"source_context_fingerprint"' in family_payload[0]


def test_run_alpha_pas_five_trigger_build_uses_checkpoint_queue_and_rematerializes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_structure_and_filter(settings)
    _seed_market_base(settings)

    first_summary = run_alpha_pas_five_trigger_build(
        settings=settings,
        run_id="alpha-pas-test-queue-001a",
    )
    assert first_summary.execution_mode == "checkpoint_queue"
    assert first_summary.queue_claimed_count == 5
    assert first_summary.checkpoint_upserted_count == 5

    filter_conn = connect_filter_ledger(settings)
    try:
        filter_conn.execute(
            """
            UPDATE filter_checkpoint
            SET source_fingerprint = 'filter-source-b', last_run_id = 'filter-run-b', updated_at = CURRENT_TIMESTAMP
            WHERE code = '000001.SZ'
            """
        )
    finally:
        filter_conn.close()

    second_summary = run_alpha_pas_five_trigger_build(
        settings=settings,
        run_id="alpha-pas-test-queue-001b",
    )

    assert second_summary.execution_mode == "checkpoint_queue"
    assert second_summary.queue_claimed_count >= 1
    assert second_summary.reused_count >= 1

    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        queue_row = conn.execute(
            """
            SELECT queue_status
            FROM alpha_pas_trigger_work_queue
            WHERE code = '000001.SZ'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ).fetchone()
        checkpoint_row = conn.execute(
            """
            SELECT last_run_id, tail_start_bar_dt, tail_confirm_until_dt
            FROM alpha_pas_trigger_checkpoint
            WHERE code = '000001.SZ' AND timeframe = 'D'
            """
        ).fetchone()
    finally:
        conn.close()

    assert queue_row == ("completed",)
    assert checkpoint_row == ("alpha-pas-test-queue-001b", date(2026, 4, 30), date(2026, 4, 30))
