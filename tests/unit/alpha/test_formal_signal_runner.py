"""覆盖 `alpha formal signal` 官方 producer 的正式落表行为。"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from mlq.alpha import alpha_ledger_path, run_alpha_formal_signal_build
from mlq.core.paths import default_settings
from tests.unit.alpha.test_runner import (
    _bootstrap_repo_root,
    _clear_workspace_env,
    _materialize_official_family,
    _materialize_official_trigger,
    _materialize_official_upstream,
    _replace_structure_source,
    _seed_malf_sources,
    _seed_trigger_source,
    _seed_wave_life_snapshot,
)


def test_run_alpha_formal_signal_build_materializes_run_event_and_run_bridge(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_trigger_source(
        settings.databases.alpha,
        [
            ("000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
            ("000002.SZ", "2026-04-08", "2026-04-08", "PAS", "pb", "PB"),
        ],
    )
    _seed_malf_sources(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-001", "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
            ("000002.SZ", "2026-04-08", "2026-04-08", "ctx-002", "BEAR_MAINSTREAM", 0, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.7, 0.6, False, None),
            ("000002.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme"),
        ],
        higher_timeframe_rows=[
            ("state-w-101", "000001.SZ", "W", "2026-04-03", "牛顺", "up", "none", 3, 1, 0),
            ("state-m-101", "000001.SZ", "M", "2026-03-31", "牛逆", "down", "trigger", 1, 0, 1),
        ],
    )
    _seed_wave_life_snapshot(
        settings.databases.malf,
        [
            (
                "wl-000001-2026-04-08",
                "stock",
                "000001.SZ",
                "D",
                "2026-04-08",
                0,
                "wave-000001",
                "state-000001.SZ-2026-04-08",
                "牛顺",
                "none",
                3,
                0.95,
                1.0,
                2.0,
                "high",
                32,
                "wave-life-v1",
                "profile-000001",
                "completed_wave_sample",
                "wave-life-seed-001",
                "wave-life-seed-001",
            ),
            (
                "wl-000002-2026-04-08",
                "stock",
                "000002.SZ",
                "D",
                "2026-04-08",
                0,
                "wave-000002",
                "state-000002.SZ-2026-04-08",
                "熊顺",
                "none",
                1,
                0.40,
                4.0,
                6.0,
                "normal",
                18,
                "wave-life-v1",
                "profile-000002",
                "completed_wave_sample",
                "wave-life-seed-002",
                "wave-life-seed-002",
            ),
        ],
    )
    _materialize_official_upstream(settings, suffix="001")
    _materialize_official_trigger(settings, suffix="001")
    _materialize_official_family(settings, suffix="001")
    conn = duckdb.connect(str(alpha_ledger_path(settings)))
    try:
        family_event_nk, payload_json = conn.execute(
            """
            SELECT family_event_nk, payload_json
            FROM alpha_family_event
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
        payload = json.loads(payload_json)
        payload["malf_phase_bucket"] = "middle"
        conn.execute(
            """
            UPDATE alpha_family_event
            SET payload_json = ?, updated_at = CURRENT_TIMESTAMP
            WHERE family_event_nk = ?
            """,
            [json.dumps(payload, ensure_ascii=False, sort_keys=True), family_event_nk],
        )
    finally:
        conn.close()
    summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        batch_size=1,
        run_id="alpha-formal-signal-test-001",
    )
    assert summary.candidate_trigger_count == 2
    assert summary.materialized_signal_count == 2
    assert summary.inserted_count == 2
    assert summary.admitted_count == 0
    assert summary.blocked_count == 0
    assert summary.deferred_count == 2
    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_status, bounded_instrument_count
            FROM alpha_formal_signal_run
            WHERE run_id = 'alpha-formal-signal-test-001'
            """
        ).fetchone()
        event_rows = conn.execute(
            """
            SELECT
                instrument,
                pattern_code,
                formal_signal_status,
                trigger_admissible,
                major_state,
                reversal_stage,
                daily_source_context_nk,
                weekly_major_state,
                monthly_major_state,
                family_code,
                family_role,
                source_family_event_nk,
                wave_life_percentile,
                termination_risk_bucket,
                admission_verdict_code,
                admission_verdict_owner,
                admission_reason_code,
                stage_percentile_decision_code,
                stage_percentile_action_owner,
                signal_contract_version
            FROM alpha_formal_signal_event
            ORDER BY instrument
            """
        ).fetchall()
        run_event_rows = conn.execute(
            """
            SELECT
                signal_nk,
                source_family_event_nk,
                family_role,
                formal_signal_status,
                admission_verdict_code,
                admission_verdict_owner,
                admission_reason_code,
                stage_percentile_decision_code,
                stage_percentile_action_owner
            FROM alpha_formal_signal_run_event
            WHERE run_id = 'alpha-formal-signal-test-001'
            ORDER BY signal_nk
            """
        ).fetchall()
    finally:
        conn.close()
    assert run_row == ("completed", 2)
    assert event_rows == [
        (
            "000001.SZ",
            "BOF",
            "deferred",
            True,
            "牛顺",
            "none",
            "state-000001.SZ-2026-04-08",
            "牛顺",
            "牛逆",
            "bof_core",
            "mainline",
            "000001.SZ|2026-04-08|2026-04-08|PAS|bof|BOF|alpha-trigger-v2|alpha-family-v2",
            0.95,
            "high",
            "note_only",
            "alpha_formal_signal",
            "stage_percentile_alpha_caution_note",
            "alpha_caution_note",
            "alpha_note",
            "alpha-formal-signal-v5",
        ),
        (
            "000002.SZ",
            "PB",
            "deferred",
            True,
            "熊顺",
            "none",
            "state-000002.SZ-2026-04-08",
            None,
            None,
            "pb_core",
            "supporting",
            "000002.SZ|2026-04-08|2026-04-08|PAS|pb|PB|alpha-trigger-v2|alpha-family-v2",
            0.40,
            "normal",
            "downgraded",
            "alpha_formal_signal",
            "family_alignment_conflicted",
            "observe_only",
            "none",
            "alpha-formal-signal-v5",
        ),
    ]
    assert run_event_rows == [
        (
            "000001.SZ|2026-04-08|2026-04-08|PAS|bof|BOF|000001.SZ|2026-04-08|2026-04-08|PAS|bof|BOF|alpha-trigger-v2|alpha-formal-signal-v5",
            "000001.SZ|2026-04-08|2026-04-08|PAS|bof|BOF|alpha-trigger-v2|alpha-family-v2",
            "mainline",
            "deferred",
            "note_only",
            "alpha_formal_signal",
            "stage_percentile_alpha_caution_note",
            "alpha_caution_note",
            "alpha_note",
        ),
        (
            "000002.SZ|2026-04-08|2026-04-08|PAS|pb|PB|000002.SZ|2026-04-08|2026-04-08|PAS|pb|PB|alpha-trigger-v2|alpha-formal-signal-v5",
            "000002.SZ|2026-04-08|2026-04-08|PAS|pb|PB|alpha-trigger-v2|alpha-family-v2",
            "supporting",
            "deferred",
            "downgraded",
            "alpha_formal_signal",
            "family_alignment_conflicted",
            "observe_only",
            "none",
        ),
    ]


def test_run_alpha_formal_signal_build_marks_rematerialized_when_official_upstream_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_trigger_source(
        settings.databases.alpha,
        [
            ("000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
        ],
    )
    _seed_malf_sources(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-101", "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 1, 0, 0.4, 0.3, False, None),
        ],
    )
    _materialize_official_upstream(settings, suffix="002a")
    _materialize_official_trigger(settings, suffix="002a")
    _materialize_official_family(settings, suffix="002a")
    first_summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-formal-signal-test-002a",
    )
    assert first_summary.inserted_count == 1
    _replace_structure_source(
        settings.databases.malf,
        [("000001.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme")],
    )
    _materialize_official_upstream(settings, suffix="002b")
    _materialize_official_trigger(settings, suffix="002b")
    _materialize_official_family(settings, suffix="002b")
    second_summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-formal-signal-test-002b",
    )
    assert second_summary.rematerialized_count == 1
    assert second_summary.admitted_count == 0
    assert second_summary.blocked_count == 0
    assert second_summary.deferred_count == 1
    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        event_row = conn.execute(
            """
            SELECT
                formal_signal_status,
                trigger_admissible,
                admission_verdict_code,
                admission_verdict_owner,
                admission_reason_code,
                family_role,
                malf_alignment,
                signal_contract_version,
                last_materialized_run_id
            FROM alpha_formal_signal_event
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        conn.close()
    assert event_row == (
        "deferred",
        True,
        "downgraded",
        "alpha_formal_signal",
        "family_alignment_conflicted",
        "supporting",
        "conflicted",
        "alpha-formal-signal-v5",
        "alpha-formal-signal-test-002b",
    )
