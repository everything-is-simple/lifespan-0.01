"""覆盖 `alpha family ledger` 官方 bounded runner。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.alpha import alpha_ledger_path, run_alpha_family_build
from mlq.core.paths import default_settings
from tests.unit.alpha.test_runner import (
    _bootstrap_repo_root,
    _clear_workspace_env,
    _materialize_official_trigger,
    _materialize_official_upstream,
    _replace_structure_source,
    _seed_malf_sources,
    _seed_trigger_source,
)


def test_run_alpha_family_build_materializes_family_run_event_and_trigger_bridge(
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
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-301", "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
            ("000002.SZ", "2026-04-08", "2026-04-08", "ctx-302", "BEAR_MAINSTREAM", 0, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.8, 0.7, False, None),
            ("000002.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme"),
        ],
    )
    _materialize_official_upstream(settings, suffix="family-001")
    _materialize_official_trigger(settings, suffix="family-001")

    summary = run_alpha_family_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        family_scope=["bof", "pb"],
        limit=10,
        batch_size=1,
        run_id="alpha-family-test-001",
    )

    assert summary.candidate_trigger_count == 2
    assert summary.materialized_family_event_count == 2
    assert summary.inserted_count == 2
    assert summary.family_counts == {"bof": 1, "pb": 1}

    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_status, bounded_instrument_count, materialized_family_event_count
            FROM alpha_family_run
            WHERE run_id = 'alpha-family-test-001'
            """
        ).fetchone()
        event_rows = conn.execute(
            """
            SELECT trigger_event_nk, trigger_type, family_code, payload_json
            FROM alpha_family_event
            ORDER BY trigger_event_nk
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", 2, 2)
    assert event_rows[0][1:3] == ("bof", "bof_core")
    assert event_rows[1][1:3] == ("pb", "pb_core")
    assert "source_trigger" in event_rows[0][3]


def test_run_alpha_family_build_marks_reused_and_rematerialized_when_trigger_context_changes(
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
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-311", "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.8, 0.7, False, None),
        ],
    )
    _materialize_official_upstream(settings, suffix="family-002a")
    _materialize_official_trigger(settings, suffix="family-002a")

    first_summary = run_alpha_family_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        family_scope=["bof"],
        run_id="alpha-family-test-002a",
    )
    second_summary = run_alpha_family_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        family_scope=["bof"],
        run_id="alpha-family-test-002b",
    )

    assert first_summary.inserted_count == 1
    assert second_summary.reused_count == 1

    _replace_structure_source(
        settings.databases.malf,
        [("000001.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme")],
    )
    _materialize_official_upstream(settings, suffix="family-002c")
    _materialize_official_trigger(settings, suffix="family-002c")
    third_summary = run_alpha_family_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        family_scope=["bof"],
        run_id="alpha-family-test-002c",
    )

    assert third_summary.rematerialized_count == 1

    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        event_row = conn.execute(
            """
            SELECT last_materialized_run_id, payload_json
            FROM alpha_family_event
            WHERE trigger_type = 'bof'
            """
        ).fetchone()
    finally:
        conn.close()

    assert event_row[0] == "alpha-family-test-002c"
    assert "failed_extreme" in event_row[1]
