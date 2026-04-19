"""覆盖 `alpha family ledger` 官方 bounded runner。"""

from __future__ import annotations

import json
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


def test_run_alpha_family_build_materializes_structured_payload_for_five_trigger_roles(
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
            ("000002.SZ", "2026-04-08", "2026-04-08", "PAS", "tst", "TST"),
            ("000003.SZ", "2026-04-08", "2026-04-08", "PAS", "pb", "PB"),
            ("000004.SZ", "2026-04-08", "2026-04-08", "PAS", "cpb", "CPB"),
            ("000005.SZ", "2026-04-08", "2026-04-08", "PAS", "bpb", "BPB"),
        ],
    )
    _seed_malf_sources(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-301", "BULL_MAINSTREAM", 2, 4, "2026-04-08"),
            ("000002.SZ", "2026-04-08", "2026-04-08", "ctx-302", "BULL_MAINSTREAM", 3, 4, "2026-04-08"),
            ("000003.SZ", "2026-04-08", "2026-04-08", "ctx-303", "BULL_MAINSTREAM", 2, 4, "2026-04-08"),
            ("000004.SZ", "2026-04-08", "2026-04-08", "ctx-304", "BULL_MAINSTREAM", 5, 4, "2026-04-08"),
            ("000005.SZ", "2026-04-08", "2026-04-08", "ctx-305", "BEAR_MAINSTREAM", 0, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.8, 0.7, False, None),
            ("000002.SZ", "2026-04-08", "2026-04-08", 3, 0, 0.8, 0.7, False, None),
            ("000003.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.8, 0.7, False, None),
            ("000004.SZ", "2026-04-08", "2026-04-08", 5, 0, 0.8, 0.7, False, None),
            ("000005.SZ", "2026-04-08", "2026-04-08", 0, 2, 0.0, 0.0, True, "failed_extreme"),
        ],
        higher_timeframe_rows=[
            ("state-w-301", "000001.SZ", "W", "2026-04-03", "牛顺", "up", "none", 3, 2, 0),
            ("state-m-301", "000001.SZ", "M", "2026-03-31", "牛顺", "up", "none", 2, 1, 0),
            ("state-w-302", "000002.SZ", "W", "2026-04-03", "牛顺", "up", "none", 4, 2, 0),
            ("state-m-302", "000002.SZ", "M", "2026-03-31", "牛顺", "up", "none", 2, 1, 0),
            ("state-w-303", "000003.SZ", "W", "2026-04-03", "牛顺", "up", "none", 4, 2, 0),
            ("state-m-303", "000003.SZ", "M", "2026-03-31", "牛顺", "up", "none", 2, 1, 0),
            ("state-w-304", "000004.SZ", "W", "2026-04-03", "牛顺", "up", "none", 5, 3, 0),
            ("state-m-304", "000004.SZ", "M", "2026-03-31", "牛顺", "up", "none", 3, 2, 0),
            ("state-w-305", "000005.SZ", "W", "2026-04-03", "熊顺", "down", "none", 4, 0, 3),
            ("state-m-305", "000005.SZ", "M", "2026-03-31", "熊顺", "down", "none", 2, 0, 2),
        ],
    )
    _materialize_official_upstream(settings, suffix="family-001")
    _materialize_official_trigger(settings, suffix="family-001")

    summary = run_alpha_family_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        family_scope=["bof", "tst", "pb", "cpb", "bpb"],
        limit=10,
        batch_size=1,
        run_id="alpha-family-test-001",
    )

    assert summary.candidate_trigger_count == 5
    assert summary.materialized_family_event_count == 5
    assert summary.inserted_count == 5
    assert summary.family_counts == {"bof": 1, "tst": 1, "pb": 1, "cpb": 1, "bpb": 1}

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
            SELECT trigger_type, family_code, payload_json
            FROM alpha_family_event
            ORDER BY trigger_type
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", 5, 5)
    payload_by_type = {
        row[0]: {
            "family_code": row[1],
            "payload": json.loads(row[2]),
        }
        for row in event_rows
    }

    assert payload_by_type["bof"]["family_code"] == "bof_core"
    assert payload_by_type["bof"]["payload"]["family_role"] == "mainline"
    assert payload_by_type["bof"]["payload"]["family_bias"] == "reversal_attempt"
    assert payload_by_type["bof"]["payload"]["structure_anchor_nk"]

    assert payload_by_type["tst"]["payload"]["family_role"] == "mainline"
    assert payload_by_type["tst"]["payload"]["malf_alignment"] == "aligned"
    assert payload_by_type["tst"]["payload"]["official_context"]["malf"]["daily"]["timeframe"] == "D"

    assert payload_by_type["pb"]["payload"]["family_role"] == "mainline"
    assert payload_by_type["pb"]["payload"]["pb_first_pullback"] is True
    assert payload_by_type["pb"]["payload"]["malf_phase_bucket"] in {"early", "middle"}

    assert payload_by_type["cpb"]["payload"]["family_role"] == "scout"
    assert payload_by_type["cpb"]["payload"]["family_bias"] == "countertrend_probe"

    assert payload_by_type["bpb"]["payload"]["family_role"] == "warning"
    assert payload_by_type["bpb"]["payload"]["malf_alignment"] == "conflicted"
    assert payload_by_type["bpb"]["payload"]["source_context_fingerprint"]


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

    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        baseline_payload = json.loads(
            conn.execute(
                """
                SELECT payload_json
                FROM alpha_family_event
                WHERE trigger_type = 'bof'
                """
            ).fetchone()[0]
        )
    finally:
        conn.close()

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
        event_payload = json.loads(
            conn.execute(
                """
                SELECT payload_json
                FROM alpha_family_event
                WHERE trigger_type = 'bof'
                """
            ).fetchone()[0]
        )
        event_run_id = conn.execute(
            """
            SELECT last_materialized_run_id
            FROM alpha_family_event
            WHERE trigger_type = 'bof'
            """
        ).fetchone()[0]
    finally:
        conn.close()

    assert event_run_id == "alpha-family-test-002c"
    assert baseline_payload["source_context_fingerprint"] != event_payload["source_context_fingerprint"]
    assert event_payload["malf_alignment"] == "conflicted"
    assert event_payload["family_role"] == "supporting"
    assert event_payload["source_context_snapshot"]["structure_progress_state"] == "failed"
