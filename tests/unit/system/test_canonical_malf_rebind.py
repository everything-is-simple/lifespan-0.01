"""覆盖 `31` 卡要求的 canonical malf downstream rebind。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.alpha import (
    alpha_ledger_path,
    run_alpha_formal_signal_build,
    run_alpha_trigger_build,
)
from mlq.core.paths import default_settings
from mlq.filter import filter_ledger_path, run_filter_snapshot_build
from mlq.structure import run_structure_snapshot_build, structure_ledger_path


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


def _seed_canonical_malf_state(settings) -> None:
    malf_path = settings.databases.malf
    malf_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute(
            """
            CREATE TABLE malf_state_snapshot (
                snapshot_nk TEXT PRIMARY KEY,
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
            ('state-001', 'stock', '000001.SZ', 'D', '2026-04-08', '牛顺', 'up', 'expand', 7, 2, 0),
            ('state-002', 'stock', '000002.SZ', 'D', '2026-04-08', '熊顺', 'down', 'expand', 9, 0, 1)
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
            ('000001.SZ', '2026-04-08', '2026-04-08', 'PAS', 'bof', 'BOF'),
            ('000002.SZ', '2026-04-08', '2026-04-08', 'PAS', 'pb', 'PB')
            """
        )
    finally:
        conn.close()


def test_canonical_malf_defaults_drive_structure_filter_and_alpha(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    settings.ensure_directories()

    _seed_canonical_malf_state(settings)
    _seed_alpha_trigger_candidates(settings)

    structure_summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-canonical-rebind-001",
    )
    filter_summary = run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="filter-canonical-rebind-001",
    )
    trigger_summary = run_alpha_trigger_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-trigger-canonical-rebind-001",
    )
    alpha_summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-formal-canonical-rebind-001",
    )

    assert structure_summary.source_context_table == "malf_state_snapshot"
    assert structure_summary.source_structure_input_table == "malf_state_snapshot"
    assert structure_summary.source_timeframe == "D"
    assert structure_summary.materialized_snapshot_count == 2
    assert filter_summary.source_context_table == "malf_state_snapshot"
    assert filter_summary.source_timeframe == "D"
    assert filter_summary.materialized_snapshot_count == 2
    assert trigger_summary.materialized_trigger_count == 2
    assert alpha_summary.materialized_signal_count == 2

    structure_conn = duckdb.connect(str(structure_ledger_path(settings)), read_only=True)
    try:
        structure_rows = structure_conn.execute(
            """
            SELECT instrument, major_state, trend_direction, reversal_stage, current_hh_count, structure_progress_state
            FROM structure_snapshot
            ORDER BY instrument
            """
        ).fetchall()
    finally:
        structure_conn.close()

    filter_conn = duckdb.connect(str(filter_ledger_path(settings)), read_only=True)
    try:
        filter_rows = filter_conn.execute(
            """
            SELECT instrument, trigger_admissible, primary_blocking_condition
            FROM filter_snapshot
            ORDER BY instrument
            """
        ).fetchall()
    finally:
        filter_conn.close()

    alpha_conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        signal_rows = alpha_conn.execute(
            """
            SELECT instrument, formal_signal_status, trigger_admissible, major_state, reversal_stage
            FROM alpha_formal_signal_event
            ORDER BY instrument
            """
        ).fetchall()
    finally:
        alpha_conn.close()

    assert structure_rows == [
        ("000001.SZ", "牛顺", "up", "expand", 2, "advancing"),
        ("000002.SZ", "熊顺", "down", "expand", 0, "failed"),
    ]
    assert filter_rows == [
        ("000001.SZ", True, None),
        ("000002.SZ", True, None),
    ]
    assert signal_rows == [
        ("000001.SZ", "admitted", True, "牛顺", "expand"),
        ("000002.SZ", "admitted", True, "熊顺", "expand"),
    ]
