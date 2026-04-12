"""覆盖 `structure snapshot` 官方 producer。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
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


def _seed_malf_inputs(
    malf_path: Path,
    *,
    context_rows: list[tuple[object, ...]],
    structure_rows: list[tuple[object, ...]],
) -> None:
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
                lifecycle_rank_total BIGINT NOT NULL
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
        for row in context_rows:
            conn.execute("INSERT INTO pas_context_snapshot VALUES (?, ?, ?, ?, ?, ?, ?)", row)
        for row in structure_rows:
            conn.execute("INSERT INTO structure_candidate_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)
    finally:
        conn.close()


def _replace_structure_inputs(malf_path: Path, rows: list[tuple[object, ...]]) -> None:
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute("DELETE FROM structure_candidate_snapshot")
        for row in rows:
            conn.execute("INSERT INTO structure_candidate_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)
    finally:
        conn.close()


def _seed_malf_sidecars(malf_path: Path) -> None:
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute(
            """
            CREATE TABLE pivot_confirmed_break_ledger (
                break_event_nk TEXT,
                instrument TEXT,
                timeframe TEXT,
                trigger_bar_dt DATE,
                confirmation_status TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE same_timeframe_stats_snapshot (
                stats_snapshot_nk TEXT,
                instrument TEXT,
                signal_date DATE,
                asof_bar_dt DATE,
                exhaustion_risk_bucket TEXT,
                reversal_probability_bucket TEXT
            )
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


def _seed_canonical_malf_state_rows(
    malf_path: Path,
    rows: list[tuple[object, ...]],
) -> None:
    malf_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_state_snapshot (
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
        for row in rows:
            conn.execute(
                """
                INSERT INTO malf_state_snapshot VALUES (?, 'stock', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
    finally:
        conn.close()


def test_run_structure_snapshot_build_materializes_run_snapshot_and_bridge(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_malf_inputs(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-001", "BULL_MAINSTREAM", 1, 4),
            ("000002.SZ", "2026-04-08", "2026-04-08", "ctx-002", "BEAR_MAINSTREAM", 0, 4),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.7, 0.6, False, None),
            ("000002.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme"),
        ],
    )

    summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        batch_size=1,
        run_id="structure-snapshot-test-001",
        source_context_table="pas_context_snapshot",
        source_structure_input_table="structure_candidate_snapshot",
    )

    assert summary.candidate_input_count == 2
    assert summary.materialized_snapshot_count == 2
    assert summary.inserted_count == 2
    assert summary.advancing_count == 1
    assert summary.failed_count == 1

    conn = duckdb.connect(str(structure_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_status, bounded_instrument_count
            FROM structure_run
            WHERE run_id = 'structure-snapshot-test-001'
            """
        ).fetchone()
        snapshot_rows = conn.execute(
            """
            SELECT
                instrument,
                major_state,
                trend_direction,
                reversal_stage,
                current_hh_count,
                current_ll_count,
                structure_progress_state,
                source_context_nk
            FROM structure_snapshot
            ORDER BY instrument
            """
        ).fetchall()
        bridge_rows = conn.execute(
            """
            SELECT structure_snapshot_nk, materialization_action
            FROM structure_run_snapshot
            WHERE run_id = 'structure-snapshot-test-001'
            ORDER BY structure_snapshot_nk
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", 2)
    assert snapshot_rows == [
        ("000001.SZ", "牛顺", "up", "none", 1, 0, "advancing", "ctx-001"),
        ("000002.SZ", "熊顺", "down", "none", 0, 0, "failed", "ctx-002"),
    ]
    assert len(bridge_rows) == 2
    assert {row[1] for row in bridge_rows} == {"inserted"}


def test_run_structure_snapshot_build_marks_rematerialized_when_structure_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_malf_inputs(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-101", "BULL_MAINSTREAM", 1, 4),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 1, 0, 0.4, 0.3, False, None),
        ],
    )

    first_summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-snapshot-test-002a",
        source_context_table="pas_context_snapshot",
        source_structure_input_table="structure_candidate_snapshot",
    )
    assert first_summary.inserted_count == 1

    _replace_structure_inputs(
        settings.databases.malf,
        [("000001.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme")],
    )
    second_summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-snapshot-test-002b",
        source_context_table="pas_context_snapshot",
        source_structure_input_table="structure_candidate_snapshot",
    )

    assert second_summary.rematerialized_count == 1
    assert second_summary.failed_count == 1

    conn = duckdb.connect(str(structure_ledger_path(settings)), read_only=True)
    try:
        snapshot_row = conn.execute(
            """
            SELECT structure_progress_state, major_state, last_materialized_run_id
            FROM structure_snapshot
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
        bridge_row = conn.execute(
            """
            SELECT materialization_action
            FROM structure_run_snapshot
            WHERE run_id = 'structure-snapshot-test-002b'
            """
        ).fetchone()
    finally:
        conn.close()

    assert snapshot_row == ("failed", "牛顺", "structure-snapshot-test-002b")
    assert bridge_row == ("rematerialized",)


def test_run_structure_snapshot_build_attaches_sidecar_fields_without_rewriting_progress(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_malf_inputs(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-301", "BULL_MAINSTREAM", 1, 4),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.8, 0.7, False, None),
        ],
    )
    _seed_malf_sidecars(settings.databases.malf)

    summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-snapshot-test-003",
        source_context_table="pas_context_snapshot",
        source_structure_input_table="structure_candidate_snapshot",
    )

    assert summary.materialized_snapshot_count == 1

    conn = duckdb.connect(str(structure_ledger_path(settings)), read_only=True)
    try:
        snapshot_row = conn.execute(
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
        conn.close()

    assert snapshot_row == ("advancing", "confirmed", "break-001", "stats-001", "high", "elevated")


def test_run_structure_snapshot_build_materializes_read_only_weekly_monthly_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_canonical_malf_state_rows(
        settings.databases.malf,
        [
            ("state-d-001", "000001.SZ", "D", "2026-04-08", "牛顺", "up", "none", 7, 2, 0),
            ("state-w-001", "000001.SZ", "W", "2026-04-04", "牛顺", "up", "expand", 3, 1, 0),
            ("state-m-001", "000001.SZ", "M", "2026-03-31", "牛逆", "down", "trigger", 1, 0, 1),
        ],
    )

    first_summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-snapshot-test-004a",
    )
    assert first_summary.inserted_count == 1

    conn = duckdb.connect(str(settings.databases.malf))
    try:
        conn.execute(
            """
            UPDATE malf_state_snapshot
            SET
                major_state = '熊逆',
                trend_direction = 'down',
                reversal_stage = 'trigger',
                current_hh_count = 0,
                current_ll_count = 2
            WHERE snapshot_nk = 'state-m-001'
            """
        )
    finally:
        conn.close()

    second_summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-snapshot-test-004b",
    )

    assert second_summary.rematerialized_count == 1

    conn = duckdb.connect(str(structure_ledger_path(settings)), read_only=True)
    try:
        snapshot_row = conn.execute(
            """
            SELECT
                major_state,
                daily_major_state,
                daily_source_context_nk,
                weekly_major_state,
                weekly_reversal_stage,
                weekly_source_context_nk,
                monthly_major_state,
                monthly_reversal_stage,
                monthly_source_context_nk,
                last_materialized_run_id
            FROM structure_snapshot
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        conn.close()

    assert snapshot_row == (
        "牛顺",
        "牛顺",
        "state-d-001",
        "牛顺",
        "expand",
        "state-w-001",
        "熊逆",
        "trigger",
        "state-m-001",
        "structure-snapshot-test-004b",
    )
