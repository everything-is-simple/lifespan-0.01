"""覆盖 `structure snapshot` 官方 producer。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from mlq.core.paths import default_settings
from mlq.structure import (
    bootstrap_structure_snapshot_ledger,
    run_structure_snapshot_build,
    structure_ledger_path,
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


def _update_canonical_state_row(
    malf_path: Path,
    *,
    snapshot_nk: str,
    major_state: str,
    trend_direction: str,
    reversal_stage: str,
    wave_id: int,
    current_hh_count: int,
    current_ll_count: int,
) -> None:
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute(
            """
            UPDATE malf_state_snapshot
            SET
                major_state = ?,
                trend_direction = ?,
                reversal_stage = ?,
                wave_id = ?,
                current_hh_count = ?,
                current_ll_count = ?
            WHERE snapshot_nk = ?
            """,
            [
                major_state,
                trend_direction,
                reversal_stage,
                wave_id,
                current_hh_count,
                current_ll_count,
                snapshot_nk,
            ],
        )
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


def _seed_canonical_malf_checkpoints(
    malf_path: Path,
    rows: list[tuple[object, ...]],
) -> None:
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS malf_canonical_checkpoint (
                asset_type TEXT NOT NULL,
                code TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                last_completed_bar_dt DATE,
                tail_start_bar_dt DATE,
                tail_confirm_until_dt DATE,
                last_wave_id BIGINT NOT NULL DEFAULT 0,
                last_run_id TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO malf_canonical_checkpoint VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                row,
            )
    finally:
        conn.close()


def _downgrade_structure_legacy_official_db(settings) -> None:
    settings.databases.structure.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(settings.databases.structure))
    try:
        bootstrap_structure_snapshot_ledger(settings, connection=conn)
        conn.execute("DROP TABLE IF EXISTS structure_work_queue")
        conn.execute("DROP TABLE IF EXISTS structure_checkpoint")
        conn.execute("DROP TABLE IF EXISTS structure_snapshot")
        conn.execute(
            """
            CREATE TABLE structure_snapshot (
                structure_snapshot_nk TEXT PRIMARY KEY,
                instrument TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_date DATE NOT NULL,
                malf_context_4 TEXT NOT NULL,
                lifecycle_rank_high BIGINT NOT NULL,
                lifecycle_rank_total BIGINT NOT NULL,
                new_high_count BIGINT NOT NULL,
                new_low_count BIGINT NOT NULL,
                refresh_density DOUBLE NOT NULL,
                advancement_density DOUBLE NOT NULL,
                is_failed_extreme BOOLEAN NOT NULL,
                failure_type TEXT,
                structure_progress_state TEXT NOT NULL,
                source_context_nk TEXT NOT NULL,
                structure_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO structure_snapshot (
                structure_snapshot_nk,
                instrument,
                signal_date,
                asof_date,
                malf_context_4,
                lifecycle_rank_high,
                lifecycle_rank_total,
                new_high_count,
                new_low_count,
                refresh_density,
                advancement_density,
                is_failed_extreme,
                failure_type,
                structure_progress_state,
                source_context_nk,
                structure_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (
                'legacy-structure-001',
                'LEGACY.SZ',
                '2026-04-01',
                '2026-04-01',
                'legacy-context',
                1,
                2,
                1,
                0,
                0.5,
                0.5,
                FALSE,
                NULL,
                'advancing',
                'legacy-context-001',
                'structure-snapshot-v1',
                'legacy-run-a',
                'legacy-run-a'
            )
            """
        )
        conn.execute("DELETE FROM structure_run_snapshot")
        conn.execute(
            """
            INSERT INTO structure_run_snapshot (
                run_id,
                structure_snapshot_nk,
                materialization_action,
                structure_progress_state
            )
            VALUES ('legacy-run-a', 'legacy-structure-001', 'inserted', 'advancing')
            """
        )
    finally:
        conn.close()


def test_run_structure_snapshot_build_materializes_run_snapshot_from_canonical(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_canonical_malf_state_rows(
        settings.databases.malf,
        [
            ("snap-d-001", "000001.SZ", "D", "2026-04-08", "\u725b\u987a", "up", "none", 7, 2, 0),
            ("snap-d-002", "000002.SZ", "D", "2026-04-08", "\u718a\u987a", "down", "expand", 9, 0, 1),
        ],
    )

    summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        batch_size=1,
        run_id="structure-snapshot-test-001",
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
        run_snapshot_rows = conn.execute(
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
        ("000001.SZ", "\u725b\u987a", "up", "none", 2, 0, "advancing", "snap-d-001"),
        ("000002.SZ", "\u718a\u987a", "down", "expand", 0, 1, "failed", "snap-d-002"),
    ]
    assert len(run_snapshot_rows) == 2
    assert {row[1] for row in run_snapshot_rows} == {"inserted"}


def test_run_structure_snapshot_build_marks_rematerialized_when_canonical_state_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_canonical_malf_state_rows(
        settings.databases.malf,
        [
            ("snap-d-101", "000001.SZ", "D", "2026-04-08", "\u725b\u987a", "up", "none", 7, 1, 0),
        ],
    )

    first_summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-snapshot-test-002a",
    )
    assert first_summary.inserted_count == 1

    _update_canonical_state_row(
        settings.databases.malf,
        snapshot_nk="snap-d-101",
        major_state="\u725b\u9006",
        trend_direction="down",
        reversal_stage="trigger",
        wave_id=7,
        current_hh_count=0,
        current_ll_count=1,
    )
    second_summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-snapshot-test-002b",
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
        run_snapshot_row = conn.execute(
            """
            SELECT materialization_action
            FROM structure_run_snapshot
            WHERE run_id = 'structure-snapshot-test-002b'
            """
        ).fetchone()
    finally:
        conn.close()

    assert snapshot_row == ("failed", "\u725b\u9006", "structure-snapshot-test-002b")
    assert run_snapshot_row == ("rematerialized",)


def test_run_structure_snapshot_build_attaches_sidecar_fields_without_rewriting_progress(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_canonical_malf_state_rows(
        settings.databases.malf,
        [
            ("snap-d-301", "000001.SZ", "D", "2026-04-08", "\u725b\u987a", "up", "none", 7, 2, 0),
        ],
    )
    _seed_malf_sidecars(settings.databases.malf)

    summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="structure-snapshot-test-003",
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
            ("state-d-001", "000001.SZ", "D", "2026-04-08", "\u725b\u987a", "up", "none", 7, 2, 0),
            ("state-w-001", "000001.SZ", "W", "2026-04-04", "\u725b\u987a", "up", "expand", 3, 1, 0),
            ("state-m-001", "000001.SZ", "M", "2026-03-31", "\u725b\u9006", "down", "trigger", 1, 0, 1),
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
                major_state = '\u718a\u9006',
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
        "\u725b\u987a",
        "\u725b\u987a",
        "state-d-001",
        "\u725b\u987a",
        "expand",
        "state-w-001",
        "\u718a\u9006",
        "trigger",
        "state-m-001",
        "structure-snapshot-test-004b",
    )


def test_run_structure_snapshot_build_uses_checkpoint_queue_by_default(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_canonical_malf_state_rows(
        settings.databases.malf,
        [
            ("state-d-q1", "000001.SZ", "D", "2026-04-08", "\u725b\u987a", "up", "none", 7, 2, 0),
            ("state-w-q1", "000001.SZ", "W", "2026-04-04", "\u725b\u987a", "up", "expand", 3, 1, 0),
            ("state-m-q1", "000001.SZ", "M", "2026-03-31", "\u725b\u9006", "down", "trigger", 1, 0, 1),
        ],
    )
    _seed_canonical_malf_checkpoints(
        settings.databases.malf,
        [
            ("stock", "000001.SZ", "D", "2026-04-08", "2026-04-08", "2026-04-08", 7, "malf-run-a"),
            ("stock", "000001.SZ", "W", "2026-04-04", "2026-04-04", "2026-04-04", 3, "malf-run-a"),
            ("stock", "000001.SZ", "M", "2026-03-31", "2026-03-31", "2026-03-31", 1, "malf-run-a"),
        ],
    )

    first_summary = run_structure_snapshot_build(
        settings=settings,
        run_id="structure-snapshot-test-queue-001a",
    )

    assert first_summary.execution_mode == "checkpoint_queue"
    assert first_summary.queue_claimed_count == 1
    assert first_summary.checkpoint_upserted_count == 1

    conn = duckdb.connect(str(settings.databases.malf))
    try:
        conn.execute(
            """
            UPDATE malf_state_snapshot
            SET
                major_state = '\u718a\u9006',
                trend_direction = 'down',
                reversal_stage = 'trigger',
                current_hh_count = 0,
                current_ll_count = 2
            WHERE snapshot_nk = 'state-m-q1'
            """
        )
        conn.execute(
            """
            UPDATE malf_canonical_checkpoint
            SET last_run_id = 'malf-run-b', updated_at = CURRENT_TIMESTAMP
            WHERE code = '000001.SZ' AND timeframe = 'M'
            """
        )
    finally:
        conn.close()

    second_summary = run_structure_snapshot_build(
        settings=settings,
        run_id="structure-snapshot-test-queue-001b",
    )

    assert second_summary.execution_mode == "checkpoint_queue"
    assert second_summary.queue_claimed_count == 1
    assert second_summary.rematerialized_count == 1

    conn = duckdb.connect(str(structure_ledger_path(settings)), read_only=True)
    try:
        queue_row = conn.execute(
            """
            SELECT queue_status
            FROM structure_work_queue
            WHERE code = '000001.SZ'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ).fetchone()
        checkpoint_row = conn.execute(
            """
            SELECT last_run_id, tail_start_bar_dt, tail_confirm_until_dt
            FROM structure_checkpoint
            WHERE code = '000001.SZ' AND timeframe = 'D'
            """
        ).fetchone()
        snapshot_row = conn.execute(
            """
            SELECT monthly_major_state, last_materialized_run_id
            FROM structure_snapshot
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        conn.close()

    assert queue_row == ("completed",)
    assert checkpoint_row == ("structure-snapshot-test-queue-001b", date(2026, 3, 31), date(2026, 4, 8))
    assert snapshot_row == ("\u718a\u9006", "structure-snapshot-test-queue-001b")


def test_run_structure_snapshot_build_bootstraps_queue_tables_on_legacy_official_db(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_canonical_malf_state_rows(
        settings.databases.malf,
        [
            ("state-d-legacy-001", "CARD44.SZ", "D", "2026-04-08", "\u725b\u987a", "up", "none", 7, 2, 0),
            ("state-w-legacy-001", "CARD44.SZ", "W", "2026-04-04", "\u725b\u987a", "up", "expand", 3, 1, 0),
            ("state-m-legacy-001", "CARD44.SZ", "M", "2026-03-31", "\u725b\u9006", "down", "trigger", 1, 0, 1),
        ],
    )
    _seed_canonical_malf_checkpoints(
        settings.databases.malf,
        [
            ("stock", "CARD44.SZ", "D", "2026-04-08", "2026-04-08", "2026-04-08", 7, "malf-run-legacy"),
            ("stock", "CARD44.SZ", "W", "2026-04-04", "2026-04-04", "2026-04-04", 3, "malf-run-legacy"),
            ("stock", "CARD44.SZ", "M", "2026-03-31", "2026-03-31", "2026-03-31", 1, "malf-run-legacy"),
        ],
    )
    _downgrade_structure_legacy_official_db(settings)

    summary = run_structure_snapshot_build(
        settings=settings,
        run_id="structure-snapshot-test-legacy-001",
    )

    assert summary.execution_mode == "checkpoint_queue"
    assert summary.queue_enqueued_count == 1
    assert summary.queue_claimed_count == 1
    assert summary.checkpoint_upserted_count == 1

    conn = duckdb.connect(str(structure_ledger_path(settings)), read_only=True)
    try:
        queue_table = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'structure_work_queue'
            """
        ).fetchone()
        checkpoint_table = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'structure_checkpoint'
            """
        ).fetchone()
        checkpoint_row = conn.execute(
            """
            SELECT last_run_id
            FROM structure_checkpoint
            WHERE code = 'CARD44.SZ' AND timeframe = 'D'
            """
        ).fetchone()
        legacy_column = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = 'structure_snapshot'
              AND column_name = 'malf_context_4'
            """
        ).fetchone()
        snapshot_rows = conn.execute(
            """
            SELECT instrument
            FROM structure_snapshot
            ORDER BY instrument
            """
        ).fetchall()
        run_snapshot_rows = conn.execute(
            """
            SELECT run_id, structure_snapshot_nk
            FROM structure_run_snapshot
            ORDER BY run_id, structure_snapshot_nk
            """
        ).fetchall()
    finally:
        conn.close()

    assert queue_table == (1,)
    assert checkpoint_table == (1,)
    assert checkpoint_row == ("structure-snapshot-test-legacy-001",)
    assert legacy_column == (0,)
    assert snapshot_rows == [("CARD44.SZ",)]
    assert run_snapshot_rows == [
        (
            "structure-snapshot-test-legacy-001",
            "CARD44.SZ|2026-04-08|2026-04-08|state-d-legacy-001|structure-snapshot-v2",
        )
    ]


def test_run_structure_snapshot_build_rejects_legacy_bridge_source_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_canonical_malf_state_rows(
        settings.databases.malf,
        [
            ("state-d-reject-001", "000001.SZ", "D", "2026-04-08", "\u725b\u987a", "up", "none", 7, 1, 0),
        ],
    )

    with pytest.raises(ValueError, match="structure mainline only accepts canonical"):
        run_structure_snapshot_build(
            settings=settings,
            signal_start_date="2026-04-08",
            signal_end_date="2026-04-08",
            run_id="structure-snapshot-test-reject-001",
            source_context_table="pas_context_snapshot",
        )
