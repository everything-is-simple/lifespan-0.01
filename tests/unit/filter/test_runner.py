"""覆盖 `filter snapshot` 官方 producer。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from mlq.core.paths import default_settings
from mlq.filter import filter_ledger_path, run_filter_snapshot_build
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
                    'ss-001', '000001.SZ', '2026-04-08', '2026-04-08', '\u725b\u987a', 'up', 'expand', 7, 2, 0,
                    '\u725b\u987a', 'up', 'expand', 7, 2, 0, 'ctx-d-001',
                    '\u725b\u987a', 'up', 'none', 3, 1, 0, 'ctx-w-001',
                    '\u725b\u9006', 'down', 'trigger', 1, 0, 1, 'ctx-m-001',
                    'advancing', 'confirmed', 'break-001', 'stats-001', 'high', 'elevated', 'ctx-001',
                    'structure-snapshot-v2', 'run-a', 'run-a'
                ),
                (
                    'ss-002', '000002.SZ', '2026-04-08', '2026-04-08', '\u718a\u987a', 'down', 'expand', 9, 0, 1,
                    '\u718a\u987a', 'down', 'expand', 9, 0, 1, 'ctx-d-002',
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
            ('state-001', 'stock', '000001.SZ', 'D', '2026-04-08', '\u725b\u987a', 'up', 'expand', 7, 2, 0)
            """
        )
    finally:
        conn.close()


def _seed_structure_checkpoints(structure_path: Path, rows: list[tuple[object, ...]]) -> None:
    conn = duckdb.connect(str(structure_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS structure_checkpoint (
                asset_type TEXT NOT NULL,
                code TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                last_completed_bar_dt DATE,
                tail_start_bar_dt DATE,
                tail_confirm_until_dt DATE,
                source_fingerprint TEXT NOT NULL,
                last_run_id TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO structure_checkpoint VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                row,
            )
    finally:
        conn.close()


def test_run_filter_snapshot_build_materializes_minimal_admission_layer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_structure_snapshots(settings)
    _seed_context_rows(settings.databases.malf)

    summary = run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        batch_size=1,
        run_id="filter-snapshot-test-001",
    )

    assert summary.candidate_structure_count == 2
    assert summary.materialized_snapshot_count == 2
    assert summary.inserted_count == 2
    assert summary.admissible_count == 1
    assert summary.blocked_count == 1
    assert summary.missing_context_count == 1

    conn = duckdb.connect(str(filter_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_status, bounded_instrument_count
            FROM filter_run
            WHERE run_id = 'filter-snapshot-test-001'
            """
        ).fetchone()
        snapshot_rows = conn.execute(
            """
            SELECT instrument, trigger_admissible, primary_blocking_condition, break_confirmation_status, exhaustion_risk_bucket
            FROM filter_snapshot
            ORDER BY instrument
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", 2)
    assert snapshot_rows == [
        ("000001.SZ", True, None, "confirmed", "high"),
        ("000002.SZ", False, "structure_progress_failed", None, None),
    ]


def test_run_filter_snapshot_build_marks_rematerialized_when_structure_turns_failed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_structure_snapshots(settings)
    _seed_context_rows(settings.databases.malf)

    first_summary = run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="filter-snapshot-test-002a",
    )
    assert first_summary.inserted_count == 2

    conn = connect_structure_ledger(settings)
    try:
        conn.execute(
            """
            UPDATE structure_snapshot
            SET
                structure_progress_state = 'failed',
                major_state = '\u718a\u987a',
                trend_direction = 'down',
                last_materialized_run_id = 'run-b'
            WHERE structure_snapshot_nk = 'ss-001'
            """
        )
    finally:
        conn.close()

    second_summary = run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="filter-snapshot-test-002b",
    )

    assert second_summary.rematerialized_count == 1

    conn = duckdb.connect(str(filter_ledger_path(settings)), read_only=True)
    try:
        snapshot_row = conn.execute(
            """
            SELECT trigger_admissible, primary_blocking_condition, last_materialized_run_id
            FROM filter_snapshot
            WHERE filter_snapshot_nk LIKE 'ss-001|%'
            """
        ).fetchone()
    finally:
        conn.close()

    assert snapshot_row == (False, "structure_progress_failed", "filter-snapshot-test-002b")


def test_run_filter_snapshot_build_copies_read_only_multi_timeframe_context_without_blocking(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_structure_snapshots(settings)
    _seed_context_rows(settings.databases.malf)

    first_summary = run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="filter-snapshot-test-003a",
    )
    assert first_summary.inserted_count == 2

    conn = duckdb.connect(str(filter_ledger_path(settings)), read_only=True)
    try:
        first_row = conn.execute(
            """
            SELECT
                trigger_admissible,
                daily_source_context_nk,
                weekly_major_state,
                monthly_major_state,
                admission_notes
            FROM filter_snapshot
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        conn.close()

    assert first_row == (
        True,
        "ctx-d-001",
        "\u725b\u987a",
        "\u725b\u9006",
        "canonical_context=\u725b\u987a/up/expand; read_only_context=W:\u725b\u987a/none;M:\u725b\u9006/trigger; break_confirmation=confirmed \u4ec5 sidecar \u63d0\u793a; exhaustion_risk=high",
    )

    conn = connect_structure_ledger(settings)
    try:
        conn.execute(
            """
            UPDATE structure_snapshot
            SET
                monthly_major_state = '\u718a\u9006',
                monthly_trend_direction = 'down',
                monthly_reversal_stage = 'trigger',
                monthly_source_context_nk = 'ctx-m-002',
                last_materialized_run_id = 'run-c'
            WHERE structure_snapshot_nk = 'ss-001'
            """
        )
    finally:
        conn.close()

    second_summary = run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="filter-snapshot-test-003b",
    )

    assert second_summary.rematerialized_count == 1
    assert second_summary.admissible_count == 1

    conn = duckdb.connect(str(filter_ledger_path(settings)), read_only=True)
    try:
        second_row = conn.execute(
            """
            SELECT
                trigger_admissible,
                primary_blocking_condition,
                monthly_major_state,
                monthly_source_context_nk,
                last_materialized_run_id
            FROM filter_snapshot
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        conn.close()

    assert second_row == (True, None, "\u718a\u9006", "ctx-m-002", "filter-snapshot-test-003b")


def test_run_filter_snapshot_build_uses_checkpoint_queue_by_default(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_structure_snapshots(settings)
    _seed_context_rows(settings.databases.malf)
    _seed_structure_checkpoints(
        settings.databases.structure,
        [
            ("stock", "000001.SZ", "D", "2026-04-08", "2026-04-08", "2026-04-08", "structure-source-a", "structure-run-a"),
            ("stock", "000002.SZ", "D", "2026-04-08", "2026-04-08", "2026-04-08", "structure-source-a", "structure-run-a"),
        ],
    )

    first_summary = run_filter_snapshot_build(
        settings=settings,
        run_id="filter-snapshot-test-queue-001a",
    )

    assert first_summary.execution_mode == "checkpoint_queue"
    assert first_summary.queue_claimed_count == 2
    assert first_summary.checkpoint_upserted_count == 2

    conn = duckdb.connect(str(settings.databases.structure))
    try:
        conn.execute(
            """
            UPDATE structure_snapshot
            SET
                monthly_major_state = '\u718a\u9006',
                monthly_trend_direction = 'down',
                monthly_reversal_stage = 'trigger',
                monthly_source_context_nk = 'ctx-m-queue-002',
                last_materialized_run_id = 'structure-run-b'
            WHERE structure_snapshot_nk = 'ss-001'
            """
        )
        conn.execute(
            """
            UPDATE structure_checkpoint
            SET source_fingerprint = 'structure-source-b', last_run_id = 'structure-run-b', updated_at = CURRENT_TIMESTAMP
            WHERE code = '000001.SZ'
            """
        )
    finally:
        conn.close()

    second_summary = run_filter_snapshot_build(
        settings=settings,
        run_id="filter-snapshot-test-queue-001b",
    )

    assert second_summary.execution_mode == "checkpoint_queue"
    assert second_summary.queue_claimed_count >= 1
    assert second_summary.rematerialized_count >= 1

    conn = duckdb.connect(str(filter_ledger_path(settings)), read_only=True)
    try:
        queue_row = conn.execute(
            """
            SELECT queue_status
            FROM filter_work_queue
            WHERE code = '000001.SZ'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ).fetchone()
        checkpoint_row = conn.execute(
            """
            SELECT last_run_id, tail_start_bar_dt, tail_confirm_until_dt
            FROM filter_checkpoint
            WHERE code = '000001.SZ' AND timeframe = 'D'
            """
        ).fetchone()
        snapshot_row = conn.execute(
            """
            SELECT monthly_major_state, last_materialized_run_id
            FROM filter_snapshot
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        conn.close()

    assert queue_row == ("completed",)
    assert checkpoint_row == ("filter-snapshot-test-queue-001b", date(2026, 4, 8), date(2026, 4, 8))
    assert snapshot_row == ("\u718a\u9006", "filter-snapshot-test-queue-001b")


def test_run_filter_snapshot_build_rejects_legacy_bridge_source_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_structure_snapshots(settings)
    _seed_context_rows(settings.databases.malf)

    with pytest.raises(ValueError, match="filter mainline only accepts official"):
        run_filter_snapshot_build(
            settings=settings,
            signal_start_date="2026-04-08",
            signal_end_date="2026-04-08",
            run_id="filter-snapshot-test-reject-001",
            source_structure_table="structure_candidate_snapshot",
        )
