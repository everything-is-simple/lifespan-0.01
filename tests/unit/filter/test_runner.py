"""覆盖 `filter snapshot` 官方 producer。"""

from __future__ import annotations

from pathlib import Path

import duckdb

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
            VALUES
                ('ss-001', '000001.SZ', '2026-04-08', '2026-04-08', 'BULL_MAINSTREAM', 1, 4, 2, 0, 0.8, 0.7, FALSE, NULL, 'advancing', 'ctx-001', 'structure-snapshot-v1', 'run-a', 'run-a'),
                ('ss-002', '000002.SZ', '2026-04-08', '2026-04-08', 'BEAR_MAINSTREAM', 0, 4, 0, 1, 0.0, 0.0, TRUE, 'failed_extreme', 'failed', 'ctx-002', 'structure-snapshot-v1', 'run-a', 'run-a')
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
            CREATE TABLE pas_context_snapshot (
                entity_code TEXT NOT NULL,
                signal_date DATE NOT NULL
            )
            """
        )
        conn.execute("INSERT INTO pas_context_snapshot VALUES ('000001.SZ', '2026-04-08')")
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
            SELECT instrument, trigger_admissible, primary_blocking_condition
            FROM filter_snapshot
            ORDER BY instrument
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", 2)
    assert snapshot_rows == [
        ("000001.SZ", True, None),
        ("000002.SZ", False, "failed_extreme"),
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
                is_failed_extreme = TRUE,
                failure_type = 'failed_extreme',
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

    assert snapshot_row == (False, "failed_extreme", "filter-snapshot-test-002b")
