"""覆盖主线本地正式库增量同步与断点续跑。"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data import (
    mainline_local_ledger_sync_control_path,
    run_mainline_local_ledger_incremental_sync,
)
from mlq.structure import bootstrap_structure_snapshot_ledger, connect_structure_ledger


def _clear_workspace_env(monkeypatch: pytest.MonkeyPatch) -> None:
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


def _build_workspace(repo_root: Path, root_name: str) -> WorkspaceRoots:
    base_root = repo_root.parent
    return WorkspaceRoots(
        repo_root=repo_root,
        data_root=base_root / f"{root_name}-data",
        temp_root=base_root / f"{root_name}-temp",
        report_root=base_root / f"{root_name}-report",
        validated_root=base_root / f"{root_name}-validated",
    )


def _insert_structure_run(settings: WorkspaceRoots, *, run_id: str, signal_end_date: str) -> None:
    bootstrap_structure_snapshot_ledger(settings)
    conn = connect_structure_ledger(settings)
    try:
        conn.execute(
            """
            INSERT INTO structure_run (
                run_id,
                runner_name,
                runner_version,
                run_status,
                signal_start_date,
                signal_end_date,
                bounded_instrument_count,
                source_context_table,
                source_structure_input_table,
                structure_contract_version,
                summary_json
            )
            VALUES (?, 'runner', 'v1', 'completed', ?, ?, 1, 'malf_state_snapshot', 'malf_state_snapshot', 'structure-snapshot-v2', '{}')
            """,
            [run_id, signal_end_date, signal_end_date],
        )
    finally:
        conn.close()


def test_mainline_incremental_sync_observes_official_ledger_in_place(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _insert_structure_run(settings, run_id="structure-run-001", signal_end_date="2026-04-08")

    summary = run_mainline_local_ledger_incremental_sync(
        settings=settings,
        ledgers=["structure"],
        run_id="mainline-sync-test-001",
    )

    assert summary.queue_enqueued_count == 1
    assert summary.observed_in_place_count == 1
    assert summary.checkpoint_upserted_count == 1
    assert summary.fresh_count == 1
    assert summary.sync_results[0].last_completed_bar_dt.isoformat() == "2026-04-08"

    control_conn = duckdb.connect(str(mainline_local_ledger_sync_control_path(settings)), read_only=True)
    try:
        checkpoint = control_conn.execute(
            """
            SELECT last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt
            FROM mainline_local_ledger_sync_checkpoint
            WHERE ledger_name = 'structure'
            """
        ).fetchone()
    finally:
        control_conn.close()

    assert tuple(value.isoformat() for value in checkpoint) == ("2026-04-08", "2026-04-08", "2026-04-08")


def test_mainline_incremental_sync_copies_updated_external_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    official_settings = default_settings(repo_root=repo_root)
    legacy_settings = _build_workspace(repo_root, "legacy")
    legacy_settings.ensure_directories()
    _insert_structure_run(legacy_settings, run_id="legacy-001", signal_end_date="2026-04-08")

    first_summary = run_mainline_local_ledger_incremental_sync(
        settings=official_settings,
        ledgers=["structure"],
        source_ledger_paths={"structure": legacy_settings.databases.structure},
        run_id="mainline-sync-test-002a",
    )
    _insert_structure_run(legacy_settings, run_id="legacy-002", signal_end_date="2026-04-10")
    second_summary = run_mainline_local_ledger_incremental_sync(
        settings=official_settings,
        ledgers=["structure"],
        source_ledger_paths={"structure": legacy_settings.databases.structure},
        run_id="mainline-sync-test-002b",
    )

    assert first_summary.copied_from_source_count == 1
    assert second_summary.copied_from_source_count == 1
    assert second_summary.sync_results[0].last_completed_bar_dt.isoformat() == "2026-04-10"

    conn = duckdb.connect(str(official_settings.databases.structure), read_only=True)
    try:
        row_count = conn.execute("SELECT COUNT(*) FROM structure_run").fetchone()[0]
    finally:
        conn.close()
    assert row_count == 2


def test_mainline_incremental_sync_honors_explicit_replay_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _insert_structure_run(settings, run_id="structure-run-003", signal_end_date="2026-04-09")
    run_mainline_local_ledger_incremental_sync(
        settings=settings,
        ledgers=["structure"],
        run_id="mainline-sync-test-003a",
    )

    summary = run_mainline_local_ledger_incremental_sync(
        settings=settings,
        ledgers=["structure"],
        replay_start_dates={"structure": "2026-04-01"},
        replay_confirm_until_dates={"structure": "2026-04-09"},
        run_id="mainline-sync-test-003b",
    )

    assert summary.queue_enqueued_count == 1
    assert summary.sync_results[0].dirty_reason == "source_replayed"

    control_conn = duckdb.connect(str(mainline_local_ledger_sync_control_path(settings)), read_only=True)
    try:
        checkpoint = control_conn.execute(
            """
            SELECT tail_start_bar_dt, tail_confirm_until_dt
            FROM mainline_local_ledger_sync_checkpoint
            WHERE ledger_name = 'structure'
            """
        ).fetchone()
    finally:
        control_conn.close()

    assert tuple(value.isoformat() for value in checkpoint) == ("2026-04-01", "2026-04-09")
