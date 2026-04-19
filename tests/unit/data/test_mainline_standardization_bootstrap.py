"""覆盖主线本地正式库标准化 bootstrap。"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data import run_mainline_local_ledger_standardization_bootstrap
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


def test_run_mainline_local_ledger_standardization_bootstrap_bootstraps_official_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    summary = run_mainline_local_ledger_standardization_bootstrap(
        settings=settings,
        ledgers=["raw_market", "market_base"],
        run_id="mainline-standardization-test-001",
    )

    assert summary.selected_ledger_count == 2
    assert summary.bootstrapped_ledger_count == 2
    assert summary.copied_ledger_count == 0
    assert summary.missing_source_count == 0
    assert settings.databases.raw_market.exists()
    assert settings.databases.market_base.exists()
    assert len(summary.inventory_rows) == 10
    assert summary.report_json_path.exists()
    assert summary.report_markdown_path.exists()

    payload = json.loads(summary.report_json_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "mainline-standardization-test-001"
    assert {row["ledger_name"] for row in payload["result_rows"]} == {"raw_market", "market_base"}


def test_run_mainline_local_ledger_standardization_bootstrap_copies_explicit_source_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    official_settings = default_settings(repo_root=repo_root)
    legacy_settings = _build_workspace(repo_root, "legacy")
    legacy_settings.ensure_directories()

    bootstrap_structure_snapshot_ledger(legacy_settings)
    conn = connect_structure_ledger(legacy_settings)
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
            VALUES (
                'legacy-structure-run-001',
                'legacy-runner',
                'v1',
                'completed',
                '2026-04-08',
                '2026-04-08',
                1,
                'malf_state_snapshot',
                'malf_state_snapshot',
                'structure-snapshot-v2',
                '{}'
            )
            """
        )
    finally:
        conn.close()

    summary = run_mainline_local_ledger_standardization_bootstrap(
        settings=official_settings,
        ledgers=["structure"],
        source_ledger_paths={"structure": legacy_settings.databases.structure},
        run_id="mainline-standardization-test-002",
    )

    assert summary.selected_ledger_count == 1
    assert summary.copied_ledger_count == 1
    assert summary.result_rows[0].migration_action == "copied_from_source"
    assert summary.result_rows[0].table_row_counts["structure_run"] == 1

    conn = duckdb.connect(str(official_settings.databases.structure), read_only=True)
    try:
        copied_row = conn.execute(
            """
            SELECT run_id, runner_name
            FROM structure_run
            """
        ).fetchone()
    finally:
        conn.close()

    assert copied_row == ("legacy-structure-run-001", "legacy-runner")
    assert "copied_from_source" in summary.report_markdown_path.read_text(encoding="utf-8")
