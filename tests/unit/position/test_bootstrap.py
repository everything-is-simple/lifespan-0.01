"""覆盖 `position` 最小账本 bootstrap。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.position import (
    DEFAULT_POSITION_POLICY_SEEDS,
    POSITION_LEDGER_TABLE_NAMES,
    bootstrap_position_ledger,
    position_ledger_path,
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


def test_bootstrap_position_ledger_creates_all_tables_and_policy_seeds(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    created_tables = bootstrap_position_ledger(settings=settings)
    db_path = position_ledger_path(settings)

    assert created_tables == POSITION_LEDGER_TABLE_NAMES
    assert db_path.exists()

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        existing_tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        assert set(POSITION_LEDGER_TABLE_NAMES).issubset(existing_tables)

        policy_ids = [
            row[0]
            for row in conn.execute(
                "SELECT policy_id FROM position_policy_registry ORDER BY policy_id"
            ).fetchall()
        ]
    finally:
        conn.close()

    assert policy_ids == sorted(seed.policy_id for seed in DEFAULT_POSITION_POLICY_SEEDS)


def test_bootstrap_position_ledger_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    bootstrap_position_ledger(settings=settings)
    bootstrap_position_ledger(settings=settings)

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        policy_count = conn.execute(
            "SELECT COUNT(*) FROM position_policy_registry"
        ).fetchone()[0]
    finally:
        conn.close()

    assert policy_count == len(DEFAULT_POSITION_POLICY_SEEDS)
