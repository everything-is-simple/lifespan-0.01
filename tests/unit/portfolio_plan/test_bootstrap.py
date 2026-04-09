"""Cover minimal `portfolio_plan` bootstrap contracts."""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.portfolio_plan import (
    PORTFOLIO_PLAN_LEDGER_TABLE_NAMES,
    bootstrap_portfolio_plan_ledger,
    portfolio_plan_ledger_path,
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


def test_bootstrap_portfolio_plan_ledger_creates_minimal_three_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    created_tables = bootstrap_portfolio_plan_ledger(settings=settings)
    db_path = portfolio_plan_ledger_path(settings)

    assert created_tables == PORTFOLIO_PLAN_LEDGER_TABLE_NAMES
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
    finally:
        conn.close()

    assert set(PORTFOLIO_PLAN_LEDGER_TABLE_NAMES).issubset(existing_tables)


def test_bootstrap_portfolio_plan_ledger_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    bootstrap_portfolio_plan_ledger(settings=settings)
    bootstrap_portfolio_plan_ledger(settings=settings)

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        table_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name IN ('portfolio_plan_run', 'portfolio_plan_snapshot', 'portfolio_plan_run_snapshot')
            """
        ).fetchone()[0]
    finally:
        conn.close()

    assert table_count == 3
