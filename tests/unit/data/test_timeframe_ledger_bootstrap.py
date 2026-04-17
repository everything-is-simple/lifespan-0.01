"""覆盖 raw/base 日周月分库的路径与 bootstrap 契约。"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from mlq.core.paths import default_settings
from mlq.data import (
    bootstrap_market_base_timeframe_ledger,
    bootstrap_raw_market_timeframe_ledger,
    market_base_ledger_path,
    market_base_timeframe_ledger_path,
    raw_market_ledger_path,
    raw_market_timeframe_ledger_path,
)


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


def test_timeframe_ledger_paths_keep_day_aliases_and_split_week_month(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    assert raw_market_ledger_path(settings) == settings.databases.raw_market_day
    assert market_base_ledger_path(settings) == settings.databases.market_base_day
    assert raw_market_timeframe_ledger_path(settings, timeframe="week") == settings.databases.raw_market_week
    assert raw_market_timeframe_ledger_path(settings, timeframe="month") == settings.databases.raw_market_month
    assert market_base_timeframe_ledger_path(settings, timeframe="week") == settings.databases.market_base_week
    assert market_base_timeframe_ledger_path(settings, timeframe="month") == settings.databases.market_base_month


def test_weekly_raw_bootstrap_uses_dedicated_db_and_scoped_tables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    ledger_path = bootstrap_raw_market_timeframe_ledger(settings, timeframe="week")

    assert ledger_path == settings.databases.raw_market_week
    assert ledger_path.exists()
    assert not settings.databases.raw_market_day.exists()

    conn = duckdb.connect(str(ledger_path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()

    assert "stock_weekly_bar" in tables
    assert "index_weekly_bar" in tables
    assert "block_weekly_bar" in tables
    assert "stock_file_registry" in tables
    assert "raw_ingest_run" in tables
    assert "stock_daily_bar" not in tables
    assert "raw_tdxquant_run" not in tables
    assert "tushare_objective_run" not in tables


def test_monthly_base_bootstrap_uses_dedicated_db_and_scoped_tables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    ledger_path = bootstrap_market_base_timeframe_ledger(settings, timeframe="month")

    assert ledger_path == settings.databases.market_base_month
    assert ledger_path.exists()
    assert not settings.databases.market_base_day.exists()

    conn = duckdb.connect(str(ledger_path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()

    assert "stock_monthly_adjusted" in tables
    assert "index_monthly_adjusted" in tables
    assert "block_monthly_adjusted" in tables
    assert "base_dirty_instrument" in tables
    assert "base_build_run" in tables
    assert "stock_daily_adjusted" not in tables
    assert "stock_weekly_adjusted" not in tables


def test_timeframe_ledger_path_rejects_unknown_timeframe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    with pytest.raises(ValueError, match="Unsupported timeframe"):
        raw_market_timeframe_ledger_path(settings, timeframe="quarter")
