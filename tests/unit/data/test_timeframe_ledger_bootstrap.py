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
    run_asset_market_base_build,
    run_tdx_asset_raw_ingest,
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


def _write_tdx_asset_file(
    root: Path,
    *,
    asset_type: str,
    folder_name: str,
    code: str,
    exchange: str,
    name: str,
    rows: list[tuple[str, float, float, float, float, float, float]],
) -> None:
    folder = root / asset_type / folder_name
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{exchange}#{code}.txt"
    content_lines = [
        f"{code} {name} 日线 {'后复权' if folder_name == 'Backward-Adjusted' else '不复权'}",
        "      日期\t    开盘\t    最高\t    最低\t    收盘\t    成交量\t    成交额",
    ]
    for row in rows:
        content_lines.append(
            "\t".join(
                [
                    row[0],
                    f"{row[1]:.2f}",
                    f"{row[2]:.2f}",
                    f"{row[3]:.2f}",
                    f"{row[4]:.2f}",
                    f"{row[5]:.0f}",
                    f"{row[6]:.2f}",
                ]
            )
        )
    path.write_text("\n".join(content_lines) + "\n", encoding="gbk")


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


def test_weekly_raw_runner_routes_to_split_raw_market_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"
    _write_tdx_asset_file(
        source_root,
        asset_type="stock-day",
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=[
            ("2026/04/06", 10.00, 10.80, 9.90, 10.50, 100, 1000),
            ("2026/04/07", 10.60, 10.90, 10.10, 10.80, 200, 2000),
            ("2026/04/10", 10.90, 11.20, 10.70, 11.00, 300, 3000),
        ],
    )

    summary = run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="stock",
        timeframe="week",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-stock-week-split-001",
        limit=0,
    )

    assert Path(summary.raw_market_path) == settings.databases.raw_market_week
    assert settings.databases.raw_market_week.exists()

    week_conn = duckdb.connect(str(settings.databases.raw_market_week), read_only=True)
    try:
        week_count = week_conn.execute("SELECT COUNT(*) FROM stock_weekly_bar").fetchone()[0]
    finally:
        week_conn.close()

    assert week_count == 1
    if settings.databases.raw_market_day.exists():
        day_conn = duckdb.connect(str(settings.databases.raw_market_day), read_only=True)
        try:
            day_tables = {row[0] for row in day_conn.execute("SHOW TABLES").fetchall()}
        finally:
            day_conn.close()
        assert "stock_weekly_bar" not in day_tables


def test_monthly_base_runner_routes_to_split_ledgers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"
    _write_tdx_asset_file(
        source_root,
        asset_type="stock-day",
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=[
            ("2026/03/30", 10.00, 10.50, 9.90, 10.20, 100, 1000),
            ("2026/03/31", 10.30, 10.90, 10.10, 10.80, 120, 1200),
            ("2026/04/01", 10.90, 11.10, 10.70, 11.00, 130, 1300),
            ("2026/04/30", 11.10, 11.80, 10.90, 11.50, 140, 1400),
        ],
    )
    raw_summary = run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="stock",
        timeframe="month",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-stock-month-split-001",
        limit=0,
    )
    base_summary = run_asset_market_base_build(
        settings=settings,
        asset_type="stock",
        timeframe="month",
        adjust_method="backward",
        build_mode="full",
        limit=0,
        run_id="base-stock-month-split-001",
    )

    assert Path(raw_summary.raw_market_path) == settings.databases.raw_market_month
    assert Path(base_summary.raw_market_path) == settings.databases.raw_market_month
    assert Path(base_summary.market_base_path) == settings.databases.market_base_month

    raw_month_conn = duckdb.connect(str(settings.databases.raw_market_month), read_only=True)
    try:
        raw_month_count = raw_month_conn.execute("SELECT COUNT(*) FROM stock_monthly_bar").fetchone()[0]
    finally:
        raw_month_conn.close()
    base_month_conn = duckdb.connect(str(settings.databases.market_base_month), read_only=True)
    try:
        base_month_count = base_month_conn.execute("SELECT COUNT(*) FROM stock_monthly_adjusted").fetchone()[0]
    finally:
        base_month_conn.close()

    assert raw_month_count == 2
    assert base_month_count == 2
