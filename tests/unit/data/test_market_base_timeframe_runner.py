"""覆盖 raw/base 周月线 timeframe runner。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.data import (
    market_base_ledger_path,
    raw_market_ledger_path,
    run_asset_market_base_build,
    run_tdx_asset_raw_ingest,
)
from tests.unit.data.market_base_test_support import (
    bootstrap_repo_root as _bootstrap_repo_root,
    clear_workspace_env as _clear_workspace_env,
    write_tdx_asset_file as _write_tdx_asset_file,
)


def test_weekly_raw_ingest_falls_back_to_day_source(tmp_path: Path, monkeypatch) -> None:
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
            ("2026/04/13", 11.10, 11.40, 10.90, 11.20, 400, 4000),
            ("2026/04/17", 11.30, 11.60, 11.00, 11.50, 500, 5000),
        ],
    )

    summary = run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="stock",
        timeframe="week",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-stock-week-fallback-001",
        limit=0,
    )

    assert summary.timeframe == "week"
    assert summary.bar_inserted_count == 2

    conn = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT code, timeframe, trade_date, open, high, low, close, volume, amount
            FROM stock_weekly_bar
            WHERE adjust_method = 'backward'
            ORDER BY trade_date
            """
        ).fetchall()
        run_row = conn.execute(
            """
            SELECT timeframe, candidate_file_count, inserted_bar_count, run_status
            FROM raw_ingest_run
            WHERE run_id = 'raw-stock-week-fallback-001'
            """
        ).fetchone()
    finally:
        conn.close()

    assert rows == [
        ("600000.SH", "week", date(2026, 4, 10), 10.0, 11.2, 9.9, 11.0, 600.0, 6000.0),
        ("600000.SH", "week", date(2026, 4, 17), 11.1, 11.6, 10.9, 11.5, 900.0, 9000.0),
    ]
    assert run_row == ("week", 1, 2, "completed")


def test_weekly_direct_source_precedes_day_fallback(tmp_path: Path, monkeypatch) -> None:
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
        ],
    )
    _write_tdx_asset_file(
        source_root,
        asset_type="stock-week",
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=[("2026/04/10", 9.50, 11.80, 9.20, 11.30, 999, 9999)],
    )

    summary = run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="stock",
        timeframe="week",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-stock-week-direct-001",
        limit=0,
    )

    assert summary.bar_inserted_count == 1

    conn = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT trade_date, open, high, low, close, volume, amount
            FROM stock_weekly_bar
            WHERE code = '600000.SH' AND adjust_method = 'backward'
            """
        ).fetchone()
    finally:
        conn.close()

    assert row == (date(2026, 4, 10), 9.5, 11.8, 9.2, 11.3, 999.0, 9999.0)


def test_monthly_dirty_queue_is_isolated_by_timeframe(tmp_path: Path, monkeypatch) -> None:
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

    run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="stock",
        timeframe="month",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-stock-month-fallback-001",
        limit=0,
    )

    day_summary = run_asset_market_base_build(
        settings=settings,
        asset_type="stock",
        timeframe="day",
        adjust_method="backward",
        build_mode="incremental",
        consume_dirty_only=True,
        run_id="base-stock-day-incremental-001",
    )
    month_summary = run_asset_market_base_build(
        settings=settings,
        asset_type="stock",
        timeframe="month",
        adjust_method="backward",
        build_mode="incremental",
        consume_dirty_only=True,
        run_id="base-stock-month-incremental-001",
    )

    assert day_summary.consumed_dirty_count == 0
    assert month_summary.consumed_dirty_count == 1

    conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT code, timeframe, trade_date, open, high, low, close, volume, amount
            FROM stock_monthly_adjusted
            WHERE adjust_method = 'backward'
            ORDER BY trade_date
            """
        ).fetchall()
        dirty_rows = conn.execute(
            """
            SELECT timeframe, dirty_status, last_consumed_run_id
            FROM base_dirty_instrument
            WHERE code = '600000.SH' AND adjust_method = 'backward'
            ORDER BY timeframe
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("600000.SH", "month", date(2026, 3, 31), 10.0, 10.9, 9.9, 10.8, 220.0, 2200.0),
        ("600000.SH", "month", date(2026, 4, 30), 10.9, 11.8, 10.7, 11.5, 270.0, 2700.0),
    ]
    assert dirty_rows == [("month", "consumed", "base-stock-month-incremental-001")]
