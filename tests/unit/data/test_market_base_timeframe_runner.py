"""覆盖 raw/base 周月线 timeframe runner。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.data import (
    market_base_timeframe_ledger_path,
    raw_market_timeframe_ledger_path,
    run_asset_market_base_build,
    run_tdx_asset_raw_ingest,
)
from tests.unit.data.market_base_test_support import (
    bootstrap_repo_root as _bootstrap_repo_root,
    clear_workspace_env as _clear_workspace_env,
    write_tdx_asset_file as _write_tdx_asset_file,
)


def test_weekly_raw_ingest_derives_from_day_raw_ledger(tmp_path: Path, monkeypatch) -> None:
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

    run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="stock",
        timeframe="day",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-stock-day-prep-001",
        limit=0,
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

    conn = duckdb.connect(str(raw_market_timeframe_ledger_path(settings, timeframe="week")), read_only=True)
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


def test_weekly_raw_ignores_direct_week_txt_and_keeps_day_raw_as_official_source(tmp_path: Path, monkeypatch) -> None:
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

    run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="stock",
        timeframe="day",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-stock-day-prep-002",
        limit=0,
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

    conn = duckdb.connect(str(raw_market_timeframe_ledger_path(settings, timeframe="week")), read_only=True)
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

    assert row == (date(2026, 4, 7), 10.0, 10.9, 9.9, 10.8, 300.0, 3000.0)


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
        timeframe="day",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-stock-day-prep-003",
        limit=0,
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

    assert day_summary.consumed_dirty_count == 1
    assert month_summary.consumed_dirty_count == 1

    day_conn = duckdb.connect(str(market_base_timeframe_ledger_path(settings, timeframe="day")), read_only=True)
    try:
        day_dirty_rows = day_conn.execute(
            """
            SELECT timeframe, dirty_status, last_consumed_run_id
            FROM base_dirty_instrument
            WHERE code = '600000.SH' AND adjust_method = 'backward'
            ORDER BY timeframe
            """
        ).fetchall()
    finally:
        day_conn.close()

    month_conn = duckdb.connect(str(market_base_timeframe_ledger_path(settings, timeframe="month")), read_only=True)
    try:
        rows = month_conn.execute(
            """
            SELECT code, timeframe, trade_date, open, high, low, close, volume, amount
            FROM stock_monthly_adjusted
            WHERE adjust_method = 'backward'
            ORDER BY trade_date
            """
        ).fetchall()
        month_dirty_rows = month_conn.execute(
            """
            SELECT timeframe, dirty_status, last_consumed_run_id
            FROM base_dirty_instrument
            WHERE code = '600000.SH' AND adjust_method = 'backward'
            ORDER BY timeframe
            """
        ).fetchall()
    finally:
        month_conn.close()

    assert rows == [
        ("600000.SH", "month", date(2026, 3, 31), 10.0, 10.9, 9.9, 10.8, 220.0, 2200.0),
        ("600000.SH", "month", date(2026, 4, 30), 10.9, 11.8, 10.7, 11.5, 270.0, 2700.0),
    ]
    assert day_dirty_rows == [("day", "consumed", "base-stock-day-incremental-001")]
    assert month_dirty_rows == [("month", "consumed", "base-stock-month-incremental-001")]


def test_index_weekly_raw_ingest_derives_from_day_raw_ledger(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    _write_tdx_asset_file(
        source_root,
        asset_type="index-day",
        folder_name="Backward-Adjusted",
        code="000300",
        exchange="SH",
        name="沪深300",
        rows=[
            ("2026/04/06", 4000.0, 4020.0, 3980.0, 4010.0, 1000, 10000),
            ("2026/04/07", 4015.0, 4030.0, 4005.0, 4025.0, 1200, 12000),
            ("2026/04/10", 4030.0, 4050.0, 4020.0, 4040.0, 1400, 14000),
            ("2026/04/13", 4045.0, 4060.0, 4035.0, 4055.0, 1500, 15000),
        ],
    )

    run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="index",
        timeframe="day",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-index-day-prep-001",
        limit=0,
    )
    summary = run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="index",
        timeframe="week",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-index-week-derived-001",
        limit=0,
    )

    assert summary.asset_type == "index"
    assert summary.timeframe == "week"
    assert summary.bar_inserted_count == 2

    conn = duckdb.connect(str(raw_market_timeframe_ledger_path(settings, timeframe="week")), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT code, timeframe, trade_date, open, high, low, close, volume, amount
            FROM index_weekly_bar
            WHERE adjust_method = 'backward'
            ORDER BY trade_date
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("000300.SH", "week", date(2026, 4, 10), 4000.0, 4050.0, 3980.0, 4040.0, 3600.0, 36000.0),
        ("000300.SH", "week", date(2026, 4, 13), 4045.0, 4060.0, 4035.0, 4055.0, 1500.0, 15000.0),
    ]


def test_block_monthly_base_runner_routes_to_split_ledgers(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    _write_tdx_asset_file(
        source_root,
        asset_type="block-day",
        folder_name="Backward-Adjusted",
        code="880001",
        exchange="SH",
        name="上证A股",
        rows=[
            ("2026/03/30", 1000.0, 1010.0, 995.0, 1005.0, 100, 1000),
            ("2026/03/31", 1006.0, 1015.0, 1002.0, 1010.0, 110, 1100),
            ("2026/04/01", 1012.0, 1020.0, 1008.0, 1018.0, 120, 1200),
            ("2026/04/30", 1019.0, 1035.0, 1015.0, 1030.0, 130, 1300),
        ],
    )

    run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="block",
        timeframe="day",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-block-day-prep-001",
        limit=0,
    )
    raw_summary = run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type="block",
        timeframe="month",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-block-month-derived-001",
        limit=0,
    )
    base_summary = run_asset_market_base_build(
        settings=settings,
        asset_type="block",
        timeframe="month",
        adjust_method="backward",
        build_mode="full",
        limit=0,
        run_id="base-block-month-derived-001",
    )

    assert Path(raw_summary.raw_market_path) == settings.databases.raw_market_month
    assert Path(base_summary.market_base_path) == settings.databases.market_base_month

    raw_month_conn = duckdb.connect(str(settings.databases.raw_market_month), read_only=True)
    try:
        raw_rows = raw_month_conn.execute(
            """
            SELECT code, timeframe, trade_date, open, high, low, close, volume, amount
            FROM block_monthly_bar
            WHERE adjust_method = 'backward'
            ORDER BY trade_date
            """
        ).fetchall()
    finally:
        raw_month_conn.close()

    base_month_conn = duckdb.connect(str(settings.databases.market_base_month), read_only=True)
    try:
        base_rows = base_month_conn.execute(
            """
            SELECT code, timeframe, trade_date, open, high, low, close, volume, amount
            FROM block_monthly_adjusted
            WHERE adjust_method = 'backward'
            ORDER BY trade_date
            """
        ).fetchall()
    finally:
        base_month_conn.close()

    expected_rows = [
        ("880001.SH", "month", date(2026, 3, 31), 1000.0, 1015.0, 995.0, 1010.0, 210.0, 2100.0),
        ("880001.SH", "month", date(2026, 4, 30), 1012.0, 1035.0, 1008.0, 1030.0, 250.0, 2500.0),
    ]
    assert raw_rows == expected_rows
    assert base_rows == expected_rows
