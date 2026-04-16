"""覆盖 market_base 分批建仓与 scoped full 清理。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.data import (
    market_base_ledger_path,
    raw_market_ledger_path,
    run_asset_market_base_build_batched,
    run_market_base_build,
    run_tdx_asset_raw_ingest_batched,
    run_tdx_stock_raw_ingest,
)
from tests.unit.data.market_base_test_support import (
    bootstrap_repo_root as _bootstrap_repo_root,
    clear_workspace_env as _clear_workspace_env,
    write_tdx_asset_file as _write_tdx_asset_file,
    write_tdx_stock_file as _write_tdx_stock_file,
)


def test_run_asset_market_base_build_batched_creates_child_runs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    for code, name, close in (
        ("600000", "浦发银行", 10.7),
        ("600001", "平安银行", 9.5),
        ("600002", "招商银行", 31.2),
    ):
        _write_tdx_stock_file(
            source_root,
            folder_name="Backward-Adjusted",
            code=code,
            exchange="SH",
            name=name,
            rows=[
                ("2026/04/08", close - 0.4, close, close - 0.5, close - 0.2, 1000, 10000),
                ("2026/04/09", close - 0.2, close + 0.2, close - 0.3, close, 1200, 12000),
            ],
        )

    run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_id="raw-batch-test-001",
        limit=0,
    )
    summary = run_asset_market_base_build_batched(
        settings=settings,
        asset_type="stock",
        adjust_method="backward",
        batch_size=1,
        run_id="base-batch-test-001",
    )

    assert summary["batch_count"] == 3
    assert summary["instrument_count"] == 3
    assert summary["source_row_count"] == 6
    assert [child["source_scope_kind"] for child in summary["child_runs"]] == [
        "instrument",
        "instrument",
        "instrument",
    ]

    conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        market_rows = conn.execute(
            """
            SELECT code, trade_date, close
            FROM stock_daily_adjusted
            WHERE adjust_method = 'backward'
            ORDER BY code, trade_date
            """
        ).fetchall()
        run_rows = conn.execute(
            """
            SELECT run_id, source_scope_kind, source_row_count, run_status
            FROM base_build_run
            WHERE run_id LIKE 'base-batch-test-001-b%'
            ORDER BY run_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert len(market_rows) == 6
    assert market_rows[0] == ("600000.SH", date(2026, 4, 8), 10.5)
    assert run_rows == [
        ("base-batch-test-001-b0001", "instrument", 2, "completed"),
        ("base-batch-test-001-b0002", "instrument", 2, "completed"),
        ("base-batch-test-001-b0003", "instrument", 2, "completed"),
    ]


def test_run_tdx_asset_raw_ingest_batched_creates_child_runs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    for code, name, close in (
        ("000300", "沪深300", 4010.0),
        ("000905", "中证500", 6120.0),
        ("399001", "深证成指", 9950.0),
    ):
        exchange = "SZ" if code.startswith("399") else "SH"
        _write_tdx_asset_file(
            source_root,
            asset_type="index-day",
            folder_name="Backward-Adjusted",
            code=code,
            exchange=exchange,
            name=name,
            rows=[("2026/04/09", close - 10.0, close + 10.0, close - 20.0, close, 1000, 10000)],
        )

    summary = run_tdx_asset_raw_ingest_batched(
        settings=settings,
        asset_type="index",
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        batch_size=1,
        run_id="raw-index-batch-test-001",
    )

    assert summary["batch_count"] == 3
    assert summary["candidate_file_count"] == 3
    assert summary["processed_file_count"] == 3
    assert [child["candidate_file_count"] for child in summary["child_runs"]] == [1, 1, 1]

    conn = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        raw_rows = conn.execute(
            """
            SELECT code, close
            FROM index_daily_bar
            WHERE adjust_method = 'backward'
            ORDER BY code
            """
        ).fetchall()
        run_rows = conn.execute(
            """
            SELECT run_id, asset_type, candidate_file_count, run_status
            FROM raw_ingest_run
            WHERE run_id LIKE 'raw-index-batch-test-001-b%'
            ORDER BY run_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert raw_rows == [
        ("000300.SH", 4010.0),
        ("000905.SH", 6120.0),
        ("399001.SZ", 9950.0),
    ]
    assert run_rows == [
        ("raw-index-batch-test-001-b0001", "index", 1, "completed"),
        ("raw-index-batch-test-001-b0002", "index", 1, "completed"),
        ("raw-index-batch-test-001-b0003", "index", 1, "completed"),
    ]


def test_instrument_scoped_full_deletes_only_current_scope(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    for code, name, close in (
        ("600000", "浦发银行", 10.7),
        ("600001", "平安银行", 9.5),
    ):
        _write_tdx_stock_file(
            source_root,
            folder_name="Backward-Adjusted",
            code=code,
            exchange="SH",
            name=name,
            rows=[
                ("2026/04/08", close - 0.4, close, close - 0.5, close - 0.2, 1000, 10000),
                ("2026/04/09", close - 0.2, close + 0.2, close - 0.3, close, 1200, 12000),
            ],
        )

    run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_id="raw-scope-test-001",
        limit=0,
    )
    run_market_base_build(
        settings=settings,
        adjust_method="backward",
        build_mode="full",
        limit=0,
        run_id="base-scope-test-001",
    )

    raw_conn = duckdb.connect(str(raw_market_ledger_path(settings)))
    try:
        raw_conn.execute(
            """
            DELETE FROM stock_daily_bar
            WHERE code = '600000.SH'
              AND trade_date = DATE '2026-04-08'
              AND adjust_method = 'backward'
            """
        )
    finally:
        raw_conn.close()

    run_market_base_build(
        settings=settings,
        adjust_method="backward",
        instruments=("600000.SH",),
        build_mode="full",
        limit=0,
        run_id="base-scope-test-002",
    )

    conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT code, trade_date
            FROM stock_daily_adjusted
            WHERE adjust_method = 'backward'
            ORDER BY code, trade_date
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("600000.SH", date(2026, 4, 9)),
        ("600001.SH", date(2026, 4, 8)),
        ("600001.SH", date(2026, 4, 9)),
    ]
