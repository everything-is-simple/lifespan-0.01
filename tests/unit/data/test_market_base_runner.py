"""覆盖 `TDX -> raw_market -> market_base` 最小正式桥接。"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import duckdb
import pytest

from mlq.core.paths import default_settings
from mlq.data import (
    bootstrap_market_base_ledger,
    bootstrap_raw_market_ledger,
    market_base_ledger_path,
    mark_base_instrument_dirty,
    raw_market_ledger_path,
    run_asset_market_base_build,
    run_market_base_build,
    run_tdx_asset_raw_ingest,
    run_tdxquant_daily_raw_sync,
    run_tdx_stock_raw_ingest,
)
from tests.unit.data.market_base_test_support import (
    FakeTdxQuantClient,
    bootstrap_repo_root as _bootstrap_repo_root,
    clear_workspace_env as _clear_workspace_env,
    write_tdx_asset_file as _write_tdx_asset_file,
    write_tdx_stock_file as _write_tdx_stock_file,
)


def test_run_market_base_build_materializes_multiple_adjust_methods(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    stock_rows = [
        ("2026/04/08", 10.0, 10.5, 9.9, 10.4, 1000, 10400),
        ("2026/04/09", 10.4, 10.8, 10.2, 10.7, 1200, 12840),
    ]
    _write_tdx_stock_file(
        source_root,
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=stock_rows,
    )
    _write_tdx_stock_file(
        source_root,
        folder_name="Non-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=[(row[0], row[1] - 1.0, row[2] - 1.0, row[3] - 1.0, row[4] - 1.0, row[5], row[6] - 1000) for row in stock_rows],
    )

    run_tdx_stock_raw_ingest(settings=settings, source_root=source_root, adjust_method="backward", run_id="raw-test-002a")
    run_tdx_stock_raw_ingest(settings=settings, source_root=source_root, adjust_method="none", run_id="raw-test-002b")

    backward_summary = run_market_base_build(settings=settings, adjust_method="backward", run_id="base-test-002a")
    none_summary = run_market_base_build(settings=settings, adjust_method="none", run_id="base-test-002b")
    rerun_summary = run_market_base_build(settings=settings, adjust_method="none", run_id="base-test-002c")

    assert backward_summary.inserted_count == 2
    assert none_summary.inserted_count == 2
    assert rerun_summary.reused_count == 2

    conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT code, trade_date, adjust_method, close
            FROM stock_daily_adjusted
            ORDER BY adjust_method, trade_date
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("600000.SH", date(2026, 4, 8), "backward", 10.4),
        ("600000.SH", date(2026, 4, 9), "backward", 10.7),
        ("600000.SH", date(2026, 4, 8), "none", 9.4),
        ("600000.SH", date(2026, 4, 9), "none", 9.7),
    ]


def test_run_market_base_build_incremental_consumes_dirty_queue(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    _write_tdx_stock_file(
        source_root,
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=[
            ("2026/04/08", 10.0, 10.5, 9.9, 10.4, 1000, 10400),
            ("2026/04/09", 10.4, 10.8, 10.2, 10.7, 1200, 12840),
        ],
    )
    _write_tdx_stock_file(
        source_root,
        folder_name="Backward-Adjusted",
        code="600001",
        exchange="SH",
        name="平安银行",
        rows=[
            ("2026/04/08", 9.0, 9.3, 8.9, 9.2, 900, 8280),
            ("2026/04/09", 9.2, 9.6, 9.1, 9.5, 1000, 9500),
        ],
    )

    run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_id="raw-test-003a",
    )
    run_market_base_build(
        settings=settings,
        adjust_method="backward",
        build_mode="full",
        run_id="base-test-003a",
    )

    _write_tdx_stock_file(
        source_root,
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=[
            ("2026/04/08", 10.0, 10.5, 9.9, 10.4, 1000, 10400),
            ("2026/04/09", 10.4, 10.8, 10.2, 11.2, 1200, 13440),
        ],
    )
    changed_path = source_root / "stock" / "Backward-Adjusted" / "SH#600000.txt"
    changed_stat = changed_path.stat()
    os.utime(changed_path, (changed_stat.st_atime + 2, changed_stat.st_mtime + 2))
    run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_id="raw-test-003b",
    )
    dirty_nk = mark_base_instrument_dirty(
        settings=settings,
        code="600000.SH",
        adjust_method="backward",
        dirty_reason="raw_changed",
        source_run_id="raw-test-003b",
    )

    summary = run_market_base_build(
        settings=settings,
        adjust_method="backward",
        build_mode="incremental",
        run_id="base-test-003b",
    )

    assert summary.build_mode == "incremental"
    assert summary.source_scope_kind == "dirty_queue"
    assert summary.consumed_dirty_count == 1
    assert summary.rematerialized_count == 1

    conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        price_rows = conn.execute(
            """
            SELECT code, trade_date, close
            FROM stock_daily_adjusted
            ORDER BY code, trade_date
            """
        ).fetchall()
        build_run_row = conn.execute(
            """
            SELECT build_mode, source_scope_kind, source_row_count, rematerialized_count, consumed_dirty_count, run_status
            FROM base_build_run
            WHERE run_id = 'base-test-003b'
            """
        ).fetchone()
        scope_rows = conn.execute(
            """
            SELECT scope_type, scope_value
            FROM base_build_scope
            WHERE run_id = 'base-test-003b'
            """
        ).fetchall()
        action_rows = conn.execute(
            """
            SELECT code, adjust_method, action, row_count
            FROM base_build_action
            WHERE run_id = 'base-test-003b'
            """
        ).fetchall()
        dirty_row = conn.execute(
            """
            SELECT dirty_status, last_consumed_run_id
            FROM base_dirty_instrument
            WHERE dirty_nk = ?
            """,
            [dirty_nk],
        ).fetchone()
    finally:
        conn.close()

    assert price_rows == [
        ("600000.SH", date(2026, 4, 8), 10.4),
        ("600000.SH", date(2026, 4, 9), 11.2),
        ("600001.SH", date(2026, 4, 8), 9.2),
        ("600001.SH", date(2026, 4, 9), 9.5),
    ]
    assert build_run_row == ("incremental", "dirty_queue", 2, 1, 1, "completed")
    assert len(scope_rows) == 1
    assert scope_rows[0][0] == "dirty_queue"
    assert "600000.SH" in scope_rows[0][1]
    assert action_rows == [("600000.SH", "backward", "rematerialized", 2)]
    assert dirty_row == ("consumed", "base-test-003b")


def test_run_market_base_build_dirty_queue_ignores_global_stage_limit(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    start_date = date(2023, 1, 1)
    original_rows: list[tuple[str, float, float, float, float, float, float]] = []
    for offset in range(1005):
        trade_date = start_date.fromordinal(start_date.toordinal() + offset).strftime("%Y/%m/%d")
        close_price = 10.0 + offset / 100.0
        original_rows.append(
            (
                trade_date,
                close_price - 0.1,
                close_price + 0.1,
                close_price - 0.2,
                close_price,
                1000 + offset,
                (1000 + offset) * close_price,
            )
        )

    _write_tdx_stock_file(
        source_root,
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=original_rows,
    )
    run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_id="raw-test-004a",
    )
    run_market_base_build(
        settings=settings,
        adjust_method="backward",
        build_mode="full",
        run_id="base-test-004a",
        limit=5000,
    )

    changed_rows = list(original_rows)
    last_row = list(changed_rows[-1])
    last_row[4] = float(last_row[4]) + 5.0
    last_row[6] = float(last_row[6]) + 5000.0
    changed_rows[-1] = tuple(last_row)  # type: ignore[assignment]
    _write_tdx_stock_file(
        source_root,
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=changed_rows,
    )
    changed_path = source_root / "stock" / "Backward-Adjusted" / "SH#600000.txt"
    changed_stat = changed_path.stat()
    os.utime(changed_path, (changed_stat.st_atime + 2, changed_stat.st_mtime + 2))
    run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_id="raw-test-004b",
    )
    dirty_nk = mark_base_instrument_dirty(
        settings=settings,
        code="600000.SH",
        adjust_method="backward",
        dirty_reason="raw_changed",
        source_run_id="raw-test-004b",
    )

    summary = run_market_base_build(
        settings=settings,
        adjust_method="backward",
        build_mode="incremental",
        run_id="base-test-004b",
    )

    assert summary.source_scope_kind == "dirty_queue"
    assert summary.source_row_count == 1005
    assert summary.rematerialized_count == 1
    assert summary.consumed_dirty_count == 1

    conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        latest_row = conn.execute(
            """
            SELECT close, last_materialized_run_id
            FROM stock_daily_adjusted
            WHERE code = '600000.SH' AND trade_date = ?
            """,
            [date.fromisoformat(changed_rows[-1][0].replace("/", "-"))],
        ).fetchone()
        dirty_row = conn.execute(
            """
            SELECT dirty_status, last_consumed_run_id
            FROM base_dirty_instrument
            WHERE dirty_nk = ?
            """,
            [dirty_nk],
        ).fetchone()
    finally:
        conn.close()

    assert latest_row == (float(last_row[4]), "base-test-004b")
    assert dirty_row == ("consumed", "base-test-004b")


def test_bootstrap_raw_and_market_base_cleanup_duplicates_and_enforce_constraints(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    settings.ensure_directories()

    raw_conn = duckdb.connect(str(raw_market_ledger_path(settings)))
    try:
        raw_conn.execute(
            """
            CREATE TABLE stock_file_registry (
                file_nk TEXT,
                asset_type TEXT,
                adjust_method TEXT,
                code TEXT,
                name TEXT,
                source_path TEXT,
                source_size_bytes BIGINT,
                source_mtime_utc TIMESTAMP,
                source_line_count BIGINT,
                source_header TEXT,
                last_ingested_run_id TEXT,
                last_ingested_at TIMESTAMP
            )
            """
        )
        raw_conn.execute(
            """
            INSERT INTO stock_file_registry VALUES
            (NULL, 'stock', 'backward', '600000.SH', '浦发银行', 'a', 1, TIMESTAMP '2026-04-10 10:00:00', 2, 'h', 'r0', TIMESTAMP '2026-04-10 10:00:00'),
            ('stock|backward|600000.SH|浦发银行|a', 'stock', 'backward', '600000.SH', '浦发银行', 'a', 1, TIMESTAMP '2026-04-10 10:00:00', 2, 'h', 'r1', TIMESTAMP '2026-04-10 10:00:00'),
            ('stock|backward|600000.SH|浦发银行|a', 'stock', 'backward', '600000.SH', '浦发银行', 'a', 1, TIMESTAMP '2026-04-10 11:00:00', 2, 'h', 'r2', TIMESTAMP '2026-04-10 11:00:00')
            """
        )
        raw_conn.execute(
            """
            CREATE TABLE stock_daily_bar (
                bar_nk TEXT,
                source_file_nk TEXT,
                asset_type TEXT,
                code TEXT,
                name TEXT,
                trade_date DATE,
                adjust_method TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                source_path TEXT,
                source_mtime_utc TIMESTAMP,
                first_seen_run_id TEXT,
                last_ingested_run_id TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        raw_conn.execute(
            """
            INSERT INTO stock_daily_bar VALUES
            (NULL, 'file-a', 'stock', '600000.SH', '浦发银行', DATE '2026-04-09', 'backward', 10, 11, 9, 10.7, 1000, 12840, 'a', TIMESTAMP '2026-04-10 10:00:00', 'r1', 'r1', TIMESTAMP '2026-04-10 10:00:00', TIMESTAMP '2026-04-10 10:00:00'),
            ('600000.SH|2026-04-09|backward', 'file-a', 'stock', '600000.SH', '浦发银行', DATE '2026-04-09', 'backward', 10, 11, 9, 10.7, 1000, 12840, 'a', TIMESTAMP '2026-04-10 10:00:00', 'r1', 'r1', TIMESTAMP '2026-04-10 10:00:00', TIMESTAMP '2026-04-10 10:00:00'),
            ('600000.SH|2026-04-09|backward', 'file-a', 'stock', '600000.SH', '浦发银行', DATE '2026-04-09', 'backward', 10, 11, 9, 10.8, 1000, 12960, 'a', TIMESTAMP '2026-04-10 11:00:00', 'r1', 'r2', TIMESTAMP '2026-04-10 10:00:00', TIMESTAMP '2026-04-10 11:00:00')
            """
        )
    finally:
        raw_conn.close()

    base_conn = duckdb.connect(str(market_base_ledger_path(settings)))
    try:
        base_conn.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                daily_bar_nk TEXT,
                code TEXT,
                name TEXT,
                trade_date DATE,
                adjust_method TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                source_bar_nk TEXT,
                first_seen_run_id TEXT,
                last_materialized_run_id TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        base_conn.execute(
            """
            INSERT INTO stock_daily_adjusted VALUES
            (NULL, '600000.SH', '浦发银行', DATE '2026-04-09', 'backward', 10, 11, 9, 10.7, 1000, 12840, 'bar-a', 'r1', 'r1', TIMESTAMP '2026-04-10 10:00:00', TIMESTAMP '2026-04-10 10:00:00'),
            ('600000.SH|2026-04-09|backward', '600000.SH', '浦发银行', DATE '2026-04-09', 'backward', 10, 11, 9, 10.7, 1000, 12840, 'bar-a', 'r1', 'r1', TIMESTAMP '2026-04-10 10:00:00', TIMESTAMP '2026-04-10 10:00:00'),
            ('600000.SH|2026-04-09|backward', '600000.SH', '浦发银行', DATE '2026-04-09', 'backward', 10, 11, 9, 10.8, 1000, 12960, 'bar-a', 'r1', 'r2', TIMESTAMP '2026-04-10 10:00:00', TIMESTAMP '2026-04-10 11:00:00')
            """
        )
        base_conn.execute(
            """
            CREATE TABLE base_dirty_instrument (
                dirty_nk TEXT,
                code TEXT,
                adjust_method TEXT,
                dirty_reason TEXT,
                source_run_id TEXT,
                source_file_nk TEXT,
                dirty_status TEXT,
                first_marked_at TIMESTAMP,
                last_marked_at TIMESTAMP,
                last_consumed_run_id TEXT
            )
            """
        )
        base_conn.execute(
            """
            INSERT INTO base_dirty_instrument VALUES
            (NULL, '600000.SH', 'backward', 'raw_inserted', 'r1', 'file-a', 'pending', TIMESTAMP '2026-04-10 10:00:00', TIMESTAMP '2026-04-10 10:00:00', NULL),
            ('600000.SH|backward', '600000.SH', 'backward', 'raw_inserted', 'r1', 'file-a', 'pending', TIMESTAMP '2026-04-10 10:00:00', TIMESTAMP '2026-04-10 10:00:00', NULL),
            ('600000.SH|backward', '600000.SH', 'backward', 'raw_rematerialized', 'r2', 'file-a', 'pending', TIMESTAMP '2026-04-10 10:00:00', TIMESTAMP '2026-04-10 11:00:00', NULL)
            """
        )
    finally:
        base_conn.close()

    bootstrap_raw_market_ledger(settings)
    bootstrap_market_base_ledger(settings)

    raw_conn = duckdb.connect(str(raw_market_ledger_path(settings)))
    try:
        raw_registry_rows = raw_conn.execute("SELECT file_nk, last_ingested_run_id FROM stock_file_registry").fetchall()
        raw_bar_rows = raw_conn.execute("SELECT bar_nk, close, last_ingested_run_id FROM stock_daily_bar").fetchall()
        with pytest.raises(duckdb.ConstraintException):
            raw_conn.execute(
                """
                INSERT INTO stock_file_registry (
                    file_nk, asset_type, adjust_method, code, name, source_path, source_size_bytes,
                    source_mtime_utc, source_line_count, source_header, last_ingested_run_id, last_ingested_at
                )
                VALUES ('stock|backward|600000.SH|浦发银行|a', 'stock', 'backward', '600000.SH', '浦发银行', 'a', 1, TIMESTAMP '2026-04-10 12:00:00', 2, 'h', 'r3', CURRENT_TIMESTAMP)
                """
            )
    finally:
        raw_conn.close()

    base_conn = duckdb.connect(str(market_base_ledger_path(settings)))
    try:
        base_daily_rows = base_conn.execute("SELECT code, close, last_materialized_run_id FROM stock_daily_adjusted").fetchall()
        dirty_rows = base_conn.execute("SELECT dirty_nk, dirty_reason, source_run_id FROM base_dirty_instrument").fetchall()
        with pytest.raises(duckdb.ConstraintException):
            base_conn.execute(
                """
                INSERT INTO stock_daily_adjusted (
                    daily_bar_nk, code, name, trade_date, adjust_method, open, high, low, close,
                    volume, amount, source_bar_nk, first_seen_run_id, last_materialized_run_id, created_at, updated_at
                )
                VALUES ('another', '600000.SH', '浦发银行', DATE '2026-04-09', 'backward', 10, 11, 9, 10.9, 1000, 13000, 'bar-b', 'r1', 'r3', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
            )
    finally:
        base_conn.close()

    assert raw_registry_rows == [("stock|backward|600000.SH|浦发银行|a", "r2")]
    assert raw_bar_rows == [("600000.SH|2026-04-09|backward", 10.8, "r2")]
    assert base_daily_rows == [("600000.SH", 10.8, "r2")]
    assert dirty_rows == [("600000.SH|backward", "raw_rematerialized", "r2")]


@pytest.mark.parametrize(
    ("asset_type", "code", "name", "registry_table", "daily_table"),
    [
        ("index", "000300", "沪深300", "index_file_registry", "index_daily_bar"),
        ("block", "881002", "煤炭开采", "block_file_registry", "block_daily_bar"),
    ],
)
def test_run_tdx_asset_raw_ingest_supports_index_and_block_incremental(
    tmp_path: Path,
    monkeypatch,
    asset_type: str,
    code: str,
    name: str,
    registry_table: str,
    daily_table: str,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    _write_tdx_asset_file(
        source_root,
        asset_type=asset_type,
        folder_name="Backward-Adjusted",
        code=code,
        exchange="SH",
        name=name,
        rows=[
            ("2026/04/08", 10.0, 10.5, 9.9, 10.4, 1000, 10400),
            ("2026/04/09", 10.4, 10.8, 10.2, 10.7, 1200, 12840),
        ],
    )

    first_summary = run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type=asset_type,
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id=f"raw-{asset_type}-test-001a",
    )
    second_summary = run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type=asset_type,
        source_root=source_root,
        adjust_method="backward",
        run_mode="incremental",
        run_id=f"raw-{asset_type}-test-001b",
    )

    assert first_summary.asset_type == asset_type
    assert first_summary.bar_inserted_count == 2
    assert second_summary.skipped_unchanged_file_count == 1

    raw_conn = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        registry_rows = raw_conn.execute(
            f"SELECT asset_type, code, last_ingested_run_id FROM {registry_table}"
        ).fetchall()
        daily_rows = raw_conn.execute(
            f"SELECT asset_type, code, trade_date, close FROM {daily_table} ORDER BY trade_date"
        ).fetchall()
        ingest_run_rows = raw_conn.execute(
            """
            SELECT asset_type, run_id, run_mode, run_status
            FROM raw_ingest_run
            WHERE run_id IN (?, ?)
            ORDER BY run_id
            """,
            [f"raw-{asset_type}-test-001a", f"raw-{asset_type}-test-001b"],
        ).fetchall()
        ingest_file_rows = raw_conn.execute(
            """
            SELECT asset_type, run_id, action
            FROM raw_ingest_file
            WHERE run_id IN (?, ?)
            ORDER BY run_id
            """,
            [f"raw-{asset_type}-test-001a", f"raw-{asset_type}-test-001b"],
        ).fetchall()
    finally:
        raw_conn.close()

    base_conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        dirty_rows = base_conn.execute(
            """
            SELECT dirty_nk, asset_type, code, adjust_method, dirty_status
            FROM base_dirty_instrument
            """
        ).fetchall()
    finally:
        base_conn.close()

    assert registry_rows == [(asset_type, f"{code}.SH", f"raw-{asset_type}-test-001a")]
    assert daily_rows == [
        (asset_type, f"{code}.SH", date(2026, 4, 8), 10.4),
        (asset_type, f"{code}.SH", date(2026, 4, 9), 10.7),
    ]
    assert ingest_run_rows == [
        (asset_type, f"raw-{asset_type}-test-001a", "full", "completed"),
        (asset_type, f"raw-{asset_type}-test-001b", "incremental", "completed"),
    ]
    assert ingest_file_rows == [
        (asset_type, f"raw-{asset_type}-test-001a", "inserted"),
        (asset_type, f"raw-{asset_type}-test-001b", "skipped_unchanged"),
    ]
    assert dirty_rows == [(f"{asset_type}|{code}.SH|backward", asset_type, f"{code}.SH", "backward", "pending")]


@pytest.mark.parametrize(
    ("asset_type", "code", "name", "market_table"),
    [
        ("index", "000300", "沪深300", "index_daily_adjusted"),
        ("block", "881002", "煤炭开采", "block_daily_adjusted"),
    ],
)
def test_run_asset_market_base_build_materializes_index_and_block(
    tmp_path: Path,
    monkeypatch,
    asset_type: str,
    code: str,
    name: str,
    market_table: str,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    _write_tdx_asset_file(
        source_root,
        asset_type=asset_type,
        folder_name="Backward-Adjusted",
        code=code,
        exchange="SH",
        name=name,
        rows=[
            ("2026/04/08", 10.0, 10.5, 9.9, 10.4, 1000, 10400),
            ("2026/04/09", 10.4, 10.8, 10.2, 10.7, 1200, 12840),
        ],
    )
    run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type=asset_type,
        source_root=source_root,
        adjust_method="backward",
        run_id=f"raw-{asset_type}-test-002a",
    )
    first_summary = run_asset_market_base_build(
        settings=settings,
        asset_type=asset_type,
        adjust_method="backward",
        build_mode="full",
        run_id=f"base-{asset_type}-test-002a",
    )

    _write_tdx_asset_file(
        source_root,
        asset_type=asset_type,
        folder_name="Backward-Adjusted",
        code=code,
        exchange="SH",
        name=name,
        rows=[
            ("2026/04/08", 10.0, 10.5, 9.9, 10.4, 1000, 10400),
            ("2026/04/09", 10.4, 10.8, 10.2, 11.2, 1200, 13440),
        ],
    )
    changed_path = source_root / asset_type / "Backward-Adjusted" / f"SH#{code}.txt"
    changed_stat = changed_path.stat()
    os.utime(changed_path, (changed_stat.st_atime + 2, changed_stat.st_mtime + 2))
    run_tdx_asset_raw_ingest(
        settings=settings,
        asset_type=asset_type,
        source_root=source_root,
        adjust_method="backward",
        run_id=f"raw-{asset_type}-test-002b",
    )
    second_summary = run_asset_market_base_build(
        settings=settings,
        asset_type=asset_type,
        adjust_method="backward",
        build_mode="incremental",
        run_id=f"base-{asset_type}-test-002b",
    )

    assert first_summary.asset_type == asset_type
    assert first_summary.inserted_count == 2
    assert second_summary.source_scope_kind == "dirty_queue"
    assert second_summary.rematerialized_count == 1
    assert second_summary.consumed_dirty_count == 1

    base_conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        market_rows = base_conn.execute(
            f"SELECT code, trade_date, close, last_materialized_run_id FROM {market_table} ORDER BY trade_date"
        ).fetchall()
        build_run_rows = base_conn.execute(
            """
            SELECT asset_type, run_id, source_scope_kind, rematerialized_count, consumed_dirty_count, run_status
            FROM base_build_run
            WHERE run_id IN (?, ?)
            ORDER BY run_id
            """,
            [f"base-{asset_type}-test-002a", f"base-{asset_type}-test-002b"],
        ).fetchall()
        build_action_rows = base_conn.execute(
            """
            SELECT asset_type, code, adjust_method, action, row_count
            FROM base_build_action
            WHERE run_id = ?
            """,
            [f"base-{asset_type}-test-002b"],
        ).fetchall()
        dirty_rows = base_conn.execute(
            """
            SELECT dirty_nk, asset_type, dirty_status, last_consumed_run_id
            FROM base_dirty_instrument
            WHERE asset_type = ?
            """,
            [asset_type],
        ).fetchall()
    finally:
        base_conn.close()

    assert market_rows == [
        (f"{code}.SH", date(2026, 4, 8), 10.4, f"base-{asset_type}-test-002b"),
        (f"{code}.SH", date(2026, 4, 9), 11.2, f"base-{asset_type}-test-002b"),
    ]
    assert build_run_rows == [
        (asset_type, f"base-{asset_type}-test-002a", "full", 0, 1, "completed"),
        (asset_type, f"base-{asset_type}-test-002b", "dirty_queue", 1, 1, "completed"),
    ]
    assert build_action_rows == [(asset_type, f"{code}.SH", "backward", "rematerialized", 2)]
    assert dirty_rows == [(f"{asset_type}|{code}.SH|backward", asset_type, "consumed", f"base-{asset_type}-test-002b")]
