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
    run_market_base_build,
    run_tdx_stock_raw_ingest,
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


def _write_tdx_stock_file(
    root: Path,
    *,
    folder_name: str,
    code: str,
    exchange: str,
    name: str,
    rows: list[tuple[str, float, float, float, float, float, float]],
) -> None:
    folder = root / "stock" / folder_name
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


def test_run_tdx_stock_raw_ingest_skips_unchanged_files_and_updates_changed_rows(tmp_path: Path, monkeypatch) -> None:
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

    first_summary = run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-test-001a",
    )
    second_summary = run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_mode="incremental",
        run_id="raw-test-001b",
    )

    assert first_summary.ingested_file_count == 1
    assert first_summary.bar_inserted_count == 2
    assert second_summary.skipped_unchanged_file_count == 1

    _write_tdx_stock_file(
        source_root,
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=[
            ("2026/04/08", 10.0, 10.5, 9.9, 10.4, 1000, 10400),
            ("2026/04/09", 10.4, 10.8, 10.2, 11.1, 1200, 13320),
        ],
    )
    changed_path = source_root / "stock" / "Backward-Adjusted" / "SH#600000.txt"
    changed_stat = changed_path.stat()
    os.utime(changed_path, (changed_stat.st_atime + 2, changed_stat.st_mtime + 2))
    third_summary = run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_mode="incremental",
        run_id="raw-test-001c",
    )

    assert first_summary.run_mode == "full"
    assert first_summary.processed_file_count == 1
    assert third_summary.bar_rematerialized_count == 1

    conn = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        registry_count = conn.execute("SELECT COUNT(*) FROM stock_file_registry").fetchone()[0]
        close_row = conn.execute(
            """
            SELECT close, last_ingested_run_id
            FROM stock_daily_bar
            WHERE code = '600000.SH' AND trade_date = DATE '2026-04-09' AND adjust_method = 'backward'
            """
        ).fetchone()
        run_rows = conn.execute(
            """
            SELECT run_id, run_mode, processed_file_count, skipped_file_count, inserted_bar_count, rematerialized_bar_count, run_status
            FROM raw_ingest_run
            ORDER BY run_id
            """
        ).fetchall()
        file_rows = conn.execute(
            """
            SELECT run_id, action, row_count
            FROM raw_ingest_file
            ORDER BY run_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert registry_count == 1
    assert close_row == (11.1, "raw-test-001c")
    assert run_rows == [
        ("raw-test-001a", "full", 1, 0, 2, 0, "completed"),
        ("raw-test-001b", "incremental", 1, 1, 0, 0, "completed"),
        ("raw-test-001c", "incremental", 1, 0, 0, 1, "completed"),
    ]
    assert file_rows == [
        ("raw-test-001a", "inserted", 2),
        ("raw-test-001b", "skipped_unchanged", 2),
        ("raw-test-001c", "rematerialized", 2),
    ]

    base_conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        dirty_row = base_conn.execute(
            """
            SELECT dirty_reason, source_run_id, source_file_nk, dirty_status
            FROM base_dirty_instrument
            WHERE dirty_nk = '600000.SH|backward'
            """
        ).fetchone()
    finally:
        base_conn.close()

    assert dirty_row[0] == "raw_rematerialized"
    assert dirty_row[1] == "raw-test-001c"
    assert dirty_row[3] == "pending"


def test_run_tdx_stock_raw_ingest_records_failed_file_and_failed_run(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"

    broken_folder = source_root / "stock" / "Backward-Adjusted"
    broken_folder.mkdir(parents=True, exist_ok=True)
    broken_path = broken_folder / "SH#600010.txt"
    broken_path.write_text("broken file\n", encoding="gbk")

    with pytest.raises(ValueError):
        run_tdx_stock_raw_ingest(
            settings=settings,
            source_root=source_root,
            adjust_method="backward",
            run_mode="incremental",
            run_id="raw-test-001d",
        )

    conn = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_mode, processed_file_count, run_status
            FROM raw_ingest_run
            WHERE run_id = 'raw-test-001d'
            """
        ).fetchone()
        file_row = conn.execute(
            """
            SELECT action, row_count, error_message
            FROM raw_ingest_file
            WHERE run_id = 'raw-test-001d'
            """
        ).fetchone()
        bar_count = conn.execute("SELECT COUNT(*) FROM stock_daily_bar").fetchone()[0]
    finally:
        conn.close()

    assert run_row == ("incremental", 1, "failed")
    assert file_row[0] == "failed"
    assert file_row[1] == 0
    assert "Unexpected TDX file format" in file_row[2]
    assert bar_count == 0


def test_run_tdx_stock_raw_ingest_force_hash_and_continue_from_last_run(tmp_path: Path, monkeypatch) -> None:
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
    valid_path = source_root / "stock" / "Backward-Adjusted" / "SH#600000.txt"

    first_summary = run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_mode="full",
        run_id="raw-test-001e",
    )
    original_stat = valid_path.stat()

    _write_tdx_stock_file(
        source_root,
        folder_name="Backward-Adjusted",
        code="600000",
        exchange="SH",
        name="浦发银行",
        rows=[
            ("2026/04/08", 10.0, 10.5, 9.9, 10.4, 1000, 10400),
            ("2026/04/09", 10.4, 10.8, 10.2, 10.8, 1200, 12960),
        ],
    )
    os.utime(valid_path, (original_stat.st_atime, original_stat.st_mtime))

    force_hash_summary = run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_mode="incremental",
        force_hash=True,
        run_id="raw-test-001f",
    )

    broken_path = source_root / "stock" / "Backward-Adjusted" / "SH#600001.txt"
    broken_path.write_text("broken file\n", encoding="gbk")
    with pytest.raises(ValueError):
        run_tdx_stock_raw_ingest(
            settings=settings,
            source_root=source_root,
            adjust_method="backward",
            run_mode="incremental",
            run_id="raw-test-001g",
        )

    broken_path.write_text(
        "\n".join(
            [
                "600001 平安银行 日线 后复权",
                "      日期\t    开盘\t    最高\t    最低\t    收盘\t    成交量\t    成交额",
                "2026/04/09\t9.20\t9.60\t9.10\t9.50\t1000\t9500.00",
            ]
        )
        + "\n",
        encoding="gbk",
    )
    continue_summary = run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_mode="incremental",
        continue_from_last_run=True,
        run_id="raw-test-001h",
    )

    assert first_summary.bar_inserted_count == 2
    assert force_hash_summary.bar_rematerialized_count == 1
    assert continue_summary.candidate_file_count == 1
    assert continue_summary.ingested_file_count == 1

    conn = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        file_rows = conn.execute(
            """
            SELECT run_id, code, fingerprint_mode, action
            FROM raw_ingest_file
            WHERE run_id IN ('raw-test-001f', 'raw-test-001g', 'raw-test-001h')
            ORDER BY run_id, code
            """
        ).fetchall()
        close_rows = conn.execute(
            """
            SELECT code, close, last_ingested_run_id
            FROM stock_daily_bar
            ORDER BY code, trade_date
            """
        ).fetchall()
    finally:
        conn.close()

    assert file_rows == [
        ("raw-test-001f", "600000.SH", "content_hash", "rematerialized"),
        ("raw-test-001g", "600000.SH", "size_mtime", "skipped_unchanged"),
        ("raw-test-001g", "600001.SH", "size_mtime", "failed"),
        ("raw-test-001h", "600001.SH", "size_mtime", "inserted"),
    ]
    assert close_rows == [
        ("600000.SH", 10.4, "raw-test-001f"),
        ("600000.SH", 10.8, "raw-test-001f"),
        ("600001.SH", 9.5, "raw-test-001h"),
    ]


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
