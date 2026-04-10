"""覆盖 `TDX -> raw_market -> market_base` 最小正式桥接。"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.data import market_base_ledger_path, raw_market_ledger_path, run_market_base_build, run_tdx_stock_raw_ingest


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
        run_id="raw-test-001a",
    )
    second_summary = run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
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
        run_id="raw-test-001c",
    )

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
    finally:
        conn.close()

    assert registry_count == 1
    assert close_row == (11.1, "raw-test-001c")


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
