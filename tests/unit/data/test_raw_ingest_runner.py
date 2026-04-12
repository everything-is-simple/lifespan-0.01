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
from mlq.data.tdxquant import TdxQuantDailyBar, TdxQuantInstrumentInfo


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


def _write_tdx_stock_file(
    root: Path,
    *,
    folder_name: str,
    code: str,
    exchange: str,
    name: str,
    rows: list[tuple[str, float, float, float, float, float, float]],
) -> None:
    _write_tdx_asset_file(
        root,
        asset_type="stock",
        folder_name=folder_name,
        code=code,
        exchange=exchange,
        name=name,
        rows=rows,
    )


class FakeTdxQuantClient:
    def __init__(
        self,
        *,
        infos: dict[str, TdxQuantInstrumentInfo],
        bars_by_code: dict[str, tuple[TdxQuantDailyBar, ...]],
        failing_codes: set[str] | None = None,
    ) -> None:
        self._infos = infos
        self._bars_by_code = bars_by_code
        self._failing_codes = failing_codes or set()
        self.closed = False

    def get_instrument_info(self, code: str) -> TdxQuantInstrumentInfo:
        if code in self._failing_codes:
            raise ValueError(f"mock failure for {code}")
        return self._infos[code]

    def get_daily_bars(
        self,
        *,
        code: str,
        end_trade_date: date,
        count: int,
        dividend_type: str = "none",
    ) -> tuple[TdxQuantDailyBar, ...]:
        if code in self._failing_codes:
            raise ValueError(f"mock failure for {code}")
        assert dividend_type == "none"
        assert count > 0
        assert end_trade_date.isoformat() == "2026-04-10"
        return self._bars_by_code[code]

    def close(self) -> None:
        self.closed = True


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


