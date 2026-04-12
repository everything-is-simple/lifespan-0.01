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


def test_run_tdxquant_daily_raw_sync_bridges_none_bars_and_checkpoint(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_root = tmp_path / "tdx"
    strategy_path = tmp_path / "card19_sync_001.py"
    strategy_path.write_text("# fake tq strategy\n", encoding="utf-8")

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
    run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_id="raw-seed-001",
    )

    bars_by_code = {
        "600000.SH": (
            TdxQuantDailyBar("600000.SH", "浦发银行", "stock", date(2026, 4, 9), 9.4, 9.8, 9.3, 9.7, 1200, 11640),
            TdxQuantDailyBar("600000.SH", "浦发银行", "stock", date(2026, 4, 10), 9.8, 10.0, 9.7, 9.9, 1300, 12870),
        ),
        "510300.SH": (
            TdxQuantDailyBar("510300.SH", "沪深300ETF", "stock", date(2026, 4, 9), 4.0, 4.1, 3.9, 4.05, 5000, 20250),
            TdxQuantDailyBar("510300.SH", "沪深300ETF", "stock", date(2026, 4, 10), 4.05, 4.2, 4.0, 4.18, 5200, 21736),
        ),
    }
    infos = {
        code: TdxQuantInstrumentInfo(code=code, name=rows[0].name, asset_type="stock")
        for code, rows in bars_by_code.items()
    }

    first_client = FakeTdxQuantClient(infos=infos, bars_by_code=bars_by_code)
    first_summary = run_tdxquant_daily_raw_sync(
        settings=settings,
        strategy_path=strategy_path,
        onboarding_instruments=["510300.SH"],
        end_trade_date="2026-04-10",
        count=5,
        run_id="tq-test-001a",
        client_factory=lambda _: first_client,
    )
    second_client = FakeTdxQuantClient(infos=infos, bars_by_code=bars_by_code)
    second_summary = run_tdxquant_daily_raw_sync(
        settings=settings,
        strategy_path=strategy_path,
        onboarding_instruments=["510300.SH"],
        end_trade_date="2026-04-10",
        count=5,
        run_id="tq-test-001b",
        client_factory=lambda _: second_client,
    )

    assert first_client.closed is True
    assert second_client.closed is True
    assert first_summary.scope_source == "registry_onboarding_union"
    assert first_summary.candidate_instrument_count == 2
    assert first_summary.inserted_bar_count == 4
    assert first_summary.dirty_mark_count == 2
    assert second_summary.reused_bar_count == 4
    assert second_summary.dirty_mark_count == 0

    raw_conn = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        run_rows = raw_conn.execute(
            """
            SELECT run_id, candidate_instrument_count, successful_request_count, failed_request_count, run_status
            FROM raw_tdxquant_run
            ORDER BY run_id
            """
        ).fetchall()
        request_rows = raw_conn.execute(
            """
            SELECT run_id, code, status, inserted_bar_count, reused_bar_count, rematerialized_bar_count
            FROM raw_tdxquant_request
            ORDER BY run_id, code
            """
        ).fetchall()
        checkpoint_rows = raw_conn.execute(
            """
            SELECT code, last_success_trade_date, last_success_run_id
            FROM raw_tdxquant_instrument_checkpoint
            ORDER BY code
            """
        ).fetchall()
        raw_rows = raw_conn.execute(
            """
            SELECT code, trade_date, adjust_method, close, source_file_nk
            FROM stock_daily_bar
            WHERE adjust_method = 'none'
            ORDER BY code, trade_date
            """
        ).fetchall()
    finally:
        raw_conn.close()

    base_conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        dirty_rows = base_conn.execute(
            """
            SELECT code, adjust_method, dirty_reason, source_run_id
            FROM base_dirty_instrument
            WHERE adjust_method = 'none'
            ORDER BY code
            """
        ).fetchall()
    finally:
        base_conn.close()

    assert run_rows == [
        ("tq-test-001a", 2, 2, 0, "completed"),
        ("tq-test-001b", 2, 2, 0, "completed"),
    ]
    assert request_rows == [
        ("tq-test-001a", "510300.SH", "completed", 2, 0, 0),
        ("tq-test-001a", "600000.SH", "completed", 2, 0, 0),
        ("tq-test-001b", "510300.SH", "skipped_unchanged", 0, 2, 0),
        ("tq-test-001b", "600000.SH", "skipped_unchanged", 0, 2, 0),
    ]
    assert checkpoint_rows == [
        ("510300.SH", date(2026, 4, 10), "tq-test-001b"),
        ("600000.SH", date(2026, 4, 10), "tq-test-001b"),
    ]
    assert raw_rows == [
        ("510300.SH", date(2026, 4, 9), "none", 4.05, "tq-test-001a|510300.SH|none|5|20260410150000"),
        ("510300.SH", date(2026, 4, 10), "none", 4.18, "tq-test-001a|510300.SH|none|5|20260410150000"),
        ("600000.SH", date(2026, 4, 9), "none", 9.7, "tq-test-001a|600000.SH|none|5|20260410150000"),
        ("600000.SH", date(2026, 4, 10), "none", 9.9, "tq-test-001a|600000.SH|none|5|20260410150000"),
    ]
    assert dirty_rows == [
        ("510300.SH", "none", "raw_tdxquant_changed", "tq-test-001a"),
        ("600000.SH", "none", "raw_tdxquant_changed", "tq-test-001a"),
    ]


def test_run_tdxquant_daily_raw_sync_records_failed_request_and_run(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    strategy_path = tmp_path / "card19_sync_fail.py"
    strategy_path.write_text("# fake tq strategy\n", encoding="utf-8")

    failing_client = FakeTdxQuantClient(
        infos={"600000.SH": TdxQuantInstrumentInfo(code="600000.SH", name="浦发银行", asset_type="stock")},
        bars_by_code={},
        failing_codes={"600000.SH"},
    )

    with pytest.raises(ValueError, match="mock failure for 600000.SH"):
        run_tdxquant_daily_raw_sync(
            settings=settings,
            strategy_path=strategy_path,
            onboarding_instruments=["600000.SH"],
            use_registry_scope=False,
            end_trade_date="2026-04-10",
            count=5,
            run_id="tq-test-002a",
            client_factory=lambda _: failing_client,
        )

    assert failing_client.closed is True

    raw_conn = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        run_row = raw_conn.execute(
            """
            SELECT candidate_instrument_count, processed_instrument_count, failed_request_count, run_status
            FROM raw_tdxquant_run
            WHERE run_id = 'tq-test-002a'
            """
        ).fetchone()
        request_row = raw_conn.execute(
            """
            SELECT code, status, response_row_count, error_message
            FROM raw_tdxquant_request
            WHERE run_id = 'tq-test-002a'
            """
        ).fetchone()
        none_count = raw_conn.execute(
            """
            SELECT COUNT(*)
            FROM stock_daily_bar
            WHERE adjust_method = 'none'
            """
        ).fetchone()[0]
    finally:
        raw_conn.close()

    assert run_row == (1, 1, 1, "failed")
    assert request_row[0] == "600000.SH"
    assert request_row[1] == "failed"
    assert request_row[2] == 0
    assert "mock failure for 600000.SH" in request_row[3]
    assert none_count == 0


