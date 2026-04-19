"""覆盖 `market_base dirty queue` 的清账与失败保护。"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import mlq.data.data_market_base_runner as market_base_runner_module
from mlq.core.paths import default_settings
from mlq.data import (
    bootstrap_market_base_ledger,
    market_base_ledger_path,
    mark_base_instrument_dirty,
    run_market_base_build,
    run_tdx_stock_raw_ingest,
)
from mlq.data.data_market_base_governance import mark_dirty_entries_consumed
from mlq.data.data_shared import BaseDirtyInstrumentEntry
from tests.unit.data.market_base_test_support import (
    bootstrap_repo_root as _bootstrap_repo_root,
    clear_workspace_env as _clear_workspace_env,
    write_tdx_stock_file as _write_tdx_stock_file,
)


def test_mark_dirty_entries_consumed_falls_back_to_natural_key_when_dirty_nk_is_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    bootstrap_market_base_ledger(settings)
    dirty_nk = mark_base_instrument_dirty(
        settings=settings,
        code="600000.SH",
        adjust_method="backward",
        dirty_reason="raw_changed",
        source_run_id="raw-test-stale-001",
    )

    conn = duckdb.connect(str(market_base_ledger_path(settings)))
    try:
        updated_count = mark_dirty_entries_consumed(
            conn,
            run_id="base-test-stale-001",
            dirty_entries=(
                BaseDirtyInstrumentEntry(
                    dirty_nk=f"stale::{dirty_nk}",
                    asset_type="stock",
                    timeframe="day",
                    code="600000.SH",
                    adjust_method="backward",
                    dirty_reason="raw_changed",
                    source_run_id="raw-test-stale-001",
                    source_file_nk=None,
                ),
            ),
        )
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

    assert updated_count == 1
    assert dirty_row == ("consumed", "base-test-stale-001")


def test_run_market_base_build_raises_when_dirty_queue_consume_count_mismatches(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
    run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        adjust_method="backward",
        run_id="raw-test-004c",
    )
    run_market_base_build(
        settings=settings,
        adjust_method="backward",
        build_mode="full",
        run_id="base-test-004c",
    )
    dirty_nk = mark_base_instrument_dirty(
        settings=settings,
        code="600000.SH",
        adjust_method="backward",
        dirty_reason="raw_changed",
        source_run_id="raw-test-004c",
    )

    monkeypatch.setattr(
        market_base_runner_module,
        "_mark_dirty_entries_consumed",
        lambda *args, **kwargs: 0,
    )

    with pytest.raises(RuntimeError, match="Dirty queue consume count mismatch: expected=1, updated=0"):
        run_market_base_build(
            settings=settings,
            adjust_method="backward",
            build_mode="incremental",
            run_id="base-test-004d",
        )

    conn = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        build_run_row = conn.execute(
            """
            SELECT run_status
            FROM base_build_run
            WHERE run_id = 'base-test-004d'
            """
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

    assert build_run_row == ("failed",)
    assert dirty_row == ("pending", "base-test-004c")
