"""验证 canonical MALF v2 runner 的最小合同。"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.malf import (
    MALF_CANONICAL_CHECKPOINT_TABLE,
    MALF_CANONICAL_WORK_QUEUE_TABLE,
    MALF_PIVOT_LEDGER_TABLE,
    MALF_SAME_LEVEL_STATS_TABLE,
    MALF_STATE_SNAPSHOT_TABLE,
    run_malf_canonical_build,
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


def _seed_market_base_rows(market_base_path: Path, *, start_date: date, day_count: int) -> None:
    market_base_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(market_base_path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                trade_date DATE NOT NULL,
                adjust_method TEXT NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE
            )
            """
        )
        pattern = [
            0.0, 2.0, 5.0, 7.0, 6.0, 4.0, 2.0, 0.0, -2.0, -4.0,
            -3.0, -1.0, 2.0, 5.0, 8.0, 7.0, 5.0, 2.0, -1.0, -3.0,
            -5.0, -4.0, -2.0, 1.0, 4.0, 7.0, 9.0, 8.0, 6.0, 3.0,
        ]
        previous_close = 30.0
        for offset in range(day_count):
            trade_date = start_date + timedelta(days=offset)
            close = 30.0 + pattern[offset % len(pattern)]
            open_price = previous_close
            high = max(open_price, close) + 0.8
            low = min(open_price, close) - 0.8
            volume = 1000000 + offset * 1000
            amount = volume * close
            conn.execute(
                "INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "600000.SH",
                    "浦发银行",
                    trade_date,
                    "backward",
                    open_price,
                    high,
                    low,
                    close,
                    volume,
                    amount,
                ],
            )
            previous_close = close
    finally:
        conn.close()


def _append_market_base_rows(market_base_path: Path, *, start_date: date, day_count: int) -> None:
    conn = duckdb.connect(str(market_base_path))
    try:
        previous_close = conn.execute(
            """
            SELECT close
            FROM stock_daily_adjusted
            WHERE code = '600000.SH'
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ).fetchone()[0]
        pattern = [1.0, 3.5, 6.0, 4.5, 2.0]
        for offset in range(day_count):
            trade_date = start_date + timedelta(days=offset)
            close = float(previous_close) + pattern[offset % len(pattern)] * 0.4
            open_price = float(previous_close)
            high = max(open_price, close) + 0.7
            low = min(open_price, close) - 0.7
            volume = 2000000 + offset * 500
            amount = volume * close
            conn.execute(
                "INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "600000.SH",
                    "浦发银行",
                    trade_date,
                    "backward",
                    open_price,
                    high,
                    low,
                    close,
                    volume,
                    amount,
                ],
            )
            previous_close = close
    finally:
        conn.close()


def test_run_malf_canonical_build_materializes_dwm_and_confirmed_pivots(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_market_base_rows(settings.databases.market_base, start_date=date(2026, 1, 1), day_count=95)

    summary = run_malf_canonical_build(
        settings=settings,
        signal_end_date="2026-04-05",
        run_id="malf-canonical-test-001",
    )

    assert summary.claimed_scope_count == 3
    assert summary.pivot_row_count > 0
    assert summary.state_row_count > 0
    assert summary.stats_row_count > 0

    malf_path = Path(summary.malf_ledger_path)
    assert malf_path.exists()
    conn = duckdb.connect(str(malf_path), read_only=True)
    try:
        pivot_rows = conn.execute(
            f"""
            SELECT timeframe, pivot_bar_dt, confirmed_at
            FROM {MALF_PIVOT_LEDGER_TABLE}
            ORDER BY timeframe, confirmed_at
            """
        ).fetchall()
        timeframe_rows = conn.execute(
            f"""
            SELECT DISTINCT timeframe
            FROM {MALF_STATE_SNAPSHOT_TABLE}
            ORDER BY timeframe
            """
        ).fetchall()
        stats_count = conn.execute(f"SELECT COUNT(*) FROM {MALF_SAME_LEVEL_STATS_TABLE}").fetchone()[0]
    finally:
        conn.close()

    assert {row[0] for row in timeframe_rows} == {"D", "M", "W"}
    assert any(row[2] > row[1] for row in pivot_rows)
    assert stats_count > 0


def test_run_malf_canonical_build_requeues_after_source_advance(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_market_base_rows(settings.databases.market_base, start_date=date(2026, 1, 1), day_count=70)

    first_summary = run_malf_canonical_build(
        settings=settings,
        signal_end_date="2026-03-11",
        run_id="malf-canonical-test-002a",
    )
    assert first_summary.completed_scope_count == 3

    _append_market_base_rows(settings.databases.market_base, start_date=date(2026, 3, 12), day_count=6)

    second_summary = run_malf_canonical_build(
        settings=settings,
        signal_end_date="2026-03-17",
        run_id="malf-canonical-test-002b",
    )

    assert second_summary.claimed_scope_count == 3
    malf_path = Path(second_summary.malf_ledger_path)
    assert malf_path.exists()
    conn = duckdb.connect(str(malf_path), read_only=True)
    try:
        queue_statuses = conn.execute(
            f"""
            SELECT timeframe, queue_status
            FROM {MALF_CANONICAL_WORK_QUEUE_TABLE}
            ORDER BY timeframe, updated_at DESC
            """
        ).fetchall()
        checkpoints = conn.execute(
            f"""
            SELECT timeframe, last_completed_bar_dt
            FROM {MALF_CANONICAL_CHECKPOINT_TABLE}
            ORDER BY timeframe
            """
        ).fetchall()
    finally:
        conn.close()

    assert all(status == "completed" for _, status in queue_statuses[-3:])
    assert len(checkpoints) == 3
    assert max(row[1] for row in checkpoints) >= date(2026, 3, 17)
