"""验证 canonical MALF v2 runner 的 timeframe native source 合同。"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.malf import (
    MALF_CANONICAL_CHECKPOINT_TABLE,
    MALF_CANONICAL_WORK_QUEUE_TABLE,
    MALF_SAME_LEVEL_STATS_TABLE,
    MALF_STATE_SNAPSHOT_TABLE,
    run_malf_canonical_build,
)


_SOURCE_TABLE_BY_TIMEFRAME = {
    "D": "stock_daily_adjusted",
    "W": "stock_weekly_adjusted",
    "M": "stock_monthly_adjusted",
}


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


def _generate_price_rows(
    *,
    dates: list[date],
    close_seed: float,
    pattern: list[float],
    code: str = "600000.SH",
    name: str = "浦发银行",
) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    previous_close = close_seed
    for offset, trade_date in enumerate(dates):
        close = close_seed + pattern[offset % len(pattern)]
        open_price = previous_close
        high = max(open_price, close) + 0.8
        low = min(open_price, close) - 0.8
        volume = 1000000 + offset * 1000
        amount = volume * close
        rows.append(
            (
                code,
                name,
                trade_date,
                "backward",
                open_price,
                high,
                low,
                close,
                volume,
                amount,
            )
        )
        previous_close = close
    return rows


def _replace_market_base_rows(
    market_base_path: Path,
    *,
    table_name: str,
    rows: list[tuple[object, ...]],
) -> None:
    market_base_path.parent.mkdir(parents=True, exist_ok=True)
    if market_base_path.exists():
        market_base_path.unlink()
    conn = duckdb.connect(str(market_base_path))
    try:
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
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
        for row in rows:
            conn.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", list(row))
    finally:
        conn.close()


def _append_market_base_rows(
    market_base_path: Path,
    *,
    table_name: str,
    rows: list[tuple[object, ...]],
) -> None:
    conn = duckdb.connect(str(market_base_path))
    try:
        for row in rows:
            conn.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", list(row))
    finally:
        conn.close()


def _seed_native_market_base(settings) -> dict[str, list[date]]:
    daily_dates = [date(2026, 1, 1) + timedelta(days=offset) for offset in range(95)]
    weekly_dates = [date(2025, 12, 5) + timedelta(days=offset * 7) for offset in range(18)]
    monthly_dates = [
        date(2025, 9, 30),
        date(2025, 10, 31),
        date(2025, 11, 30),
        date(2025, 12, 31),
        date(2026, 1, 31),
        date(2026, 2, 28),
        date(2026, 3, 31),
    ]
    _replace_market_base_rows(
        settings.databases.market_base_day,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["D"],
        rows=_generate_price_rows(
            dates=daily_dates,
            close_seed=30.0,
            pattern=[
                0.0, 2.0, 5.0, 7.0, 6.0, 4.0, 2.0, 0.0, -2.0, -4.0,
                -3.0, -1.0, 2.0, 5.0, 8.0, 7.0, 5.0, 2.0, -1.0, -3.0,
                -5.0, -4.0, -2.0, 1.0, 4.0, 7.0, 9.0, 8.0, 6.0, 3.0,
            ],
        ),
    )
    _replace_market_base_rows(
        settings.databases.market_base_week,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["W"],
        rows=_generate_price_rows(
            dates=weekly_dates,
            close_seed=50.0,
            pattern=[0.0, 5.0, 8.0, 4.0, -1.0, -5.0, -2.0, 3.0, 7.0, 2.0],
        ),
    )
    _replace_market_base_rows(
        settings.databases.market_base_month,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["M"],
        rows=_generate_price_rows(
            dates=monthly_dates,
            close_seed=80.0,
            pattern=[0.0, 6.0, 12.0, 7.0, -3.0, -9.0, 2.0],
        ),
    )
    return {"D": daily_dates, "W": weekly_dates, "M": monthly_dates}


def test_run_malf_canonical_build_materializes_native_dwm_ledgers(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    source_dates = _seed_native_market_base(settings)

    summary = run_malf_canonical_build(
        settings=settings,
        signal_end_date="2026-04-05",
        run_id="malf-canonical-test-001",
    )

    assert summary.claimed_scope_count == 3
    assert summary.completed_scope_count == 3
    assert summary.primary_summary_timeframe == "D"
    assert summary.market_base_path_map == {
        "D": str(settings.databases.market_base_day),
        "W": str(settings.databases.market_base_week),
        "M": str(settings.databases.market_base_month),
    }
    assert summary.malf_ledger_path_map == {
        "D": str(settings.databases.malf_day),
        "W": str(settings.databases.malf_week),
        "M": str(settings.databases.malf_month),
    }
    assert summary.source_price_table_map == _SOURCE_TABLE_BY_TIMEFRAME
    assert summary.source_scope_count_by_timeframe == {"D": 1, "W": 1, "M": 1}
    assert summary.completed_scope_count_by_timeframe == {"D": 1, "W": 1, "M": 1}
    assert summary.source_row_count_by_timeframe == {
        "D": len(source_dates["D"]),
        "W": len(source_dates["W"]),
        "M": len(source_dates["M"]),
    }
    assert summary.source_date_range_by_timeframe == {
        "D": {"start": source_dates["D"][0].isoformat(), "end": source_dates["D"][-1].isoformat()},
        "W": {"start": source_dates["W"][0].isoformat(), "end": source_dates["W"][-1].isoformat()},
        "M": {"start": source_dates["M"][0].isoformat(), "end": source_dates["M"][-1].isoformat()},
    }

    for timeframe, expected_path in summary.malf_ledger_path_map.items():
        conn = duckdb.connect(expected_path, read_only=True)
        try:
            state_rows = conn.execute(
                f"""
                SELECT DISTINCT timeframe
                FROM {MALF_STATE_SNAPSHOT_TABLE}
                ORDER BY timeframe
                """
            ).fetchall()
            checkpoint_rows = conn.execute(
                f"""
                SELECT timeframe, last_completed_bar_dt
                FROM {MALF_CANONICAL_CHECKPOINT_TABLE}
                ORDER BY timeframe
                """
            ).fetchall()
            stats_count = conn.execute(f"SELECT COUNT(*) FROM {MALF_SAME_LEVEL_STATS_TABLE}").fetchone()[0]
        finally:
            conn.close()

        assert state_rows == [(timeframe,)]
        assert checkpoint_rows == [(timeframe, min(source_dates[timeframe][-1], date(2026, 4, 5)))]
        assert stats_count > 0


def test_run_malf_canonical_build_requeues_after_native_source_advance(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _replace_market_base_rows(
        settings.databases.market_base_day,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["D"],
        rows=_generate_price_rows(
            dates=[date(2026, 1, 1) + timedelta(days=offset) for offset in range(70)],
            close_seed=30.0,
            pattern=[0.0, 2.0, 5.0, 3.0, -1.0, -4.0, -2.0, 1.0],
        ),
    )
    _replace_market_base_rows(
        settings.databases.market_base_week,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["W"],
        rows=_generate_price_rows(
            dates=[date(2025, 12, 12) + timedelta(days=offset * 7) for offset in range(10)],
            close_seed=50.0,
            pattern=[0.0, 4.0, 7.0, 2.0, -2.0, -6.0],
        ),
    )
    _replace_market_base_rows(
        settings.databases.market_base_month,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["M"],
        rows=_generate_price_rows(
            dates=[
                date(2025, 10, 31),
                date(2025, 11, 30),
                date(2025, 12, 31),
                date(2026, 1, 31),
                date(2026, 2, 28),
            ],
            close_seed=80.0,
            pattern=[0.0, 5.0, 9.0, 4.0, -5.0],
        ),
    )

    first_summary = run_malf_canonical_build(
        settings=settings,
        signal_end_date="2026-03-11",
        run_id="malf-canonical-test-002a",
    )
    assert first_summary.completed_scope_count == 3

    _append_market_base_rows(
        settings.databases.market_base_day,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["D"],
        rows=_generate_price_rows(
            dates=[date(2026, 3, 12) + timedelta(days=offset) for offset in range(6)],
            close_seed=36.0,
            pattern=[1.0, 3.5, 6.0, 4.5, 2.0],
        ),
    )
    _append_market_base_rows(
        settings.databases.market_base_week,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["W"],
        rows=_generate_price_rows(
            dates=[date(2026, 3, 13), date(2026, 3, 20)],
            close_seed=56.0,
            pattern=[1.0, 5.0],
        ),
    )
    _append_market_base_rows(
        settings.databases.market_base_month,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["M"],
        rows=_generate_price_rows(
            dates=[date(2026, 3, 31)],
            close_seed=83.0,
            pattern=[4.0],
        ),
    )

    second_summary = run_malf_canonical_build(
        settings=settings,
        signal_end_date="2026-03-31",
        run_id="malf-canonical-test-002b",
    )

    assert second_summary.claimed_scope_count == 3
    assert second_summary.completed_scope_count == 3

    expected_checkpoint_end = {"D": date(2026, 3, 17), "W": date(2026, 3, 20), "M": date(2026, 3, 31)}
    for timeframe, ledger_path in second_summary.malf_ledger_path_map.items():
        conn = duckdb.connect(ledger_path, read_only=True)
        try:
            queue_status = conn.execute(
                f"""
                SELECT queue_status
                FROM {MALF_CANONICAL_WORK_QUEUE_TABLE}
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()[0]
            checkpoint = conn.execute(
                f"""
                SELECT last_completed_bar_dt
                FROM {MALF_CANONICAL_CHECKPOINT_TABLE}
                WHERE timeframe = ?
                """,
                [timeframe],
            ).fetchone()[0]
        finally:
            conn.close()

        assert queue_status == "completed"
        assert checkpoint == expected_checkpoint_end[timeframe]


def test_run_malf_canonical_build_uses_native_week_month_sources_without_day_resample(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _replace_market_base_rows(
        settings.databases.market_base_day,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["D"],
        rows=_generate_price_rows(
            dates=[date(2026, 1, 1) + timedelta(days=offset) for offset in range(40)],
            close_seed=30.0,
            pattern=[0.0, 2.0, 4.0, 1.0, -1.0, -3.0],
        ),
    )
    weekly_dates = [
        date(2026, 1, 9),
        date(2026, 1, 16),
        date(2026, 1, 23),
        date(2026, 1, 30),
        date(2026, 2, 6),
    ]
    monthly_dates = [
        date(2025, 11, 30),
        date(2025, 12, 31),
        date(2026, 1, 31),
        date(2026, 2, 28),
    ]
    _replace_market_base_rows(
        settings.databases.market_base_week,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["W"],
        rows=_generate_price_rows(
            dates=weekly_dates,
            close_seed=70.0,
            pattern=[0.0, 8.0, 3.0, -2.0, 5.0],
        ),
    )
    _replace_market_base_rows(
        settings.databases.market_base_month,
        table_name=_SOURCE_TABLE_BY_TIMEFRAME["M"],
        rows=_generate_price_rows(
            dates=monthly_dates,
            close_seed=90.0,
            pattern=[0.0, 10.0, -4.0, 6.0],
        ),
    )

    summary = run_malf_canonical_build(
        settings=settings,
        timeframes=["W", "M"],
        signal_end_date="2026-02-28",
        run_id="malf-canonical-test-003",
    )

    assert summary.timeframe_list == ["W", "M"]
    assert summary.source_row_count_by_timeframe == {"W": len(weekly_dates), "M": len(monthly_dates)}
    assert summary.source_date_range_by_timeframe == {
        "W": {"start": weekly_dates[0].isoformat(), "end": weekly_dates[-1].isoformat()},
        "M": {"start": monthly_dates[0].isoformat(), "end": monthly_dates[-1].isoformat()},
    }
    assert summary.source_price_table_map == {
        "W": "stock_weekly_adjusted",
        "M": "stock_monthly_adjusted",
    }

    for timeframe, expected_date in {"W": weekly_dates[-1], "M": monthly_dates[-1]}.items():
        conn = duckdb.connect(summary.malf_ledger_path_map[timeframe], read_only=True)
        try:
            checkpoint = conn.execute(
                f"""
                SELECT last_completed_bar_dt
                FROM {MALF_CANONICAL_CHECKPOINT_TABLE}
                WHERE timeframe = ?
                """,
                [timeframe],
            ).fetchone()[0]
        finally:
            conn.close()
        assert checkpoint == expected_date
