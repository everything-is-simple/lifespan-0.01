"""覆盖 `77` day 库 week/month 尾巴清理。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.data import bootstrap_market_base_ledger, bootstrap_raw_market_ledger
from mlq.data.bootstrap import MARKET_BASE_LEDGER_TABLES, RAW_MARKET_LEDGER_TABLES
from mlq.data.data_timeframe_split_cleanup import purge_day_timeframe_split_tail
from mlq.data.ledger_timeframe import market_base_ledger_path, raw_market_ledger_path


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


def test_purge_day_timeframe_split_tail_keeps_day_and_objective_tables(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    bootstrap_raw_market_ledger(settings)
    bootstrap_market_base_ledger(settings)

    raw_connection = duckdb.connect(str(raw_market_ledger_path(settings)))
    try:
        raw_connection.execute(
            """
            INSERT INTO stock_daily_bar (
                bar_nk, source_file_nk, asset_type, timeframe, code, name, trade_date, adjust_method,
                open, high, low, close, volume, amount, source_path, source_mtime_utc,
                first_seen_run_id, last_ingested_run_id
            ) VALUES (
                'day-bar', 'day-file', 'stock', 'day', '600000.SH', '浦发银行', DATE '2026-04-10', 'backward',
                10.0, 10.5, 9.9, 10.4, 1000, 10000, 'day-path', TIMESTAMP '2026-04-10 09:00:00',
                'raw-day-run', 'raw-day-run'
            )
            """
        )
        raw_connection.execute(RAW_MARKET_LEDGER_TABLES["index_weekly_bar"])
        raw_connection.execute(
            """
            INSERT INTO index_weekly_bar (
                bar_nk, source_file_nk, asset_type, timeframe, code, name, trade_date, adjust_method,
                open, high, low, close, volume, amount, source_path, source_mtime_utc,
                first_seen_run_id, last_ingested_run_id
            ) VALUES (
                'week-bar', 'week-file', 'index', 'week', '000300.SH', '沪深300', DATE '2026-04-10', 'backward',
                4000.0, 4050.0, 3980.0, 4040.0, 1000, 10000, 'week-path', TIMESTAMP '2026-04-10 09:00:00',
                'raw-week-run', 'raw-week-run'
            )
            """
        )
        raw_connection.execute(
            """
            INSERT INTO stock_file_registry (
                file_nk, asset_type, timeframe, adjust_method, code, name, source_path,
                source_size_bytes, source_mtime_utc, source_line_count, source_header,
                source_content_hash, last_ingested_run_id
            ) VALUES
            (
                'day-file', 'stock', 'day', 'backward', '600000.SH', '浦发银行', 'day-path',
                1, TIMESTAMP '2026-04-10 09:00:00', 1, 'header', 'hash-day', 'raw-day-run'
            ),
            (
                'week-file', 'stock', 'week', 'backward', '600000.SH', '浦发银行', 'week-path',
                1, TIMESTAMP '2026-04-10 09:00:00', 1, 'header', 'hash-week', 'raw-week-run'
            )
            """
        )
        raw_connection.execute(
            """
            INSERT INTO raw_ingest_run (
                run_id, asset_type, timeframe, runner_name, runner_version, adjust_method, run_mode,
                source_root, candidate_file_count, processed_file_count, skipped_file_count,
                inserted_bar_count, reused_bar_count, rematerialized_bar_count, run_status, completed_at
            ) VALUES
            (
                'raw-day-run', 'stock', 'day', 'runner', 'v1', 'backward', 'full',
                'day-root', 1, 1, 0, 1, 0, 0, 'completed', TIMESTAMP '2026-04-10 10:00:00'
            ),
            (
                'raw-week-run', 'index', 'week', 'runner', 'v1', 'backward', 'full',
                'week-root', 1, 1, 0, 1, 0, 0, 'completed', TIMESTAMP '2026-04-10 10:00:00'
            )
            """
        )
        raw_connection.execute(
            """
            INSERT INTO raw_ingest_file (
                run_id, asset_type, timeframe, file_nk, code, name, adjust_method, source_path,
                fingerprint_mode, action, row_count
            ) VALUES
            (
                'raw-day-run', 'stock', 'day', 'day-file', '600000.SH', '浦发银行', 'backward',
                'day-path', 'content_hash', 'inserted', 1
            ),
            (
                'raw-week-run', 'index', 'week', 'week-file', '000300.SH', '沪深300', 'backward',
                'week-path', 'content_hash', 'inserted', 1
            )
            """
        )
    finally:
        raw_connection.close()

    base_connection = duckdb.connect(str(market_base_ledger_path(settings)))
    try:
        base_connection.execute(
            """
            INSERT INTO stock_daily_adjusted (
                daily_bar_nk, code, name, timeframe, trade_date, adjust_method,
                open, high, low, close, volume, amount, source_bar_nk,
                first_seen_run_id, last_materialized_run_id
            ) VALUES (
                'day-base', '600000.SH', '浦发银行', 'day', DATE '2026-04-10', 'backward',
                10.0, 10.5, 9.9, 10.4, 1000, 10000, 'day-bar', 'base-day-run', 'base-day-run'
            )
            """
        )
        base_connection.execute(MARKET_BASE_LEDGER_TABLES["block_monthly_adjusted"])
        base_connection.execute(
            """
            INSERT INTO block_monthly_adjusted (
                daily_bar_nk, code, name, timeframe, trade_date, adjust_method,
                open, high, low, close, volume, amount, source_bar_nk,
                first_seen_run_id, last_materialized_run_id
            ) VALUES (
                'month-base', '880001.SH', '上证A股', 'month', DATE '2026-04-30', 'backward',
                1000.0, 1030.0, 995.0, 1020.0, 100.0, 1000.0, 'month-bar',
                'base-month-run', 'base-month-run'
            )
            """
        )
        base_connection.execute(
            """
            INSERT INTO base_dirty_instrument (
                dirty_nk, asset_type, timeframe, code, adjust_method, dirty_reason,
                source_run_id, source_file_nk, dirty_status, last_consumed_run_id
            ) VALUES
            (
                'day-dirty', 'stock', 'day', '600000.SH', 'backward', 'raw_inserted',
                'raw-day-run', 'day-file', 'pending', NULL
            ),
            (
                'month-dirty', 'block', 'month', '880001.SH', 'backward', 'raw_inserted',
                'raw-month-run', 'month-file', 'pending', NULL
            )
            """
        )
        base_connection.execute(
            """
            INSERT INTO base_build_run (
                run_id, asset_type, timeframe, runner_name, runner_version, adjust_method, build_mode,
                source_scope_kind, source_row_count, inserted_count, reused_count, rematerialized_count,
                consumed_dirty_count, run_status, completed_at
            ) VALUES
            (
                'base-day-run', 'stock', 'day', 'runner', 'v1', 'backward', 'full',
                'instrument', 1, 1, 0, 0, 0, 'completed', TIMESTAMP '2026-04-10 10:00:00'
            ),
            (
                'base-month-run', 'block', 'month', 'runner', 'v1', 'backward', 'full',
                'instrument', 1, 1, 0, 0, 1, 'completed', TIMESTAMP '2026-04-10 10:00:00'
            )
            """
        )
        base_connection.execute(
            """
            INSERT INTO base_build_scope (run_id, asset_type, timeframe, scope_type, scope_value) VALUES
            ('base-day-run', 'stock', 'day', 'instrument', '600000.SH'),
            ('base-month-run', 'block', 'month', 'instrument', '880001.SH')
            """
        )
        base_connection.execute(
            """
            INSERT INTO base_build_action (
                run_id, asset_type, timeframe, code, adjust_method, action, row_count
            ) VALUES
            ('base-day-run', 'stock', 'day', '600000.SH', 'backward', 'inserted', 1),
            ('base-month-run', 'block', 'month', '880001.SH', 'backward', 'inserted', 1)
            """
        )
    finally:
        base_connection.close()

    dry_run_summary = purge_day_timeframe_split_tail(settings=settings, execute=False)
    assert any(
        item["table"] == "index_weekly_bar" and item["row_count"] == 1
        for item in dry_run_summary["raw"]["drop_tables"]
    )
    assert any(item["table"] == "stock_file_registry" and item["row_count"] == 1 for item in dry_run_summary["raw"]["delete_rows"])
    assert any(item["table"] == "base_build_run" and item["row_count"] == 1 for item in dry_run_summary["base"]["delete_rows"])

    execute_summary = purge_day_timeframe_split_tail(settings=settings, execute=True)
    bootstrap_raw_market_ledger(settings)
    bootstrap_market_base_ledger(settings)

    raw_check = duckdb.connect(str(raw_market_ledger_path(settings)), read_only=True)
    try:
        raw_tables = {row[0] for row in raw_check.execute("SHOW TABLES").fetchall()}
        day_bar_count = raw_check.execute("SELECT COUNT(*) FROM stock_daily_bar").fetchone()[0]
        day_run_rows = raw_check.execute(
            "SELECT COALESCE(timeframe, 'day'), COUNT(*) FROM raw_ingest_run GROUP BY 1 ORDER BY 1"
        ).fetchall()
        day_registry_rows = raw_check.execute(
            "SELECT COALESCE(timeframe, 'day'), COUNT(*) FROM stock_file_registry GROUP BY 1 ORDER BY 1"
        ).fetchall()
    finally:
        raw_check.close()

    base_check = duckdb.connect(str(market_base_ledger_path(settings)), read_only=True)
    try:
        base_tables = {row[0] for row in base_check.execute("SHOW TABLES").fetchall()}
        day_base_count = base_check.execute("SELECT COUNT(*) FROM stock_daily_adjusted").fetchone()[0]
        dirty_rows = base_check.execute(
            "SELECT COALESCE(timeframe, 'day'), COUNT(*) FROM base_dirty_instrument GROUP BY 1 ORDER BY 1"
        ).fetchall()
        build_run_rows = base_check.execute(
            "SELECT COALESCE(timeframe, 'day'), COUNT(*) FROM base_build_run GROUP BY 1 ORDER BY 1"
        ).fetchall()
    finally:
        base_check.close()

    assert "stock_daily_bar" in raw_tables
    assert "raw_tdxquant_run" in raw_tables
    assert "tushare_objective_run" in raw_tables
    assert "index_weekly_bar" not in raw_tables
    assert "block_monthly_bar" not in raw_tables
    assert day_bar_count == 1
    assert day_run_rows == [("day", 1)]
    assert day_registry_rows == [("day", 1)]

    assert "stock_daily_adjusted" in base_tables
    assert "index_weekly_adjusted" not in base_tables
    assert "block_monthly_adjusted" not in base_tables
    assert day_base_count == 1
    assert dirty_rows == [("day", 1)]
    assert build_run_rows == [("day", 1)]

    assert any(
        item["table"] == "index_weekly_bar" and item["row_count"] == 0
        for item in execute_summary["raw"]["drop_tables"]
    )
    assert any(item["table"] == "base_build_run" and item["row_count"] == 0 for item in execute_summary["base"]["delete_rows"])
