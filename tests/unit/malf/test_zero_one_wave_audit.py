"""覆盖 `80` 的 malf 0/1 wave 只读分类审计。"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.malf import bootstrap_malf_ledger, run_malf_zero_one_wave_audit


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


def _seed_wave_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    code: str,
    wave_id: int,
    direction: str,
    major_state: str,
    reversal_stage: str,
    start_bar_dt: str,
    end_bar_dt: str,
    active_flag: bool,
    bar_count: int,
) -> None:
    connection.execute(
        """
        INSERT INTO malf_wave_ledger (
            wave_nk,
            asset_type,
            code,
            timeframe,
            wave_id,
            direction,
            major_state,
            reversal_stage,
            start_bar_dt,
            end_bar_dt,
            active_flag,
            hh_count,
            ll_count,
            bar_count,
            wave_high,
            wave_low,
            range_ratio,
            first_seen_run_id,
            last_materialized_run_id
        )
        VALUES (?, 'stock', ?, 'D', ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 1.0, 1.0, 0.0, 'seed', 'seed')
        """,
        [
            f"{code}|D|wave|{wave_id}",
            code,
            wave_id,
            direction,
            major_state,
            reversal_stage,
            start_bar_dt,
            end_bar_dt,
            active_flag,
            bar_count,
        ],
    )


def _seed_state_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    code: str,
    wave_id: int,
    asof_bar_dt: str,
    trend_direction: str,
    major_state: str,
    reversal_stage: str,
    last_valid_hl_bar_dt: str | None,
    last_valid_hl_price: float | None,
    last_valid_lh_bar_dt: str | None,
    last_valid_lh_price: float | None,
) -> None:
    connection.execute(
        """
        INSERT INTO malf_state_snapshot (
            snapshot_nk,
            asset_type,
            code,
            timeframe,
            asof_bar_dt,
            major_state,
            trend_direction,
            reversal_stage,
            wave_id,
            last_valid_hl_bar_dt,
            last_valid_hl_price,
            last_valid_lh_bar_dt,
            last_valid_lh_price,
            current_hh_count,
            current_ll_count,
            first_seen_run_id,
            last_materialized_run_id
        )
        VALUES (?, 'stock', ?, 'D', ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 'seed', 'seed')
        """,
        [
            f"{code}|D|state|{wave_id}|{asof_bar_dt}",
            code,
            asof_bar_dt,
            major_state,
            trend_direction,
            reversal_stage,
            wave_id,
            last_valid_hl_bar_dt,
            last_valid_hl_price,
            last_valid_lh_bar_dt,
            last_valid_lh_price,
        ],
    )


def test_run_malf_zero_one_wave_audit_tags_three_categories_and_exports_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    settings = default_settings(repo_root=_bootstrap_repo_root(tmp_path))
    day_path = bootstrap_malf_ledger(settings=settings, timeframe="D")

    conn = duckdb.connect(str(day_path))
    try:
        _seed_wave_row(
            conn,
            code="ZERO.SZ",
            wave_id=1,
            direction="up",
            major_state="熊逆",
            reversal_stage="trigger",
            start_bar_dt="2026-01-02",
            end_bar_dt="2026-01-02",
            active_flag=False,
            bar_count=0,
        )
        _seed_wave_row(
            conn,
            code="STALE.SZ",
            wave_id=1,
            direction="up",
            major_state="熊逆",
            reversal_stage="trigger",
            start_bar_dt="2026-01-03",
            end_bar_dt="2026-01-06",
            active_flag=False,
            bar_count=1,
        )
        _seed_state_row(
            conn,
            code="STALE.SZ",
            wave_id=1,
            asof_bar_dt="2026-01-03",
            trend_direction="up",
            major_state="熊逆",
            reversal_stage="trigger",
            last_valid_hl_bar_dt=None,
            last_valid_hl_price=None,
            last_valid_lh_bar_dt="2025-01-01",
            last_valid_lh_price=10.0,
        )
        _seed_wave_row(
            conn,
            code="STALE.SZ",
            wave_id=2,
            direction="down",
            major_state="牛逆",
            reversal_stage="trigger",
            start_bar_dt="2026-01-06",
            end_bar_dt="2026-01-06",
            active_flag=True,
            bar_count=0,
        )
        _seed_wave_row(
            conn,
            code="FRESH.SZ",
            wave_id=1,
            direction="down",
            major_state="牛逆",
            reversal_stage="trigger",
            start_bar_dt="2026-01-04",
            end_bar_dt="2026-01-05",
            active_flag=False,
            bar_count=1,
        )
        _seed_state_row(
            conn,
            code="FRESH.SZ",
            wave_id=1,
            asof_bar_dt="2026-01-04",
            trend_direction="down",
            major_state="牛逆",
            reversal_stage="trigger",
            last_valid_hl_bar_dt="2026-01-02",
            last_valid_hl_price=9.5,
            last_valid_lh_bar_dt=None,
            last_valid_lh_price=None,
        )
        _seed_wave_row(
            conn,
            code="FRESH.SZ",
            wave_id=2,
            direction="up",
            major_state="熊逆",
            reversal_stage="trigger",
            start_bar_dt="2026-01-05",
            end_bar_dt="2026-01-05",
            active_flag=True,
            bar_count=0,
        )
    finally:
        conn.close()

    summary_path = settings.report_root / "malf" / "zero-one-summary.json"
    report_path = settings.report_root / "malf" / "zero-one-report.md"
    detail_path = settings.report_root / "malf" / "zero-one-detail.csv"

    summary = run_malf_zero_one_wave_audit(
        settings=settings,
        timeframes=["D"],
        stale_guard_age_days=250,
        sample_limit=5,
        summary_path=summary_path,
        report_path=report_path,
        detail_path=detail_path,
    )

    assert summary.total_short_wave_count == 3
    assert summary.same_bar_double_switch_count == 1
    assert summary.stale_guard_trigger_count == 1
    assert summary.next_bar_reflip_count == 1
    assert summary.non_immediate_boundary_count == 1
    assert summary.timeframe_summaries[0].timeframe == "D"
    assert summary.timeframe_summaries[0].same_bar_double_switch_count == 1
    assert summary.timeframe_summaries[0].stale_guard_trigger_count == 1
    assert summary.timeframe_summaries[0].next_bar_reflip_count == 1

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["total_short_wave_count"] == 3
    assert report_path.read_text(encoding="utf-8").startswith("# MALF 0/1 Wave Audit")

    detail_frame = duckdb.read_csv(str(detail_path))
    detail_rows = detail_frame.fetchall()
    detail_columns = [column[0] for column in detail_frame.description]
    category_index = detail_columns.index("category")
    code_index = detail_columns.index("code")
    has_state_index = detail_columns.index("has_state_at_start_flag")
    immediate_index = detail_columns.index("immediate_opposite_reflip_flag")
    rows_by_code = {row[code_index]: row for row in detail_rows}

    assert rows_by_code["ZERO.SZ"][category_index] == "same_bar_double_switch"
    assert rows_by_code["ZERO.SZ"][has_state_index] is False
    assert rows_by_code["ZERO.SZ"][immediate_index] is False
    assert rows_by_code["STALE.SZ"][category_index] == "stale_guard_trigger"
    assert rows_by_code["STALE.SZ"][immediate_index] is True
    assert rows_by_code["FRESH.SZ"][category_index] == "next_bar_reflip"
