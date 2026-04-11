"""覆盖 `market_base -> malf -> structure` 最小正式桥接。"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.malf import malf_ledger_path, run_malf_snapshot_build
from mlq.structure import run_structure_snapshot_build, structure_ledger_path


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


def _seed_market_base_rows(market_base_path: Path) -> None:
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
                close DOUBLE
            )
            """
        )
        start_date = date(2025, 11, 1)
        for offset in range(150):
            trade_date = start_date + timedelta(days=offset)
            base_price = 10.0 + offset * 0.1
            conn.execute(
                "INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "600000.SH",
                    "浦发银行",
                    trade_date,
                    "backward",
                    base_price,
                    base_price + 0.3,
                    base_price - 0.2,
                    base_price + 0.2,
                ],
            )
    finally:
        conn.close()


def test_run_malf_snapshot_build_outputs_rows_consumable_by_structure(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_market_base_rows(settings.databases.market_base)

    first_summary = run_malf_snapshot_build(
        settings=settings,
        signal_start_date="2026-03-20",
        signal_end_date="2026-03-22",
        run_id="malf-test-001a",
    )
    second_summary = run_malf_snapshot_build(
        settings=settings,
        signal_start_date="2026-03-20",
        signal_end_date="2026-03-22",
        run_id="malf-test-001b",
    )

    assert first_summary.context_snapshot_count == 3
    assert first_summary.structure_candidate_count == 3
    assert first_summary.context_inserted_count == 3
    assert second_summary.context_reused_count == 3

    structure_summary = run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-03-20",
        signal_end_date="2026-03-22",
        run_id="structure-test-001",
        # bridge v1 兼容回退：此测试只 seed 了 pas_context_snapshot /
        # structure_candidate_snapshot，没有 canonical malf 数据；
        # 显式指定两个上游表以验证 bridge v1 路径仍可用。
        source_context_table="pas_context_snapshot",
        source_structure_input_table="structure_candidate_snapshot",
    )
    assert structure_summary.materialized_snapshot_count == 3

    malf_conn = duckdb.connect(str(malf_ledger_path(settings)), read_only=True)
    structure_conn = duckdb.connect(str(structure_ledger_path(settings)), read_only=True)
    try:
        context_rows = malf_conn.execute(
            """
            SELECT entity_code, malf_context_4, lifecycle_rank_total
            FROM pas_context_snapshot
            ORDER BY signal_date
            """
        ).fetchall()
        structure_rows = structure_conn.execute(
            """
            SELECT instrument, structure_progress_state
            FROM structure_snapshot
            ORDER BY signal_date
            """
        ).fetchall()
    finally:
        malf_conn.close()
        structure_conn.close()

    assert all(row == ("600000.SH", "BULL_MAINSTREAM", 4) for row in context_rows)
    assert len(structure_rows) == 3


def test_run_malf_snapshot_build_marks_rematerialized_when_market_base_changes(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_market_base_rows(settings.databases.market_base)

    first_summary = run_malf_snapshot_build(
        settings=settings,
        signal_start_date="2026-03-22",
        signal_end_date="2026-03-22",
        run_id="malf-test-002a",
    )
    assert first_summary.structure_inserted_count == 1

    conn = duckdb.connect(str(settings.databases.market_base))
    try:
        conn.execute(
            """
            UPDATE stock_daily_adjusted
            SET open = 60.0,
                close = 59.0
            WHERE code = '600000.SH'
              AND trade_date = DATE '2026-03-22'
              AND adjust_method = 'backward'
            """
        )
    finally:
        conn.close()

    second_summary = run_malf_snapshot_build(
        settings=settings,
        signal_start_date="2026-03-22",
        signal_end_date="2026-03-22",
        run_id="malf-test-002b",
    )

    assert second_summary.context_rematerialized_count == 1 or second_summary.structure_rematerialized_count == 1
