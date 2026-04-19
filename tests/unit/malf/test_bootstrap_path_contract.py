"""覆盖 `79` 的 malf 三库路径与 bootstrap 契约。"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from mlq.core.paths import default_settings
from mlq.malf.bootstrap import (
    MALF_LEDGER_CONTRACT_TABLE,
    bootstrap_malf_ledger,
    malf_ledger_path,
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


def test_default_settings_exposes_official_and_legacy_malf_paths(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    settings = default_settings(repo_root=_bootstrap_repo_root(tmp_path))

    assert settings.databases.malf_day.name == "malf_day.duckdb"
    assert settings.databases.malf_week.name == "malf_week.duckdb"
    assert settings.databases.malf_month.name == "malf_month.duckdb"
    assert settings.databases.malf_legacy.name == "malf.duckdb"
    assert malf_ledger_path(settings, timeframe="D") == settings.databases.malf_day
    assert malf_ledger_path(settings, timeframe="W") == settings.databases.malf_week
    assert malf_ledger_path(settings, timeframe="M") == settings.databases.malf_month
    assert malf_ledger_path(settings) == settings.databases.malf_legacy


def test_bootstrap_native_malf_ledger_enforces_single_timeframe_contract(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    settings = default_settings(repo_root=_bootstrap_repo_root(tmp_path))
    week_path = bootstrap_malf_ledger(settings=settings, timeframe="W")

    conn = duckdb.connect(str(week_path))
    try:
        contract_row = conn.execute(
            f"""
            SELECT storage_mode, native_timeframe
            FROM {MALF_LEDGER_CONTRACT_TABLE}
            WHERE contract_key = 'malf'
            """
        ).fetchone()
        conn.execute(
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
                current_hh_count,
                current_ll_count,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (
                'state-week-001',
                'stock',
                '000001.SZ',
                'W',
                DATE '2026-04-18',
                'bull',
                'up',
                'none',
                1,
                1,
                0,
                'seed-week',
                'seed-week'
            )
            """
        )
        with pytest.raises(duckdb.ConstraintException):
            conn.execute(
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
                    current_hh_count,
                    current_ll_count,
                    first_seen_run_id,
                    last_materialized_run_id
                )
                VALUES (
                    'state-week-002',
                    'stock',
                    '000001.SZ',
                    'D',
                    DATE '2026-04-18',
                    'bull',
                    'up',
                    'none',
                    1,
                    1,
                    0,
                    'seed-week',
                    'seed-week'
                )
                """
            )
    finally:
        conn.close()

    assert contract_row == ("official_native", "W")


def test_bootstrap_legacy_malf_ledger_keeps_single_db_fallback_open(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    settings = default_settings(repo_root=_bootstrap_repo_root(tmp_path))
    legacy_path = bootstrap_malf_ledger(settings=settings, use_legacy=True)

    conn = duckdb.connect(str(legacy_path))
    try:
        contract_row = conn.execute(
            f"""
            SELECT storage_mode, native_timeframe
            FROM {MALF_LEDGER_CONTRACT_TABLE}
            WHERE contract_key = 'malf'
            """
        ).fetchone()
        conn.execute(
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
                current_hh_count,
                current_ll_count,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES
                ('state-legacy-d', 'stock', '000001.SZ', 'D', DATE '2026-04-18', 'bull', 'up', 'none', 1, 1, 0, 'seed-legacy', 'seed-legacy'),
                ('state-legacy-w', 'stock', '000001.SZ', 'W', DATE '2026-04-18', 'bull', 'up', 'none', 1, 1, 0, 'seed-legacy', 'seed-legacy')
            """
        )
        timeframe_rows = conn.execute(
            """
            SELECT timeframe
            FROM malf_state_snapshot
            ORDER BY timeframe
            """
        ).fetchall()
    finally:
        conn.close()

    assert contract_row == ("legacy_compat", None)
    assert timeframe_rows == [("D",), ("W",)]
