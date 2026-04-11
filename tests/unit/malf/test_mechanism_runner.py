"""覆盖 `malf` 机制层 sidecar bounded runner。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.malf import (
    bootstrap_malf_ledger,
    malf_ledger_path,
    run_malf_mechanism_build,
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


def _seed_bridge_inputs(malf_path: Path) -> None:
    malf_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(malf_path))
    try:
        bootstrap_malf_ledger(connection=conn)
        context_rows = [
            ("ctx-001", "000001.SZ", "样本一", "2026-04-01", "2026-04-01", "ctx-001", "BULL_MAINSTREAM", 1, 4),
            ("ctx-002", "000001.SZ", "样本一", "2026-04-02", "2026-04-02", "ctx-002", "BULL_MAINSTREAM", 1, 4),
            ("ctx-003", "000001.SZ", "样本一", "2026-04-03", "2026-04-03", "ctx-003", "BULL_MAINSTREAM", 1, 4),
            ("ctx-004", "000001.SZ", "样本一", "2026-04-04", "2026-04-04", "ctx-004", "BULL_MAINSTREAM", 1, 4),
        ]
        for row in context_rows:
            conn.execute(
                """
                INSERT INTO pas_context_snapshot (
                    context_nk, entity_code, entity_name, signal_date, asof_date,
                    source_context_nk, malf_context_4, lifecycle_rank_high, lifecycle_rank_total
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        structure_rows = [
            ("cand-001", "000001.SZ", "样本一", "2026-04-01", "2026-04-01", 1, 0, 0.60, 0.55, False, None),
            ("cand-002", "000001.SZ", "样本一", "2026-04-02", "2026-04-02", 0, 1, 0.10, 0.20, True, "failed_breakdown"),
            ("cand-003", "000001.SZ", "样本一", "2026-04-03", "2026-04-03", 0, 1, 0.05, 0.10, False, None),
            ("cand-004", "000001.SZ", "样本一", "2026-04-04", "2026-04-04", 1, 0, 0.70, 0.65, False, None),
        ]
        for row in structure_rows:
            conn.execute(
                """
                INSERT INTO structure_candidate_snapshot (
                    candidate_nk, instrument, instrument_name, signal_date, asof_date,
                    new_high_count, new_low_count, refresh_density, advancement_density,
                    is_failed_extreme, failure_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
    finally:
        conn.close()


def test_run_malf_mechanism_build_materializes_break_stats_and_checkpoint(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_bridge_inputs(settings.databases.malf)

    summary = run_malf_mechanism_build(
        settings=settings,
        signal_start_date="2026-04-01",
        signal_end_date="2026-04-04",
        run_id="malf-mechanism-test-001",
    )

    assert summary.source_candidate_count == 4
    assert summary.break_ledger_count == 1
    assert summary.confirmed_break_count == 1
    assert summary.stats_profile_count == 4
    assert summary.stats_snapshot_count == 4
    assert summary.checkpoint_upserted_count == 1

    conn = duckdb.connect(str(malf_ledger_path(settings)), read_only=True)
    try:
        break_row = conn.execute(
            """
            SELECT confirmation_status, break_direction
            FROM pivot_confirmed_break_ledger
            """
        ).fetchone()
        snapshot_row = conn.execute(
            """
            SELECT exhaustion_risk_bucket, reversal_probability_bucket
            FROM same_timeframe_stats_snapshot
            WHERE instrument = '000001.SZ' AND asof_bar_dt = DATE '2026-04-02'
            """
        ).fetchone()
        checkpoint_row = conn.execute(
            """
            SELECT last_signal_date, last_asof_date
            FROM malf_mechanism_checkpoint
            WHERE instrument = '000001.SZ' AND timeframe = 'D'
            """
        ).fetchone()
    finally:
        conn.close()

    assert break_row == ("confirmed", "DOWN")
    assert snapshot_row[0] in {"normal", "elevated", "high"}
    assert snapshot_row[1] in {"normal", "elevated", "high"}
    assert checkpoint_row == (date(2026, 4, 4), date(2026, 4, 4))


def test_run_malf_mechanism_build_uses_checkpoint_for_incremental_rerun(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_bridge_inputs(settings.databases.malf)

    first_summary = run_malf_mechanism_build(settings=settings, run_id="malf-mechanism-test-002a")
    second_summary = run_malf_mechanism_build(settings=settings, run_id="malf-mechanism-test-002b")

    assert first_summary.source_candidate_count == 4
    assert second_summary.source_candidate_count == 0
    assert second_summary.break_ledger_count == 0
    assert second_summary.stats_snapshot_count == 0
