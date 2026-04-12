"""覆盖 `malf` 波段寿命概率 sidecar runner。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.malf import bootstrap_malf_ledger, malf_ledger_path, run_malf_wave_life_build


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


def _seed_wave_life_sources(
    malf_path: Path,
    *,
    wave_rows: list[tuple[object, ...]],
    state_rows: list[tuple[object, ...]],
    checkpoint_rows: list[tuple[object, ...]],
    stats_rows: list[tuple[object, ...]] | None = None,
) -> None:
    malf_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(malf_path))
    try:
        bootstrap_malf_ledger(connection=conn)
        for row in wave_rows:
            conn.execute(
                """
                INSERT INTO malf_wave_ledger (
                    wave_nk, asset_type, code, timeframe, wave_id, direction, major_state, reversal_stage,
                    start_bar_dt, end_bar_dt, active_flag, start_pivot_nk, end_pivot_nk, hh_count, ll_count,
                    bar_count, wave_high, wave_low, range_ratio, first_seen_run_id, last_materialized_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        for row in state_rows:
            conn.execute(
                """
                INSERT INTO malf_state_snapshot (
                    snapshot_nk, asset_type, code, timeframe, asof_bar_dt, major_state, trend_direction,
                    reversal_stage, wave_id, current_hh_count, current_ll_count, first_seen_run_id, last_materialized_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        for row in checkpoint_rows:
            conn.execute(
                """
                INSERT INTO malf_canonical_checkpoint (
                    asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt,
                    tail_confirm_until_dt, last_wave_id, last_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        for row in stats_rows or []:
            conn.execute(
                """
                INSERT INTO malf_same_level_stats (
                    stats_nk, universe, timeframe, major_state, metric_name, sample_version, sample_size,
                    p10, p25, p50, p75, p90, mean, std, first_seen_run_id, last_materialized_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
    finally:
        conn.close()


def test_run_malf_wave_life_build_separates_completed_profile_and_active_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_wave_life_sources(
        settings.databases.malf,
        wave_rows=[
            ("wave-1", "stock", "000001.SZ", "D", 1, "up", "牛顺", "none", "2026-03-01", "2026-03-10", False, None, None, 2, 0, 10, 12.0, 9.0, 0.25, "seed-a", "seed-a"),
            ("wave-2", "stock", "000001.SZ", "D", 2, "up", "牛顺", "none", "2026-03-11", "2026-03-24", False, None, None, 3, 0, 14, 14.0, 10.0, 0.28, "seed-a", "seed-a"),
            ("wave-3", "stock", "000001.SZ", "D", 3, "up", "牛顺", "none", "2026-04-10", None, True, None, None, 1, 0, 2, 15.0, 12.0, 0.20, "seed-a", "seed-a"),
        ],
        state_rows=[
            ("state-2026-04-10", "stock", "000001.SZ", "D", "2026-04-10", "牛顺", "up", "none", 3, 1, 0, "seed-a", "seed-a"),
            ("state-2026-04-11", "stock", "000001.SZ", "D", "2026-04-11", "牛顺", "up", "none", 3, 1, 0, "seed-a", "seed-a"),
        ],
        checkpoint_rows=[
            ("stock", "000001.SZ", "D", "2026-04-11", "2026-04-10", "2026-04-11", 3, "malf-run-a"),
        ],
    )

    summary = run_malf_wave_life_build(
        settings=settings,
        signal_start_date="2026-04-10",
        signal_end_date="2026-04-11",
        instruments=["000001.SZ"],
        timeframes=["D"],
        run_id="malf-wave-life-test-001",
    )

    assert summary.execution_mode == "bounded_window"
    assert summary.profile_row_count == 1
    assert summary.snapshot_row_count == 2
    assert summary.completed_wave_sample_count == 2

    conn = duckdb.connect(str(malf_ledger_path(settings)), read_only=True)
    try:
        profile_row = conn.execute(
            """
            SELECT sample_size, p50, p75, profile_origin
            FROM malf_wave_life_profile
            WHERE timeframe = 'D' AND major_state = '牛顺' AND reversal_stage = 'none'
            """
        ).fetchone()
        snapshot_rows = conn.execute(
            """
            SELECT asof_bar_dt, active_wave_bar_age, wave_life_percentile, remaining_life_bars_p50, remaining_life_bars_p75
            FROM malf_wave_life_snapshot
            ORDER BY asof_bar_dt
            """
        ).fetchall()
    finally:
        conn.close()

    assert profile_row == (2, 12.0, 13.0, "completed_wave_sample")
    assert snapshot_rows == [
        (date(2026, 4, 10), 1, 0.0, 11.0, 12.0),
        (date(2026, 4, 11), 2, 0.0, 10.0, 11.0),
    ]


def test_run_malf_wave_life_build_uses_queue_and_requeues_on_source_fingerprint_change(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_wave_life_sources(
        settings.databases.malf,
        wave_rows=[
            ("wave-q1", "stock", "000001.SZ", "D", 1, "up", "牛顺", "none", "2026-03-01", "2026-03-08", False, None, None, 2, 0, 8, 11.0, 9.0, 0.18, "seed-q", "seed-q"),
            ("wave-q2", "stock", "000001.SZ", "D", 2, "up", "牛顺", "none", "2026-04-10", None, True, None, None, 1, 0, 2, 12.0, 10.0, 0.16, "seed-q", "seed-q"),
        ],
        state_rows=[
            ("state-q-10", "stock", "000001.SZ", "D", "2026-04-10", "牛顺", "up", "none", 2, 1, 0, "seed-q", "seed-q"),
            ("state-q-11", "stock", "000001.SZ", "D", "2026-04-11", "牛顺", "up", "none", 2, 1, 0, "seed-q", "seed-q"),
        ],
        checkpoint_rows=[
            ("stock", "000001.SZ", "D", "2026-04-11", "2026-04-10", "2026-04-11", 2, "malf-run-a"),
        ],
        stats_rows=[
            ("stats-q-1", "stock:000001.SZ", "D", "牛顺", "wave_duration_bars", "malf-wave-stats-v1", 5, 5.0, 6.0, 8.0, 10.0, 12.0, 8.2, 2.1, "seed-q", "seed-q"),
        ],
    )

    first_summary = run_malf_wave_life_build(
        settings=settings,
        timeframes=["D"],
        run_id="malf-wave-life-test-queue-001a",
    )

    assert first_summary.execution_mode == "checkpoint_queue"
    assert first_summary.queue_claimed_count == 1
    assert first_summary.checkpoint_upserted_count == 1

    conn = duckdb.connect(str(settings.databases.malf))
    try:
        conn.execute(
            """
            UPDATE malf_wave_ledger
            SET reversal_stage = 'trigger', last_materialized_run_id = 'malf-run-b'
            WHERE wave_nk = 'wave-q2'
            """
        )
        conn.execute(
            """
            UPDATE malf_state_snapshot
            SET reversal_stage = 'trigger', last_materialized_run_id = 'malf-run-b'
            WHERE code = '000001.SZ' AND timeframe = 'D'
            """
        )
        conn.execute(
            """
            UPDATE malf_canonical_checkpoint
            SET last_run_id = 'malf-run-b', updated_at = CURRENT_TIMESTAMP
            WHERE code = '000001.SZ' AND timeframe = 'D'
            """
        )
    finally:
        conn.close()

    second_summary = run_malf_wave_life_build(
        settings=settings,
        timeframes=["D"],
        run_id="malf-wave-life-test-queue-001b",
    )

    assert second_summary.queue_claimed_count == 1
    assert second_summary.snapshot_rematerialized_count == 2
    assert second_summary.fallback_profile_count >= 1

    conn = duckdb.connect(str(malf_ledger_path(settings)), read_only=True)
    try:
        queue_row = conn.execute(
            """
            SELECT queue_status
            FROM malf_wave_life_work_queue
            WHERE code = '000001.SZ'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ).fetchone()
        checkpoint_row = conn.execute(
            """
            SELECT last_run_id, last_sample_version
            FROM malf_wave_life_checkpoint
            WHERE code = '000001.SZ' AND timeframe = 'D'
            """
        ).fetchone()
        snapshot_rows = conn.execute(
            """
            SELECT reversal_stage, last_materialized_run_id
            FROM malf_wave_life_snapshot
            ORDER BY asof_bar_dt
            """
        ).fetchall()
    finally:
        conn.close()

    assert queue_row == ("completed",)
    assert checkpoint_row == ("malf-wave-life-test-queue-001b", "wave-life-v1")
    assert snapshot_rows == [
        ("trigger", "malf-wave-life-test-queue-001b"),
        ("trigger", "malf-wave-life-test-queue-001b"),
    ]
