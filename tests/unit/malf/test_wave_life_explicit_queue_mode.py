"""覆盖 `wave_life` 官方脚本显式执行模式防呆。"""

from __future__ import annotations

from pathlib import Path

import pytest

from mlq.core.paths import default_settings
from mlq.malf import run_malf_wave_life_build
from tests.unit.malf.test_wave_life_runner import (
    _bootstrap_repo_root,
    _clear_workspace_env,
    _seed_wave_life_sources,
)


def test_run_malf_wave_life_build_requires_explicit_queue_mode_when_requested(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    settings = default_settings(repo_root=_bootstrap_repo_root(tmp_path))

    with pytest.raises(ValueError, match="explicit bounded window"):
        run_malf_wave_life_build(
            settings=settings,
            run_id="malf-wave-life-test-explicit-queue-001",
            require_explicit_queue_mode=True,
        )


def test_run_malf_wave_life_build_allows_explicit_checkpoint_queue_when_required(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    settings = default_settings(repo_root=_bootstrap_repo_root(tmp_path))
    _seed_wave_life_sources(
        settings.databases.malf,
        wave_rows=[
            (
                "wave-explicit-1",
                "stock",
                "000001.SZ",
                "D",
                1,
                "up",
                "牛顺",
                "none",
                "2026-03-01",
                "2026-03-08",
                False,
                None,
                None,
                2,
                0,
                8,
                11.0,
                9.0,
                0.18,
                "seed-explicit",
                "seed-explicit",
            ),
            (
                "wave-explicit-2",
                "stock",
                "000001.SZ",
                "D",
                2,
                "up",
                "牛顺",
                "none",
                "2026-04-10",
                None,
                True,
                None,
                None,
                1,
                0,
                2,
                12.0,
                10.0,
                0.16,
                "seed-explicit",
                "seed-explicit",
            ),
        ],
        state_rows=[
            ("state-explicit-10", "stock", "000001.SZ", "D", "2026-04-10", "牛顺", "up", "none", 2, 1, 0, "seed-explicit", "seed-explicit"),
            ("state-explicit-11", "stock", "000001.SZ", "D", "2026-04-11", "牛顺", "up", "none", 2, 1, 0, "seed-explicit", "seed-explicit"),
        ],
        checkpoint_rows=[
            ("stock", "000001.SZ", "D", "2026-04-11", "2026-04-10", "2026-04-11", 2, "malf-run-explicit"),
        ],
        stats_rows=[
            (
                "stats-explicit-1",
                "stock:000001.SZ",
                "D",
                "牛顺",
                "wave_duration_bars",
                "malf-wave-stats-v1",
                5,
                5.0,
                6.0,
                8.0,
                10.0,
                12.0,
                8.2,
                2.1,
                "seed-explicit",
                "seed-explicit",
            ),
        ],
    )

    summary = run_malf_wave_life_build(
        settings=settings,
        timeframes=["D"],
        run_id="malf-wave-life-test-explicit-queue-002",
        use_checkpoint_queue=True,
        require_explicit_queue_mode=True,
    )

    assert summary.execution_mode == "checkpoint_queue"
    assert summary.queue_claimed_count == 1
    assert summary.checkpoint_upserted_count == 1
