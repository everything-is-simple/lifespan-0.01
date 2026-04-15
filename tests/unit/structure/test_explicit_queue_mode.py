from __future__ import annotations

from pathlib import Path

import pytest

from mlq.core.paths import default_settings
from mlq.structure import run_structure_snapshot_build
from tests.unit.structure.test_runner import (
    _bootstrap_repo_root,
    _clear_workspace_env,
    _seed_canonical_malf_checkpoints,
    _seed_canonical_malf_state_rows,
)


# 这些用例专门覆盖 61 卡新增的正式脚本入口防呆。
def test_run_structure_snapshot_build_requires_explicit_queue_mode_when_requested(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    settings = default_settings(repo_root=_bootstrap_repo_root(tmp_path))

    with pytest.raises(ValueError, match="explicit bounded window"):
        run_structure_snapshot_build(
            settings=settings,
            run_id="structure-snapshot-test-explicit-queue-001",
            require_explicit_queue_mode=True,
        )


def test_run_structure_snapshot_build_allows_explicit_checkpoint_queue_when_required(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    settings = default_settings(repo_root=_bootstrap_repo_root(tmp_path))
    _seed_canonical_malf_state_rows(
        settings.databases.malf,
        [
            ("state-d-eq1", "000001.SZ", "D", "2026-04-08", "\u725b\u987a", "up", "none", 7, 2, 0),
            ("state-w-eq1", "000001.SZ", "W", "2026-04-04", "\u725b\u987a", "up", "expand", 3, 1, 0),
            ("state-m-eq1", "000001.SZ", "M", "2026-03-31", "\u725b\u9006", "down", "trigger", 1, 0, 1),
        ],
    )
    _seed_canonical_malf_checkpoints(
        settings.databases.malf,
        [
            ("stock", "000001.SZ", "D", "2026-04-08", "2026-04-08", "2026-04-08", 7, "malf-run-explicit"),
            ("stock", "000001.SZ", "W", "2026-04-04", "2026-04-04", "2026-04-04", 3, "malf-run-explicit"),
            ("stock", "000001.SZ", "M", "2026-03-31", "2026-03-31", "2026-03-31", 1, "malf-run-explicit"),
        ],
    )

    summary = run_structure_snapshot_build(
        settings=settings,
        run_id="structure-snapshot-test-explicit-queue-002",
        use_checkpoint_queue=True,
        require_explicit_queue_mode=True,
    )

    assert summary.execution_mode == "checkpoint_queue"
    assert summary.queue_claimed_count == 1
    assert summary.checkpoint_upserted_count == 1
