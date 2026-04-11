"""覆盖 `position` CLI 入口默认价格口径。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "position"
    / "run_position_formal_signal_materialization.py"
)


def _load_cli_module():
    spec = importlib.util.spec_from_file_location("position_cli_entrypoint", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_position_cli_defaults_to_none_adjust_method(monkeypatch) -> None:
    module = _load_cli_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "--policy-id",
            "fixed_notional_full_exit_v1",
            "--capital-base-value",
            "1000000",
        ],
    )

    args = module.parse_args()

    assert args.adjust_method == "none"
