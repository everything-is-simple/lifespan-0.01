"""`system` 主线 readout 正式 materialization 的 CLI 入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.system import (
    DEFAULT_SYSTEM_CONTRACT_VERSION,
    DEFAULT_SYSTEM_SCENE,
    run_system_mainline_readout_build,
)


def build_argument_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""

    parser = argparse.ArgumentParser(
        description="Build official system mainline readout facts from bounded upstream ledgers.",
    )
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument("--snapshot-date", dest="snapshot_date")
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--run-id")
    parser.add_argument("--system-scene", default=DEFAULT_SYSTEM_SCENE)
    parser.add_argument(
        "--system-contract-version",
        default=DEFAULT_SYSTEM_CONTRACT_VERSION,
    )
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    """解析命令行参数并执行正式 builder。"""

    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_system_mainline_readout_build(
        portfolio_id=args.portfolio_id,
        snapshot_date=args.snapshot_date,
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        run_id=args.run_id,
        system_scene=args.system_scene,
        system_contract_version=args.system_contract_version,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
