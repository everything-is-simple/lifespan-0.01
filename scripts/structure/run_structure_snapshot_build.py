"""执行 `structure snapshot` 官方 producer 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.structure import (
    DEFAULT_STRUCTURE_CONTEXT_TABLE,
    DEFAULT_STRUCTURE_CONTRACT_VERSION,
    DEFAULT_STRUCTURE_INPUT_TABLE,
    run_structure_snapshot_build,
)


def build_argument_parser() -> argparse.ArgumentParser:
    """构建脚本命令行参数。"""

    parser = argparse.ArgumentParser(
        description="Build official structure snapshot facts from bounded MALF inputs.",
    )
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--source-context-table",
        default=DEFAULT_STRUCTURE_CONTEXT_TABLE,
    )
    parser.add_argument(
        "--source-structure-input-table",
        default=DEFAULT_STRUCTURE_INPUT_TABLE,
    )
    parser.add_argument(
        "--structure-contract-version",
        default=DEFAULT_STRUCTURE_CONTRACT_VERSION,
    )
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    """解析命令行并执行 producer。"""

    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_structure_snapshot_build(
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instruments=args.instruments,
        limit=args.limit,
        batch_size=args.batch_size,
        run_id=args.run_id,
        source_context_table=args.source_context_table,
        source_structure_input_table=args.source_structure_input_table,
        structure_contract_version=args.structure_contract_version,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
