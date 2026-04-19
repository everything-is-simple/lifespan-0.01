"""执行 `alpha trigger ledger` bounded runner 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.alpha import (
    DEFAULT_ALPHA_TRIGGER_CONTRACT_VERSION,
    DEFAULT_ALPHA_TRIGGER_FILTER_TABLE,
    DEFAULT_ALPHA_TRIGGER_INPUT_TABLE,
    DEFAULT_ALPHA_TRIGGER_STRUCTURE_TABLE,
    run_alpha_trigger_build,
)


def build_argument_parser() -> argparse.ArgumentParser:
    """构建脚本命令行参数。"""

    parser = argparse.ArgumentParser(
        description="Build official alpha trigger ledger facts from bounded detector inputs.",
    )
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--source-trigger-input-table",
        default=DEFAULT_ALPHA_TRIGGER_INPUT_TABLE,
    )
    parser.add_argument(
        "--source-filter-table",
        default=DEFAULT_ALPHA_TRIGGER_FILTER_TABLE,
    )
    parser.add_argument(
        "--source-structure-table",
        default=DEFAULT_ALPHA_TRIGGER_STRUCTURE_TABLE,
    )
    parser.add_argument(
        "--trigger-contract-version",
        default=DEFAULT_ALPHA_TRIGGER_CONTRACT_VERSION,
    )
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    """解析命令行并执行 runner。"""

    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_alpha_trigger_build(
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instruments=args.instruments,
        limit=args.limit,
        batch_size=args.batch_size,
        run_id=args.run_id,
        source_trigger_input_table=args.source_trigger_input_table,
        source_filter_table=args.source_filter_table,
        source_structure_table=args.source_structure_table,
        trigger_contract_version=args.trigger_contract_version,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
