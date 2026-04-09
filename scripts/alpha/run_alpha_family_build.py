"""执行 `alpha family ledger` bounded runner 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.alpha import (
    ALPHA_FAMILY_SCOPE_ALL,
    DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE,
    DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION,
    DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE,
    run_alpha_family_build,
)


def build_argument_parser() -> argparse.ArgumentParser:
    """构建脚本命令行参数。"""

    parser = argparse.ArgumentParser(
        description="Build official alpha family ledger facts from bounded trigger inputs.",
    )
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--family", dest="family_scope", action="append", default=[])
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--source-trigger-table",
        default=DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE,
    )
    parser.add_argument(
        "--source-candidate-table",
        default=DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE,
    )
    parser.add_argument(
        "--family-contract-version",
        default=DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION,
    )
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    """解析命令行并执行 runner。"""

    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_alpha_family_build(
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        family_scope=args.family_scope or list(ALPHA_FAMILY_SCOPE_ALL),
        instruments=args.instruments,
        limit=args.limit,
        batch_size=args.batch_size,
        run_id=args.run_id,
        source_trigger_table=args.source_trigger_table,
        source_candidate_table=args.source_candidate_table,
        family_contract_version=args.family_contract_version,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
