"""执行 `filter objective coverage audit` 的只读脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.filter import run_filter_objective_coverage_audit


def build_argument_parser() -> argparse.ArgumentParser:
    """构建脚本命令行参数。"""

    parser = argparse.ArgumentParser(
        description="Audit historical objective-profile coverage for official filter snapshots.",
    )
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument(
        "--group-limit",
        type=int,
        default=50,
        help="Maximum number of grouped missing buckets to keep for each dimension.",
    )
    parser.add_argument("--summary-path", type=Path)
    parser.add_argument("--report-path", type=Path)
    return parser


def main() -> None:
    """解析命令行并执行只读 coverage audit。"""

    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_filter_objective_coverage_audit(
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        group_limit=args.group_limit,
        summary_path=args.summary_path,
        report_path=args.report_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
