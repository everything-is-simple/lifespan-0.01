"""执行 `malf 0/1 wave` 只读分类审计的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.malf import run_malf_zero_one_wave_audit


def build_argument_parser() -> argparse.ArgumentParser:
    """构建命令行参数。"""

    parser = argparse.ArgumentParser(
        description="Audit official malf day/week/month ledgers and tag every completed 0/1 wave.",
    )
    parser.add_argument(
        "--timeframes",
        nargs="*",
        default=None,
        help="Optional timeframe subset, for example: --timeframes D W M",
    )
    parser.add_argument(
        "--stale-guard-age-days",
        type=int,
        default=250,
        help="Classify one-bar waves as stale-guard-trigger when the relevant guard age reaches this many days.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help="Maximum number of representative samples to retain for each category in the summary/report.",
    )
    parser.add_argument("--summary-path", type=Path)
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--detail-path", type=Path)
    return parser


def main() -> None:
    """解析参数并执行只读审计。"""

    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_malf_zero_one_wave_audit(
        timeframes=args.timeframes,
        stale_guard_age_days=args.stale_guard_age_days,
        sample_limit=args.sample_limit,
        summary_path=args.summary_path,
        report_path=args.report_path,
        detail_path=args.detail_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
