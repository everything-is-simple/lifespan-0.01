"""执行 `raw_market -> market_base` 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data import run_market_base_build


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build market_base stock_daily_adjusted from raw_market.")
    parser.add_argument("--adjust-method", default="backward")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_market_base_build(
        adjust_method=args.adjust_method,
        instruments=args.instruments,
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
