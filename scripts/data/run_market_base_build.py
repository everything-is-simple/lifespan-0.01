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
    parser.add_argument("--build-mode", choices=("full", "incremental"), default="full")
    parser.add_argument(
        "--consume-dirty-only",
        dest="consume_dirty_only",
        action="store_true",
        help="Only consume pending base_dirty_instrument rows.",
    )
    parser.add_argument(
        "--no-consume-dirty-only",
        dest="consume_dirty_only",
        action="store_false",
        help="Allow non-dirty scope build even in incremental mode.",
    )
    parser.set_defaults(consume_dirty_only=None)
    parser.add_argument(
        "--keep-dirty-on-success",
        dest="mark_clean_on_success",
        action="store_false",
        help="Do not mark consumed dirty rows as clean after a successful build.",
    )
    parser.set_defaults(mark_clean_on_success=True)
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
        build_mode=args.build_mode,
        consume_dirty_only=args.consume_dirty_only,
        mark_clean_on_success=args.mark_clean_on_success,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
