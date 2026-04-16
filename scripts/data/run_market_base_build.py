"""执行 `raw_market -> market_base` 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data import run_asset_market_base_build, run_asset_market_base_build_batched


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build market_base daily_adjusted from raw_market by asset type.")
    parser.add_argument("--asset-type", choices=("stock", "index", "block"), default="stock")
    parser.add_argument("--timeframe", choices=("day", "week", "month"), default="day")
    parser.add_argument("--adjust-method", default="backward")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Split full bootstrap into instrument batches. 0 disables batching.",
    )
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
    if args.batch_size > 0:
        summary_payload = run_asset_market_base_build_batched(
            asset_type=args.asset_type,
            timeframe=args.timeframe,
            adjust_method=args.adjust_method,
            instruments=args.instruments,
            start_date=args.start_date,
            end_date=args.end_date,
            batch_size=args.batch_size,
            build_mode=args.build_mode,
            run_id=args.run_id,
            summary_path=args.summary_path,
        )
    else:
        summary = run_asset_market_base_build(
            asset_type=args.asset_type,
            timeframe=args.timeframe,
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
        summary_payload = summary.as_dict()
    print(json.dumps(summary_payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
