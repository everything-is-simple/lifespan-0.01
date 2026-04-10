"""执行 `TdxQuant(none) -> raw_market` 日更桥接的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data import run_tdxquant_daily_raw_sync


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bridge bounded TdxQuant none daily bars into raw_market.")
    parser.add_argument("--strategy-path", type=Path, required=True)
    parser.add_argument("--instrument", dest="onboarding_instruments", action="append", default=[])
    parser.add_argument(
        "--no-registry-scope",
        dest="use_registry_scope",
        action="store_false",
        help="Only consume explicitly onboarded instruments.",
    )
    parser.set_defaults(use_registry_scope=True)
    parser.add_argument("--end-trade-date")
    parser.add_argument("--count", type=int, default=120)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--no-continue-from-checkpoint", dest="continue_from_checkpoint", action="store_false")
    parser.set_defaults(continue_from_checkpoint=True)
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_tdxquant_daily_raw_sync(
        strategy_path=args.strategy_path,
        onboarding_instruments=args.onboarding_instruments,
        use_registry_scope=args.use_registry_scope,
        end_trade_date=args.end_trade_date,
        count=args.count,
        limit=args.limit,
        continue_from_checkpoint=args.continue_from_checkpoint,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
