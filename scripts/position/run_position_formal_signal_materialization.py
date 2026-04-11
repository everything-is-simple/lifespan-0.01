"""执行 `position` 对官方 `alpha formal signal` 的 bounded materialization。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.position import run_position_formal_signal_materialization


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run bounded position materialization from official alpha formal signals.",
    )
    parser.add_argument("--policy-id", required=True, type=str)
    parser.add_argument("--capital-base-value", required=True, type=float)
    parser.add_argument("--signal-start-date", type=str, default=None)
    parser.add_argument("--signal-end-date", type=str, default=None)
    parser.add_argument("--instrument", action="append", default=None)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--alpha-formal-signal-table", type=str, default="alpha_formal_signal_event")
    parser.add_argument("--market-price-table", type=str, default="stock_daily_adjusted")
    parser.add_argument("--adjust-method", type=str, default="none")
    parser.add_argument("--allow-same-day-price-fallback", action="store_true")
    parser.add_argument("--summary-path", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_position_formal_signal_materialization(
        policy_id=args.policy_id,
        capital_base_value=args.capital_base_value,
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instruments=args.instrument,
        limit=args.limit,
        run_id=args.run_id,
        alpha_formal_signal_table=args.alpha_formal_signal_table,
        market_price_table=args.market_price_table,
        adjust_method=args.adjust_method,
        allow_same_day_price_fallback=args.allow_same_day_price_fallback,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
