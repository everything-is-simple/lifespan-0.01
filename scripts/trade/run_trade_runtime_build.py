"""CLI entrypoint for bounded `portfolio_plan -> trade_runtime` materialization."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.trade import (
    DEFAULT_MARKET_PRICE_TABLE,
    DEFAULT_SOURCE_PORTFOLIO_PLAN_TABLE,
    DEFAULT_TRADE_CONTRACT_VERSION,
    run_trade_runtime_build,
)


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""

    parser = argparse.ArgumentParser(
        description="Build official trade runtime facts from bounded portfolio plan inputs.",
    )
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--source-portfolio-plan-table",
        default=DEFAULT_SOURCE_PORTFOLIO_PLAN_TABLE,
    )
    parser.add_argument(
        "--market-price-table",
        default=DEFAULT_MARKET_PRICE_TABLE,
    )
    parser.add_argument(
        "--trade-contract-version",
        default=DEFAULT_TRADE_CONTRACT_VERSION,
    )
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    """Parse CLI arguments and run the builder."""

    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_trade_runtime_build(
        portfolio_id=args.portfolio_id,
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instruments=args.instruments,
        limit=args.limit,
        run_id=args.run_id,
        source_portfolio_plan_table=args.source_portfolio_plan_table,
        market_price_table=args.market_price_table,
        trade_contract_version=args.trade_contract_version,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
