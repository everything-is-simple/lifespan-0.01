"""CLI entrypoint for bounded `portfolio_plan` materialization."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.portfolio_plan import (
    DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION,
    DEFAULT_SOURCE_POSITION_TABLE,
    run_portfolio_plan_build,
)


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""

    parser = argparse.ArgumentParser(
        description="Build official portfolio plan facts from bounded position inputs.",
    )
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument("--portfolio-gross-cap-weight", type=float, required=True)
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--candidate-nk", dest="candidate_nks", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--source-position-table",
        default=DEFAULT_SOURCE_POSITION_TABLE,
    )
    parser.add_argument(
        "--portfolio-plan-contract-version",
        default=DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION,
    )
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    """Parse CLI arguments and run the builder."""

    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_portfolio_plan_build(
        portfolio_id=args.portfolio_id,
        portfolio_gross_cap_weight=args.portfolio_gross_cap_weight,
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instruments=args.instruments,
        candidate_nks=args.candidate_nks,
        limit=args.limit,
        run_id=args.run_id,
        source_position_table=args.source_position_table,
        portfolio_plan_contract_version=args.portfolio_plan_contract_version,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
