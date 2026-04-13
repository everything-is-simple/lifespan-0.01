"""运行 alpha PAS 五触发官方 detector。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.alpha import (
    DEFAULT_ALPHA_PAS_TRIGGER_ADJUST_METHOD,
    DEFAULT_ALPHA_PAS_TRIGGER_CONTRACT_VERSION,
    DEFAULT_ALPHA_PAS_TRIGGER_FILTER_TABLE,
    DEFAULT_ALPHA_PAS_TRIGGER_PRICE_TABLE,
    DEFAULT_ALPHA_PAS_TRIGGER_STRUCTURE_TABLE,
    run_alpha_pas_five_trigger_build,
)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build official alpha PAS five-trigger detector candidates from filter/structure/market_base.",
    )
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--run-id")
    parser.add_argument("--source-filter-table", default=DEFAULT_ALPHA_PAS_TRIGGER_FILTER_TABLE)
    parser.add_argument("--source-structure-table", default=DEFAULT_ALPHA_PAS_TRIGGER_STRUCTURE_TABLE)
    parser.add_argument("--source-price-table", default=DEFAULT_ALPHA_PAS_TRIGGER_PRICE_TABLE)
    parser.add_argument("--source-adjust-method", default=DEFAULT_ALPHA_PAS_TRIGGER_ADJUST_METHOD)
    parser.add_argument("--detector-contract-version", default=DEFAULT_ALPHA_PAS_TRIGGER_CONTRACT_VERSION)
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_alpha_pas_five_trigger_build(
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instruments=args.instruments,
        limit=args.limit,
        run_id=args.run_id,
        source_filter_table=args.source_filter_table,
        source_structure_table=args.source_structure_table,
        source_price_table=args.source_price_table,
        source_adjust_method=args.source_adjust_method,
        detector_contract_version=args.detector_contract_version,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
