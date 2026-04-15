"""执行 `Tushare objective source -> source ledger` 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data import run_tushare_objective_source_sync


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync bounded Tushare objective events into raw_market source ledgers.")
    parser.add_argument("--raw-db-path", type=Path)
    parser.add_argument(
        "--source-api",
        dest="source_apis",
        action="append",
        choices=("stock_basic", "suspend_d", "stock_st", "namechange"),
        default=[],
    )
    parser.add_argument("--signal-start-date")
    parser.add_argument("--signal-end-date")
    parser.add_argument("--instrument", dest="instrument_list", action="append", default=[])
    parser.add_argument("--instrument-limit", type=int)
    parser.add_argument("--use-checkpoint-queue", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    parser.add_argument("--tushare-token")
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_tushare_objective_source_sync(
        raw_db_path=args.raw_db_path,
        source_apis=args.source_apis or None,
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instrument_limit=args.instrument_limit,
        instrument_list=args.instrument_list,
        use_checkpoint_queue=args.use_checkpoint_queue,
        run_id=args.run_id,
        summary_path=args.summary_path,
        tushare_token=args.tushare_token,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
