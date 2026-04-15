"""执行 `Tushare objective event -> raw_tdxquant_instrument_profile` 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data import run_tushare_objective_profile_materialization


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize bounded Tushare objective events into raw_tdxquant_instrument_profile."
    )
    parser.add_argument("--raw-db-path", type=Path)
    parser.add_argument("--signal-start-date")
    parser.add_argument("--signal-end-date")
    parser.add_argument("--instrument", dest="instrument_list", action="append", default=[])
    parser.add_argument("--instrument-limit", type=int)
    parser.add_argument("--use-checkpoint-queue", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_tushare_objective_profile_materialization(
        raw_db_path=args.raw_db_path,
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instrument_limit=args.instrument_limit,
        instrument_list=args.instrument_list,
        use_checkpoint_queue=args.use_checkpoint_queue,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
