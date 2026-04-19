"""执行 canonical MALF v2 bounded runner 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.malf import run_malf_canonical_build


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build canonical MALF v2 ledgers from official market_base prices.")
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--timeframe", dest="timeframes", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--adjust-method", default="backward")
    parser.add_argument("--pivot-confirmation-window", type=int, default=2)
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_malf_canonical_build(
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instruments=args.instruments,
        timeframes=args.timeframes,
        limit=args.limit,
        adjust_method=args.adjust_method,
        pivot_confirmation_window=args.pivot_confirmation_window,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
