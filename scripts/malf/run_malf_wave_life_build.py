"""执行 `malf` 波段寿命概率 sidecar 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.malf import run_malf_wave_life_build


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build MALF wave-life probability sidecar ledgers.")
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--timeframe", dest="timeframes", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--sample-version", default="wave-life-v1")
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_malf_wave_life_build(
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instruments=args.instruments,
        timeframes=args.timeframes,
        limit=args.limit,
        sample_version=args.sample_version,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
