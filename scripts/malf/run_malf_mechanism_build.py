"""执行 `malf` 机制层 sidecar 账本物化的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.malf import run_malf_mechanism_build


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build MALF mechanism-sidecar ledgers from bridge v1 snapshots.")
    parser.add_argument("--signal-start-date", dest="signal_start_date")
    parser.add_argument("--signal-end-date", dest="signal_end_date")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--timeframe", default="D")
    parser.add_argument("--stats-sample-version", default="bridge-v1")
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_malf_mechanism_build(
        signal_start_date=args.signal_start_date,
        signal_end_date=args.signal_end_date,
        instruments=args.instruments,
        limit=args.limit,
        batch_size=args.batch_size,
        timeframe=args.timeframe,
        stats_sample_version=args.stats_sample_version,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
