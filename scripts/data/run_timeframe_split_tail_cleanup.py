"""执行 `77` 所需的旧 day 库 week/month 尾巴清理。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data.data_timeframe_split_cleanup import purge_day_timeframe_split_tail


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect or purge week/month price and audit tail from day raw/base ledgers.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually purge old week/month price tables and timeframe-scoped audit rows from day ledgers.",
    )
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    summary = purge_day_timeframe_split_tail(execute=args.execute)
    if args.summary_path is not None:
        args.summary_path.parent.mkdir(parents=True, exist_ok=True)
        args.summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
