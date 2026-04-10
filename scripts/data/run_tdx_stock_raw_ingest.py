"""执行 `TDX -> raw_market` 股票日线 ingest 的脚本入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data import run_tdx_stock_raw_ingest


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest bounded TDX stock daily files into raw_market.")
    parser.add_argument("--source-root", type=Path, default=Path("H:/tdx_offline_Data"))
    parser.add_argument("--adjust-method", default="backward")
    parser.add_argument("--run-mode", choices=("incremental", "full"), default="incremental")
    parser.add_argument("--force-hash", action="store_true")
    parser.add_argument("--continue-from-last-run", action="store_true")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_tdx_stock_raw_ingest(
        source_root=args.source_root,
        adjust_method=args.adjust_method,
        run_mode=args.run_mode,
        force_hash=args.force_hash,
        continue_from_last_run=args.continue_from_last_run,
        instruments=args.instruments,
        limit=args.limit,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
