"""执行 `TDX -> raw_market` 的 stock/index/block 日线 ingest 入口。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data import run_tdx_asset_raw_ingest, run_tdx_asset_raw_ingest_batched


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest bounded TDX daily files into raw_market by asset type.")
    parser.add_argument("--asset-type", choices=("stock", "index", "block"), default="stock")
    parser.add_argument("--timeframe", choices=("day", "week", "month"), default="day")
    parser.add_argument("--source-root", type=Path, default=Path("H:/tdx_offline_Data"))
    parser.add_argument("--adjust-method", default="backward")
    parser.add_argument("--run-mode", choices=("incremental", "full"), default="incremental")
    parser.add_argument("--force-hash", action="store_true")
    parser.add_argument("--continue-from-last-run", action="store_true")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Split raw ingest into instrument/file batches. 0 disables batching.",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    if args.batch_size > 0:
        if args.continue_from_last_run:
            raise ValueError("--continue-from-last-run is only supported in non-batched raw ingest mode.")
        summary_payload = run_tdx_asset_raw_ingest_batched(
            asset_type=args.asset_type,
            timeframe=args.timeframe,
            source_root=args.source_root,
            adjust_method=args.adjust_method,
            run_mode=args.run_mode,
            force_hash=args.force_hash,
            instruments=args.instruments,
            batch_size=args.batch_size,
            run_id=args.run_id,
            summary_path=args.summary_path,
        )
    else:
        summary = run_tdx_asset_raw_ingest(
            asset_type=args.asset_type,
            timeframe=args.timeframe,
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
        summary_payload = summary.as_dict()
    print(json.dumps(summary_payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
