"""执行 `TDX -> raw_market` 的 stock/index/block 日线 ingest 入口。"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from mlq.data import (
    resolve_tdx_asset_pending_registry_scope,
    run_tdx_asset_raw_ingest,
    run_tdx_asset_raw_ingest_batched,
)


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
    parser.add_argument(
        "--pending-only-from-registry",
        action="store_true",
        help="Resolve source folder codes minus official raw registry coverage before launching the run.",
    )
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


def parse_args() -> argparse.Namespace:
    return build_argument_parser().parse_args()


def _write_summary_file(summary_payload: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_empty_summary(
    *,
    args: argparse.Namespace,
    pending_scope: dict[str, object],
) -> dict[str, object]:
    base_payload: dict[str, object] = {
        "run_id": args.run_id,
        "asset_type": str(pending_scope["asset_type"]),
        "timeframe": str(pending_scope["timeframe"]),
        "adjust_method": str(pending_scope["adjust_method"]),
        "run_mode": args.run_mode,
        "raw_market_path": str(pending_scope["raw_market_path"]),
        "source_root": str(pending_scope["source_root"]),
        "pending_only_from_registry": True,
        "candidate_file_count": 0,
        "processed_file_count": 0,
        "ingested_file_count": 0,
        "skipped_unchanged_file_count": 0,
        "failed_file_count": 0,
        "bar_inserted_count": 0,
        "bar_reused_count": 0,
        "bar_rematerialized_count": 0,
    }
    if args.batch_size > 0:
        return {
            **base_payload,
            "batch_size": int(args.batch_size),
            "batch_count": 0,
            "child_runs": [],
        }
    return base_payload


def main() -> None:
    args = parse_args()
    pending_scope: dict[str, object] | None = None
    if args.pending_only_from_registry:
        pending_scope = resolve_tdx_asset_pending_registry_scope(
            asset_type=args.asset_type,
            timeframe=args.timeframe,
            source_root=args.source_root,
            adjust_method=args.adjust_method,
            instruments=args.instruments,
        )
        args.instruments = list(pending_scope["pending_instruments"])
        startup_payload = {
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "asset_type": pending_scope["asset_type"],
            "timeframe": pending_scope["timeframe"],
            "source_timeframe": pending_scope["source_timeframe"],
            "total_codes": pending_scope["candidate_instrument_count"],
            "existing_codes": pending_scope["existing_instrument_count"],
            "pending_codes": pending_scope["pending_instrument_count"],
            "batch_size": int(args.batch_size),
            "pending_only_from_registry": True,
        }
        print(json.dumps(startup_payload, ensure_ascii=False), flush=True)
        if not args.instruments:
            summary_payload = _build_empty_summary(args=args, pending_scope=pending_scope)
            _write_summary_file(summary_payload, args.summary_path)
            print(json.dumps(summary_payload, ensure_ascii=False, indent=2))
            return
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
    if pending_scope is not None:
        summary_payload["pending_only_from_registry"] = True
        _write_summary_file(summary_payload, args.summary_path)
    print(json.dumps(summary_payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
