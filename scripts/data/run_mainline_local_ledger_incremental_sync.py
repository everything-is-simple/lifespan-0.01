"""执行主线本地正式库增量同步与断点续跑。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data import run_mainline_local_ledger_incremental_sync


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run mainline local ledger incremental sync, checkpoint and freshness audit.")
    parser.add_argument("--ledger", dest="ledgers", action="append", default=[])
    parser.add_argument("--source-ledger", dest="source_ledgers", action="append", default=[])
    parser.add_argument("--source-latest-bar-date", dest="source_latest_bar_dates", action="append", default=[])
    parser.add_argument("--replay-start-date", dest="replay_start_dates", action="append", default=[])
    parser.add_argument("--replay-confirm-until-date", dest="replay_confirm_dates", action="append", default=[])
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def _parse_path_mappings(items: list[str]) -> dict[str, Path]:
    mappings: dict[str, Path] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid mapping: {item}")
        ledger_name, raw_path = item.split("=", 1)
        mappings[ledger_name.strip()] = Path(raw_path.strip())
    return mappings


def _parse_string_mappings(items: list[str]) -> dict[str, str]:
    mappings: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid mapping: {item}")
        ledger_name, raw_value = item.split("=", 1)
        mappings[ledger_name.strip()] = raw_value.strip()
    return mappings


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_mainline_local_ledger_incremental_sync(
        ledgers=args.ledgers or None,
        source_ledger_paths=_parse_path_mappings(args.source_ledgers),
        source_latest_bar_dates=_parse_string_mappings(args.source_latest_bar_dates),
        replay_start_dates=_parse_string_mappings(args.replay_start_dates),
        replay_confirm_until_dates=_parse_string_mappings(args.replay_confirm_dates),
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
