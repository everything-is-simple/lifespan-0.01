"""执行主线本地正式库标准化 bootstrap。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mlq.data import run_mainline_local_ledger_standardization_bootstrap


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bootstrap official mainline ledger paths and migrate selected legacy DuckDB files.")
    parser.add_argument("--ledger", dest="ledgers", action="append", default=[])
    parser.add_argument(
        "--source-ledger",
        dest="source_ledgers",
        action="append",
        default=[],
        help="Repeatable mapping in the form ledger_name=absolute_or_relative_path.",
    )
    parser.add_argument("--force-copy", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def _parse_source_ledgers(items: list[str]) -> dict[str, Path]:
    mappings: dict[str, Path] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid --source-ledger mapping: {item}")
        ledger_name, raw_path = item.split("=", 1)
        mappings[ledger_name.strip()] = Path(raw_path.strip())
    return mappings


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    summary = run_mainline_local_ledger_standardization_bootstrap(
        ledgers=args.ledgers or None,
        source_ledger_paths=_parse_source_ledgers(args.source_ledgers),
        force_copy=args.force_copy,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
