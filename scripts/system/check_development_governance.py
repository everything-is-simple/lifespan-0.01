"""串联文件长度、中文化、仓库卫生、入口新鲜度与文档先行门禁检查。"""

from __future__ import annotations

import argparse
from pathlib import Path

from check_chinese_governance import run_check as run_chinese_check
from check_doc_first_gating_governance import run_check as run_doc_first_gating_check
from check_entry_freshness_governance import run_check as run_entry_freshness_check
from check_file_length_governance import run_check as run_file_length_check
from check_repo_hygiene_governance import run_check as run_repo_hygiene_check


REPO_ROOT = Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数。"""

    parser = argparse.ArgumentParser(description="串联文件长度、中文化、仓库卫生、入口新鲜度与文档先行门禁检查。")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="仓库根目录。")
    parser.add_argument("--report-path", help="可选，把检查结果写入 Markdown 报告。")
    parser.add_argument("paths", nargs="*", help="可选，只检查本次新增或改动文件。")
    return parser


def main() -> int:
    """程序入口。"""

    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(args.repo_root).resolve()

    checks = [
        run_file_length_check(repo_root, paths=args.paths),
        run_chinese_check(repo_root, paths=args.paths),
        run_repo_hygiene_check(repo_root, paths=args.paths),
        run_entry_freshness_check(repo_root, paths=args.paths),
        run_doc_first_gating_check(repo_root, paths=args.paths),
    ]

    lines: list[str] = []
    overall_ok = True
    for section_lines, ok in checks:
        lines.extend(section_lines)
        overall_ok = overall_ok and ok

    output_text = "\n".join(lines)
    print(output_text)

    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("\n".join(["# development governance report", "", "```text", output_text, "```", ""]), encoding="utf-8")

    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
