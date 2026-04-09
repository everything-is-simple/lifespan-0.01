"""检查治理入口改动时是否同步刷新仓库入口文件。"""

from __future__ import annotations

import argparse
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ENTRY_FILES = ("AGENTS.md", "README.md", "pyproject.toml")
GOVERNANCE_PREFIXES = (".codex/", "scripts/", "docs/01-design/", "docs/02-spec/")
GOVERNANCE_FILES = ("src/mlq/core/paths.py",)


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数。"""

    parser = argparse.ArgumentParser(description="检查治理入口改动时是否同步刷新仓库入口文件。")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="仓库根目录。")
    parser.add_argument("--report-path", help="可选，把检查结果写入 Markdown 报告。")
    parser.add_argument("paths", nargs="*", help="可选，只检查本次新增或改动文件。")
    return parser


def _normalize_path(repo_root: Path, raw_path: str) -> str | None:
    """把输入路径转换成仓库相对路径。"""

    path = Path(raw_path)
    resolved = path.resolve(strict=False) if path.is_absolute() else (repo_root / path).resolve(strict=False)
    try:
        return resolved.relative_to(repo_root).as_posix()
    except ValueError:
        return None


def run_check(repo_root: Path, paths: list[str] | None = None) -> tuple[list[str], bool]:
    """执行入口文件新鲜度检查。"""

    lines = ["[entry-freshness]"]
    missing_entries = [entry for entry in ENTRY_FILES if not (repo_root / entry).exists()]
    if missing_entries:
        lines.append("  - 缺少仓库入口文件：")
        lines.extend(f"    - {entry}" for entry in missing_entries)
        return lines, False

    if not paths:
        lines.append("  - 当前口径：全仓扫描时只确认入口文件存在；严格联动检查在按改动范围运行时生效。")
        lines.append("  - 通过：`AGENTS.md`、`README.md`、`pyproject.toml` 均存在。")
        return lines, True

    normalized_paths = {rel for raw in paths if (rel := _normalize_path(repo_root, raw))}
    triggered = any(rel.startswith(prefix) for rel in normalized_paths for prefix in GOVERNANCE_PREFIXES) or any(
        rel in GOVERNANCE_FILES for rel in normalized_paths
    )
    if not triggered:
        lines.append("  - 本次改动未触发治理入口联动检查。")
        return lines, True

    missing_updates = [entry for entry in ENTRY_FILES if entry not in normalized_paths]
    if missing_updates:
        lines.append("  - 本次改动触发了治理入口联动，但以下入口文件未同步刷新：")
        lines.extend(f"    - {entry}" for entry in missing_updates)
        return lines, False

    lines.append("  - 通过：治理入口改动已同步刷新 `AGENTS.md`、`README.md`、`pyproject.toml`。")
    return lines, True


def main() -> int:
    """程序入口。"""

    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(args.repo_root).resolve()

    lines, ok = run_check(repo_root, paths=args.paths)
    output_text = "\n".join(lines)
    print(output_text)

    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("\n".join(["# entry freshness governance report", "", "```text", output_text, "```", ""]), encoding="utf-8")

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
