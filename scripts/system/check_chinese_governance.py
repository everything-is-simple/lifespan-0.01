"""检查正式文档中文化与代码中文注释治理规则。"""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

from development_governance_legacy_backlog import LEGACY_CHINESE_PYTHON_BACKLOG


REPO_ROOT = Path(__file__).resolve().parents[2]
CJK_PATTERN = re.compile(r"[\u4e00-\u9fff]")


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数。"""

    parser = argparse.ArgumentParser(description="检查正式文档中文化与代码中文注释治理规则。")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="仓库根目录。")
    parser.add_argument("--report-path", help="可选，把检查结果写入 Markdown 报告。")
    parser.add_argument("paths", nargs="*", help="可选，只检查本次新增或改动文件。")
    return parser


def _git_tracked_files(repo_root: Path) -> list[Path]:
    """收集 git 已跟踪文件。"""

    result = subprocess.run(
        ["git", "ls-files"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return [repo_root / line for line in result.stdout.splitlines() if line.strip()]


def _candidate_files(repo_root: Path, paths: list[str] | None) -> list[Path]:
    """收集需要参与检查的文件。"""

    if not paths:
        return _git_tracked_files(repo_root)

    candidates: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        resolved = path.resolve() if path.is_absolute() else (repo_root / path).resolve()
        if resolved.exists() and resolved.is_file():
            candidates.append(resolved)
    return candidates


def _has_chinese(text: str) -> bool:
    """判断文本中是否含有中文。"""

    return bool(CJK_PATTERN.search(text))


def _has_chinese_comment_hint(text: str) -> bool:
    """判断 Python 文件是否带中文注释或中文 docstring。"""

    in_docstring = False
    delimiter = ""
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#") and _has_chinese(line):
            return True
        for token in ('"""', "'''"):
            if token not in line:
                continue
            count = line.count(token)
            if count >= 2:
                return _has_chinese(line)
            if not in_docstring:
                in_docstring = True
                delimiter = token
                if _has_chinese(line):
                    return True
                break
            if token == delimiter:
                if _has_chinese(line):
                    return True
                in_docstring = False
                delimiter = ""
                break
        if in_docstring and _has_chinese(line):
            return True
    return False


def run_check(repo_root: Path, paths: list[str] | None = None) -> tuple[list[str], bool]:
    """执行中文治理检查。"""

    markdown_failures: list[str] = []
    python_failures: list[str] = []
    legacy_backlog: list[str] = []
    strict_mode = bool(paths)

    for path in _candidate_files(repo_root, paths):
        rel = path.relative_to(repo_root).as_posix()
        suffix = path.suffix.lower()
        if suffix not in {".md", ".py"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue

        if suffix == ".md" and (
            rel.startswith("docs/")
            or rel.startswith(".codex/")
            or rel == "README.md"
            or rel == "AGENTS.md"
        ):
            if not _has_chinese(text):
                markdown_failures.append(rel)

        if suffix == ".py" and (
            rel.startswith("src/")
            or rel.startswith("scripts/")
            or rel.startswith("tests/")
            or rel.startswith(".codex/")
        ):
            if not _has_chinese_comment_hint(text):
                if strict_mode or rel not in LEGACY_CHINESE_PYTHON_BACKLOG:
                    python_failures.append(rel)
                else:
                    legacy_backlog.append(rel)

    lines = ["[chinese-governance]"]
    ok = (not markdown_failures and not python_failures) if strict_mode else True
    if strict_mode:
        lines.append("  - 当前口径：对本次改动文件做硬闸门。")
    else:
        lines.append("  - 当前口径：全仓扫描只做历史债务盘点。")

    if markdown_failures:
        title = "  - 缺少中文内容的正式 Markdown："
        if not strict_mode:
            title = "  - 历史债务：缺少中文内容的正式 Markdown："
        lines.append(title)
        lines.extend(f"    - {entry}" for entry in sorted(markdown_failures))

    if python_failures:
        title = "  - 缺少中文注释或中文 docstring 的 Python 文件："
        if not strict_mode:
            title = "  - 历史债务：缺少中文注释或中文 docstring 的 Python 文件："
        lines.append(title)
        lines.extend(f"    - {entry}" for entry in sorted(python_failures))
    if legacy_backlog:
        lines.append("  - 已登记的历史 Python 中文化旧债：")
        lines.extend(f"    - {entry}" for entry in sorted(legacy_backlog))

    if ok:
        if strict_mode:
            lines.append("  - 通过：本次改动范围满足中文治理硬闸门。")
        else:
            lines.append("  - 通过：当前全仓没有新增未登记中文化缺口。")

    return lines, ok


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
        report_path.write_text("\n".join(["# chinese governance report", "", "```text", output_text, "```", ""]), encoding="utf-8")

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
