"""检查仓库污染与临时产物混入的最低治理规则。"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FORBIDDEN_SUFFIXES = {
    ".duckdb",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".parquet",
    ".feather",
    ".pkl",
    ".pickle",
    ".joblib",
    ".tmp",
    ".temp",
    ".bak",
    ".orig",
    ".rej",
    ".log",
}
FORBIDDEN_FILENAMES = {"thumbs.db", ".ds_store"}
FORBIDDEN_DIR_TOKENS = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
FORBIDDEN_ROOT_DIRS = {".pytest_cache", ".tmp-pytest"}


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数。"""

    parser = argparse.ArgumentParser(description="检查仓库污染与临时产物混入。")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="仓库根目录。")
    parser.add_argument("--report-path", help="可选，把检查结果写入 Markdown 报告。")
    parser.add_argument("paths", nargs="*", help="可选，只检查本次新增或改动文件。")
    return parser


def _git_workspace_files(repo_root: Path) -> list[Path]:
    """收集 git 视角下的工作区文件。"""

    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return [repo_root / line for line in result.stdout.splitlines() if line.strip()]


def _candidate_files(repo_root: Path, paths: list[str] | None) -> list[Path]:
    """收集需要参与检查的文件。"""

    if not paths:
        return _git_workspace_files(repo_root)

    candidates: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        resolved = path.resolve() if path.is_absolute() else (repo_root / path).resolve()
        if resolved.exists() and resolved.is_file():
            candidates.append(resolved)
    return candidates


def run_check(repo_root: Path, paths: list[str] | None = None) -> tuple[list[str], bool]:
    """执行仓库卫生检查。"""

    violations: list[str] = []
    for dirname in sorted(FORBIDDEN_ROOT_DIRS):
        if (repo_root / dirname).exists():
            violations.append(dirname + "/")

    for path in _candidate_files(repo_root, paths):
        rel = path.relative_to(repo_root).as_posix()
        if rel.startswith(".git/") or rel.startswith(".venv/"):
            continue
        if any(token in path.parts for token in FORBIDDEN_DIR_TOKENS):
            violations.append(rel)
            continue
        if path.name.lower() in FORBIDDEN_FILENAMES or path.suffix.lower() in FORBIDDEN_SUFFIXES:
            violations.append(rel)

    lines = ["[repo-hygiene]"]
    ok = not violations
    if violations:
        lines.append("  - 仓库内发现不允许存在的临时 / 中间 / 数据产物：")
        lines.extend(f"    - {entry}" for entry in sorted(violations))
    else:
        scope_label = "本次改动范围" if paths else "当前 git 视角工作区"
        lines.append(f"  - 通过：{scope_label}未发现临时文件、中间文件、数据库产物或未忽略缓存。")
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
        report_path.write_text("\n".join(["# repo hygiene governance report", "", "```text", output_text, "```", ""]), encoding="utf-8")

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
