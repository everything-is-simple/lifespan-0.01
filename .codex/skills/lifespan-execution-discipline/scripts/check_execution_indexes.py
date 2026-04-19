"""检查 execution 索引回填是否与当前正式文件一致。"""

from __future__ import annotations

import argparse
import fnmatch
from pathlib import Path
import re
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[4]
EXECUTION_ROOT = REPO_ROOT / "docs" / "03-execution"
EVIDENCE_ROOT = EXECUTION_ROOT / "evidence"
RECORD_ROOT = EXECUTION_ROOT / "records"
READING_ORDER_PATH = EXECUTION_ROOT / "A-execution-reading-order-20260409.md"
CARD_CATALOG_PATH = EXECUTION_ROOT / "B-card-catalog-20260409.md"
COMPLETION_LEDGER_PATH = EXECUTION_ROOT / "C-system-completion-ledger-20260409.md"

CATALOG_CONFIG = {
    "conclusion": {
        "catalog": EXECUTION_ROOT / "00-conclusion-catalog-20260409.md",
        "files_root": EXECUTION_ROOT,
        "glob": "*-conclusion-*.md",
        "expected_token": "-conclusion-",
        "exclude_keywords": ("catalog", "template"),
    },
    "evidence": {
        "catalog": EVIDENCE_ROOT / "00-evidence-catalog-20260409.md",
        "files_root": EVIDENCE_ROOT,
        "glob": "*-evidence-*.md",
        "expected_token": "-evidence-",
        "exclude_keywords": ("catalog", "template"),
    },
    "card": {
        "catalog": CARD_CATALOG_PATH,
        "files_root": EXECUTION_ROOT,
        "glob": "*-card-*.md",
        "expected_token": "-card-",
        "exclude_keywords": ("catalog", "template", "discipline"),
    },
}

REFERENCE_PATTERN = re.compile(r"`([^`]+\.md)`")
CARD_FILENAME_PATTERN = re.compile(r"^\d+-(.+)-card-\d{8}\.md$")
RECORD_FILENAME_PATTERN = re.compile(r"^\d+-(.+)-record-\d{8}\.md$")
CONCLUSION_FILENAME_PATTERN = re.compile(r"^\d+-(.+)-conclusion-\d{8}\.md$")
EVIDENCE_FILENAME_PATTERN = re.compile(r"^\d+-(.+)-evidence-\d{8}\.md$")
NEXT_HAMMER_PATTERN = re.compile(r"当前下一锤：`([^`]+)`")
CURRENT_CARD_PATTERN = re.compile(r"当前待施工卡：`([^`]+)`")
COUNT_PATTERN = re.compile(r"(正式主线剩余卡|可选 Sidecar 剩余卡|后置修复剩余卡)：`(\d+)`")


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数。"""

    parser = argparse.ArgumentParser(description="检查 conclusion / evidence / card 目录与索引回填一致性。")
    parser.add_argument("--root", default=str(REPO_ROOT), help="仓库根目录，默认当前仓库。")
    parser.add_argument("--report-path", help="把检查结果写成 Markdown 报告。")
    parser.add_argument("--include-untracked", action="store_true", help="显式把未跟踪草稿也纳入检查。")
    return parser


def _run_git_ls_files(repo_root: Path, *args: str) -> set[str]:
    """执行 git ls-files 并返回相对路径集合。"""

    result = subprocess.run(["git", "ls-files", *args], cwd=repo_root, check=True, capture_output=True, text=True)
    return {line.strip().replace("\\", "/") for line in result.stdout.splitlines() if line.strip()}


def _git_relative_paths(repo_root: Path, include_untracked: bool) -> set[str]:
    """收集要纳入治理检查的相对路径。"""

    tracked_paths = _run_git_ls_files(repo_root)
    if not include_untracked:
        return tracked_paths
    return tracked_paths | _run_git_ls_files(repo_root, "--others", "--exclude-standard")


def extract_references(catalog_path: Path, expected_token: str) -> list[str]:
    """从目录文档中抽取对应文件引用。"""

    text = catalog_path.read_text(encoding="utf-8")
    return [name for name in REFERENCE_PATTERN.findall(text) if expected_token in name]


def collect_actual_files(
    repo_root: Path,
    files_root: Path,
    glob_pattern: str,
    exclude_keywords: tuple[str, ...],
    *,
    include_untracked: bool,
) -> set[str]:
    """从 git 视角收集当前目录下的正式文件。"""

    tracked_paths = _git_relative_paths(repo_root, include_untracked)
    root_prefix = f"{files_root.relative_to(repo_root).as_posix()}/"
    actual_files: set[str] = set()
    for rel_path in tracked_paths:
        if not rel_path.startswith(root_prefix):
            continue
        name = rel_path[len(root_prefix):]
        if "/" in name:
            continue
        if not (repo_root / rel_path).exists():
            continue
        lower_name = name.lower()
        if any(keyword in lower_name for keyword in exclude_keywords):
            continue
        if fnmatch.fnmatch(name, glob_pattern):
            actual_files.add(name)
    return actual_files


def _collect_names_under_root(repo_root: Path, files_root: Path, glob_pattern: str, *, include_untracked: bool) -> set[str]:
    """收集指定目录下的文件名。"""

    tracked_paths = _git_relative_paths(repo_root, include_untracked)
    root_prefix = f"{files_root.relative_to(repo_root).as_posix()}/"
    names: set[str] = set()
    for rel_path in tracked_paths:
        if not rel_path.startswith(root_prefix):
            continue
        name = rel_path[len(root_prefix):]
        if "/" in name:
            continue
        if not (repo_root / rel_path).exists():
            continue
        if fnmatch.fnmatch(name, glob_pattern):
            names.add(name)
    return names


def check_catalog(name: str, repo_root: Path, *, include_untracked: bool) -> tuple[list[str], bool]:
    """检查某个目录索引是否与正式文件一致。"""

    config = CATALOG_CONFIG[name]
    catalog_path = repo_root / config["catalog"].relative_to(REPO_ROOT)
    files_root = repo_root / config["files_root"].relative_to(REPO_ROOT)

    references = extract_references(catalog_path, config["expected_token"])
    reference_set = set(references)
    actual_files = collect_actual_files(
        repo_root,
        files_root,
        config["glob"],
        config["exclude_keywords"],
        include_untracked=include_untracked,
    )

    missing_from_catalog = sorted(actual_files - reference_set)
    missing_from_disk = sorted(reference_set - actual_files)

    lines = [f"[{name}]"]
    ok = True
    if not missing_from_catalog and not missing_from_disk:
        lines.append("  - 通过：索引与正式文件一致。")
        return lines, ok

    ok = False
    if missing_from_catalog:
        lines.append("  - 缺少回填：")
        lines.extend(f"    - {entry}" for entry in missing_from_catalog)
    if missing_from_disk:
        lines.append("  - 索引指向不存在的正式文件：")
        lines.extend(f"    - {entry}" for entry in missing_from_disk)
    return lines, ok


def extract_slug(name: str, pattern: re.Pattern[str]) -> str | None:
    """抽取四件套文件的 slug。"""

    match = pattern.match(name)
    return match.group(1) if match else None


def check_records(repo_root: Path, *, include_untracked: bool) -> tuple[list[str], bool]:
    """检查 record 是否与 card/evidence/conclusion 链条一致。"""

    execution_root = repo_root / EXECUTION_ROOT.relative_to(REPO_ROOT)
    record_root = repo_root / RECORD_ROOT.relative_to(REPO_ROOT)
    evidence_root = repo_root / EVIDENCE_ROOT.relative_to(REPO_ROOT)

    card_files = {
        name
        for name in _collect_names_under_root(repo_root, execution_root, "*-card-*.md", include_untracked=include_untracked)
        if "catalog" not in name.lower() and "template" not in name.lower()
    }
    record_files = _collect_names_under_root(repo_root, record_root, "*-record-*.md", include_untracked=include_untracked)
    conclusion_files = {
        name
        for name in _collect_names_under_root(repo_root, execution_root, "*-conclusion-*.md", include_untracked=include_untracked)
        if "catalog" not in name.lower() and "template" not in name.lower()
    }
    evidence_files = {
        name
        for name in _collect_names_under_root(repo_root, evidence_root, "*-evidence-*.md", include_untracked=include_untracked)
        if "catalog" not in name.lower() and "template" not in name.lower()
    }

    card_slugs = {extract_slug(name, CARD_FILENAME_PATTERN) for name in card_files}
    card_slugs.discard(None)
    record_slug_to_name = {extract_slug(name, RECORD_FILENAME_PATTERN): name for name in record_files}
    conclusion_slugs = {extract_slug(name, CONCLUSION_FILENAME_PATTERN) for name in conclusion_files}
    evidence_slugs = {extract_slug(name, EVIDENCE_FILENAME_PATTERN) for name in evidence_files}
    conclusion_slugs.discard(None)
    evidence_slugs.discard(None)

    expected_record_slugs = card_slugs & conclusion_slugs & evidence_slugs
    orphan_records = sorted(name for slug, name in record_slug_to_name.items() if slug not in card_slugs)
    missing_records = sorted(slug for slug in expected_record_slugs if slug not in record_slug_to_name)

    lines = ["[records]"]
    ok = True
    if not orphan_records and not missing_records:
        lines.append("  - 通过：records 与 card/evidence/conclusion 链一致。")
        return lines, ok

    ok = False
    if orphan_records:
        lines.append("  - record 无对应 card：")
        lines.extend(f"    - {name}" for name in orphan_records)
    if missing_records:
        lines.append("  - 已形成 card+evidence+conclusion 但缺 record：")
        lines.extend(f"    - {slug}" for slug in missing_records)
    return lines, ok


def check_execution_layout(repo_root: Path, *, include_untracked: bool) -> tuple[list[str], bool]:
    """检查 execution 根目录是否错误放置 evidence / record。"""

    execution_root = repo_root / EXECUTION_ROOT.relative_to(REPO_ROOT)
    misplaced_evidence = sorted(
        name
        for name in _collect_names_under_root(repo_root, execution_root, "*-evidence-*.md", include_untracked=include_untracked)
        if "template" not in name.lower() and "catalog" not in name.lower()
    )
    misplaced_records = sorted(
        name
        for name in _collect_names_under_root(repo_root, execution_root, "*-record-*.md", include_untracked=include_untracked)
        if "template" not in name.lower() and "catalog" not in name.lower()
    )

    lines = ["[execution-layout]"]
    ok = True
    if not misplaced_evidence and not misplaced_records:
        lines.append("  - 通过：execution 根目录未错误放置 evidence / record。")
        return lines, ok

    ok = False
    if misplaced_evidence:
        lines.append("  - 根目录错误放置 evidence：")
        lines.extend(f"    - {name}" for name in misplaced_evidence)
    if misplaced_records:
        lines.append("  - 根目录错误放置 record：")
        lines.extend(f"    - {name}" for name in misplaced_records)
    return lines, ok


def check_reading_order(repo_root: Path) -> tuple[list[str], bool]:
    """检查阅读顺序文档是否与当前施工卡一致。"""

    reading_text = (repo_root / READING_ORDER_PATH.relative_to(REPO_ROOT)).read_text(encoding="utf-8")
    card_catalog_text = (repo_root / CARD_CATALOG_PATH.relative_to(REPO_ROOT)).read_text(encoding="utf-8")

    current_card_match = CURRENT_CARD_PATTERN.search(card_catalog_text)
    current_card = current_card_match.group(1) if current_card_match else None

    lines = ["[reading-order]"]
    ok = True
    if current_card and current_card not in reading_text:
        ok = False
        lines.append("  - reading-order 未提及当前待施工卡：")
        lines.append(f"    - {current_card}")
    if ok:
        lines.append("  - 通过：reading-order 与当前施工卡一致。")
    return lines, ok


def parse_counts(text: str) -> dict[str, int]:
    """解析计数器。"""

    return {match.group(1): int(match.group(2)) for match in COUNT_PATTERN.finditer(text)}


def check_completion_ledger(repo_root: Path) -> tuple[list[str], bool]:
    """检查完工账本与卡目录是否一致。"""

    card_catalog_text = (repo_root / CARD_CATALOG_PATH.relative_to(REPO_ROOT)).read_text(encoding="utf-8")
    ledger_text = (repo_root / COMPLETION_LEDGER_PATH.relative_to(REPO_ROOT)).read_text(encoding="utf-8")

    card_current = CURRENT_CARD_PATTERN.search(card_catalog_text)
    ledger_next = NEXT_HAMMER_PATTERN.search(ledger_text)
    card_counts = parse_counts(card_catalog_text)
    ledger_counts = parse_counts(ledger_text)

    lines = ["[completion-ledger]"]
    ok = True
    if not ledger_next or not card_current or ledger_next.group(1) != card_current.group(1):
        ok = False
        lines.append("  - 账本当前下一锤与卡目录当前待施工卡不一致：")
        lines.append(f"    - card-catalog: {card_current.group(1) if card_current else '未找到'}")
        lines.append(f"    - completion-ledger: {ledger_next.group(1) if ledger_next else '未找到'}")

    for key, value in card_counts.items():
        if ledger_counts.get(key) != value:
            ok = False
            lines.append("  - 剩余工作计数不一致：")
            lines.append(f"    - {key}: card-catalog={value}, completion-ledger={ledger_counts.get(key)}")

    if ok:
        lines.append("  - 通过：completion-ledger 与卡目录一致。")
    return lines, ok


def main() -> int:
    """程序入口。"""

    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(args.root).resolve()

    overall_ok = True
    all_lines: list[str] = []
    for name in ("conclusion", "evidence", "card"):
        lines, ok = check_catalog(name, repo_root, include_untracked=args.include_untracked)
        all_lines.extend(lines)
        overall_ok = overall_ok and ok

    lines, ok = check_records(repo_root, include_untracked=args.include_untracked)
    all_lines.extend(lines)
    overall_ok = overall_ok and ok

    lines, ok = check_execution_layout(repo_root, include_untracked=args.include_untracked)
    all_lines.extend(lines)
    overall_ok = overall_ok and ok

    for check_fn in (check_reading_order, check_completion_ledger):
        lines, ok = check_fn(repo_root)
        all_lines.extend(lines)
        overall_ok = overall_ok and ok

    output_text = "\n".join(all_lines)
    print(output_text)

    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("\n".join(["# execution index report", "", "```text", output_text, "```", ""]), encoding="utf-8")

    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
