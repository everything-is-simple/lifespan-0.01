"""检查当前待施工卡是否满足文档先行硬门禁。"""

from __future__ import annotations

import argparse
from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[2]
CARD_CATALOG_PATH = REPO_ROOT / "docs" / "03-execution" / "B-card-catalog-20260409.md"
CURRENT_CARD_PATTERN = re.compile(r"当前待施工卡：`([^`]+)`")
TRIGGER_PREFIXES = ("src/", "scripts/", ".codex/")
PLACEHOLDER_TOKENS = ("<", "切片 1", "切片 2", "切片 3", "问题：", "目标结果：", "为什么现在做：")


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数。"""

    parser = argparse.ArgumentParser(description="检查当前待施工卡是否满足文档先行硬门禁。")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="仓库根目录。")
    parser.add_argument("--report-path", help="可选，把检查结果写入 Markdown 报告。")
    parser.add_argument("paths", nargs="*", help="可选，只检查本次新增或改动文件。")
    return parser


def _normalize_path(repo_root: Path, raw_path: str) -> str | None:
    """把输入路径转换成仓库相对路径。"""

    candidate = Path(raw_path)
    resolved = candidate.resolve(strict=False) if candidate.is_absolute() else (repo_root / candidate).resolve(strict=False)
    try:
        return resolved.relative_to(repo_root).as_posix()
    except ValueError:
        return None


def _find_section(text: str, heading: str) -> str:
    """提取指定二级标题下的正文。"""

    pattern = re.compile(rf"^## {re.escape(heading)}\n(?P<body>.*?)(?=^## |\Z)", re.MULTILINE | re.DOTALL)
    match = pattern.search(text)
    return match.group("body").strip() if match else ""


def _collect_bullet_blocks(section_text: str) -> list[str]:
    """收集 bullet 及其续行文本。"""

    blocks: list[str] = []
    current: list[str] = []
    for raw_line in section_text.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("- "):
            if current:
                blocks.append(" ".join(part for part in current if part).strip())
            current = [stripped[2:].strip()]
            continue
        if current and stripped:
            current.append(stripped)
    if current:
        blocks.append(" ".join(part for part in current if part).strip())
    return blocks


def _collect_backtick_links(section_text: str) -> list[str]:
    """抽取 Markdown 中的反引号文档链接。"""

    return re.findall(r"`([^`]+\.md)`", section_text)


def _collect_task_items(section_text: str) -> list[str]:
    """提取编号任务项。"""

    return [match.group(1).strip() for match in re.finditer(r"^\d+\.\s+(.*\S)\s*$", section_text, re.MULTILINE)]


def _current_card_path(repo_root: Path) -> Path | None:
    """定位当前待施工卡。"""

    catalog_path = repo_root / CARD_CATALOG_PATH.relative_to(REPO_ROOT)
    if not catalog_path.exists():
        return None

    text = catalog_path.read_text(encoding="utf-8")
    match = CURRENT_CARD_PATTERN.search(text)
    if not match:
        return None
    return repo_root / "docs" / "03-execution" / match.group(1)


def _is_placeholder(text: str) -> bool:
    """判断一段内容是否仍是模板或占位。"""

    stripped = text.strip()
    if not stripped:
        return True
    if stripped.startswith("<") and stripped.endswith(">"):
        return True
    return any(token == stripped or token in stripped for token in PLACEHOLDER_TOKENS)


def _validate_current_card(card_path: Path, repo_root: Path) -> tuple[list[str], bool]:
    """验证当前待施工卡是否满足门禁。"""

    lines: list[str] = []
    if not card_path.exists():
        lines.append(f"  - 当前待施工卡不存在：`{card_path.relative_to(repo_root).as_posix()}`")
        return lines, False

    text = card_path.read_text(encoding="utf-8")
    ok = True

    requirement_section = _find_section(text, "需求")
    if not requirement_section:
        lines.append("  - 缺少 `## 需求` 章节。")
        ok = False
    else:
        bullet_blocks = _collect_bullet_blocks(requirement_section)
        for label in ("问题", "目标结果", "为什么现在做"):
            matched = next((item for item in bullet_blocks if item.startswith(f"{label}：")), None)
            if matched is None:
                lines.append(f"  - `需求` 章节缺少 `{label}` 条目。")
                ok = False
                continue
            value = matched.split("：", 1)[1].strip() if "：" in matched else ""
            if _is_placeholder(value):
                lines.append(f"  - `需求` 章节中的 `{label}` 仍是占位内容。")
                ok = False

    design_section = _find_section(text, "设计输入")
    if not design_section:
        lines.append("  - 缺少 `## 设计输入` 章节。")
        ok = False
    else:
        links = _collect_backtick_links(design_section)
        design_links = [link for link in links if link.startswith("docs/01-design/")]
        spec_links = [link for link in links if link.startswith("docs/02-spec/")]
        if not design_links:
            lines.append("  - `设计输入` 章节缺少 `docs/01-design/` 设计文档链接。")
            ok = False
        if not spec_links:
            lines.append("  - `设计输入` 章节缺少 `docs/02-spec/` 规格文档链接。")
            ok = False
        for rel_path in design_links + spec_links:
            if not (repo_root / rel_path).exists():
                lines.append(f"  - `设计输入` 链接不存在：`{rel_path}`")
                ok = False

    task_section = _find_section(text, "任务分解")
    if not task_section:
        lines.append("  - 缺少 `## 任务分解` 章节。")
        ok = False
    else:
        task_items = [item for item in _collect_task_items(task_section) if not _is_placeholder(item)]
        if not task_items:
            lines.append("  - `任务分解` 章节仍是占位内容，缺少正式任务项。")
            ok = False

    if ok:
        lines.append(f"  - 通过：当前待施工卡 `docs/03-execution/{card_path.name}` 已具备需求、设计、规格与任务分解。")
    return lines, ok


def run_check(repo_root: Path, paths: list[str] | None = None) -> tuple[list[str], bool]:
    """执行文档先行硬门禁检查。"""

    lines = ["[doc-first-gating]"]
    card_path = _current_card_path(repo_root)
    if card_path is None:
        lines.append("  - 无法从 `B-card-catalog-20260409.md` 解析当前待施工卡。")
        return lines, False

    if not paths:
        lines.append("  - 当前口径：全仓扫描时验证当前待施工卡是否已脱离模板态。")
        detail_lines, ok = _validate_current_card(card_path, repo_root)
        lines.extend(detail_lines)
        return lines, ok

    normalized_paths = {rel for raw in paths if (rel := _normalize_path(repo_root, raw))}
    triggered = any(rel.startswith(prefix) for rel in normalized_paths for prefix in TRIGGER_PREFIXES)
    if not triggered:
        lines.append("  - 本次改动未命中 `src/`、`scripts/`、`.codex/`，不触发严格文档先行门禁。")
        return lines, True

    lines.append("  - 本次改动命中正式实现前缀，执行严格文档先行门禁。")
    detail_lines, ok = _validate_current_card(card_path, repo_root)
    lines.extend(detail_lines)
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
        report_path.write_text("\n".join(["# doc-first gating governance report", "", "```text", output_text, "```", ""]), encoding="utf-8")

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
