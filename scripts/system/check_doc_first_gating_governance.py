"""检查当前待施工卡是否满足文档先行与历史账本增量治理硬门禁。"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CARD_CATALOG_RELATIVE_PATH = Path("docs/03-execution/B-card-catalog-20260409.md")
TRIGGER_PREFIXES = ("src/", "scripts/", ".codex/")

CURRENT_CARD_PATTERNS = (
    re.compile(r"当前待施工卡[：:]\s*`([^`]+)`"),
    re.compile(r"当前下一锤[：:]\s*`([^`]+)`"),
)
PLACEHOLDER_TOKENS = (
    "<待填编号>",
    "<yyyy-mm-dd>",
    "问题：",
    "目标结果：",
    "为什么现在做：",
    "切片 1：",
    "切片 2：",
    "切片 3：",
    "实体锚点：",
    "业务自然键：",
    "批量建仓：",
    "增量更新：",
    "断点续跑：",
    "审计账本：",
    "待回填",
)
REQUIREMENT_LABELS = ("问题", "目标结果", "为什么现在做")
LEDGER_LABELS = ("实体锚点", "业务自然键", "批量建仓", "增量更新", "断点续跑", "审计账本")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="检查当前待施工卡是否满足文档先行与历史账本增量治理硬门禁。")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="仓库根目录。")
    parser.add_argument("--report-path", help="可选，把检查结果写入 Markdown 报告。")
    parser.add_argument("paths", nargs="*", help="可选，只检查本次新增或改动文件。")
    return parser


def _normalize_path(repo_root: Path, raw_path: str) -> str | None:
    candidate = Path(raw_path)
    resolved = candidate.resolve(strict=False) if candidate.is_absolute() else (repo_root / candidate).resolve(strict=False)
    try:
        return resolved.relative_to(repo_root).as_posix()
    except ValueError:
        return None


def _find_section(text: str, heading: str) -> str:
    pattern = re.compile(rf"^## {re.escape(heading)}\n(?P<body>.*?)(?=^## |\Z)", re.MULTILINE | re.DOTALL)
    match = pattern.search(text)
    return match.group("body").strip() if match else ""


def _collect_bullet_blocks(section_text: str) -> list[str]:
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
    return re.findall(r"`([^`]+\.md)`", section_text)


def _collect_task_items(section_text: str) -> list[str]:
    return [match.group(1).strip() for match in re.finditer(r"^\d+\.\s+(.*\S)\s*$", section_text, re.MULTILINE)]


def _current_card_path(repo_root: Path) -> Path | None:
    catalog_path = repo_root / CARD_CATALOG_RELATIVE_PATH
    if not catalog_path.exists():
        return None
    text = catalog_path.read_text(encoding="utf-8")
    for pattern in CURRENT_CARD_PATTERNS:
        match = pattern.search(text)
        if match:
            return repo_root / "docs" / "03-execution" / match.group(1)
    return None


def _is_placeholder(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return True
    if stripped.startswith("<") and stripped.endswith(">"):
        return True
    if stripped == "待回填" or stripped.startswith("待回填："):
        return True
    return any(token == stripped for token in PLACEHOLDER_TOKENS)


def _extract_labeled_value(blocks: list[str], label: str) -> str | None:
    prefixes = (f"{label}：", f"{label}:")
    for item in blocks:
        for prefix in prefixes:
            if item.startswith(prefix):
                return item[len(prefix) :].strip()
    return None


def _validate_labeled_section(
    *,
    lines: list[str],
    section_name: str,
    section_text: str,
    labels: tuple[str, ...],
) -> bool:
    ok = True
    if not section_text:
        lines.append(f"  - 缺少 `## {section_name}` 章节。")
        return False
    blocks = _collect_bullet_blocks(section_text)
    for label in labels:
        value = _extract_labeled_value(blocks, label)
        if value is None:
            lines.append(f"  - `## {section_name}` 章节缺少 `{label}` 条目。")
            ok = False
            continue
        if _is_placeholder(value):
            lines.append(f"  - `## {section_name}` 章节中的 `{label}` 仍是占位内容。")
            ok = False
    return ok


def _validate_current_card(card_path: Path, repo_root: Path) -> tuple[list[str], bool]:
    lines: list[str] = []
    if not card_path.exists():
        lines.append(f"  - 当前待施工卡不存在：`{card_path.relative_to(repo_root).as_posix()}`")
        return lines, False

    text = card_path.read_text(encoding="utf-8")
    ok = True

    if not _validate_labeled_section(
        lines=lines,
        section_name="需求",
        section_text=_find_section(text, "需求"),
        labels=REQUIREMENT_LABELS,
    ):
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
            lines.append("  - `## 设计输入` 章节缺少 `docs/01-design/` 设计文档链接。")
            ok = False
        if not spec_links:
            lines.append("  - `## 设计输入` 章节缺少 `docs/02-spec/` 规格文档链接。")
            ok = False
        for rel_path in design_links + spec_links:
            if not (repo_root / rel_path).exists():
                lines.append(f"  - `## 设计输入` 链接不存在：`{rel_path}`")
                ok = False

    task_section = _find_section(text, "任务分解")
    if not task_section:
        lines.append("  - 缺少 `## 任务分解` 章节。")
        ok = False
    else:
        task_items = [item for item in _collect_task_items(task_section) if not _is_placeholder(item)]
        if not task_items:
            lines.append("  - `## 任务分解` 章节仍是占位内容，缺少正式任务项。")
            ok = False

    if not _validate_labeled_section(
        lines=lines,
        section_name="历史账本约束",
        section_text=_find_section(text, "历史账本约束"),
        labels=LEDGER_LABELS,
    ):
        ok = False

    if ok:
        lines.append(
            f"  - 通过：当前待施工卡 `docs/03-execution/{card_path.name}` 已具备需求、设计、规格、任务分解与历史账本约束。"
        )
    return lines, ok


def run_check(repo_root: Path, paths: list[str] | None = None) -> tuple[list[str], bool]:
    lines = ["[doc-first-gating]"]
    card_path = _current_card_path(repo_root)
    if card_path is None:
        lines.append("  - 无法从 `B-card-catalog-20260409.md` 解析当前待施工卡。")
        return lines, False

    if not paths:
        lines.append("  - 当前口径：全仓扫描时验证当前待施工卡是否已脱离模板态并补齐历史账本约束。")
        detail_lines, ok = _validate_current_card(card_path, repo_root)
        lines.extend(detail_lines)
        return lines, ok

    normalized_paths = {rel for raw in paths if (rel := _normalize_path(repo_root, raw))}
    triggered = any(rel.startswith(prefix) for rel in normalized_paths for prefix in TRIGGER_PREFIXES)
    if not triggered:
        lines.append("  - 本次改动未命中 `src/`、`scripts/`、`.codex/`，不触发严格文档先行门禁。")
        return lines, True

    lines.append("  - 本次改动命中正式实现前缀，执行严格文档先行与历史账本约束门禁。")
    detail_lines, ok = _validate_current_card(card_path, repo_root)
    lines.extend(detail_lines)
    return lines, ok


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(args.repo_root).resolve()

    lines, ok = run_check(repo_root, paths=args.paths)
    output_text = "\n".join(lines)
    print(output_text)

    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            "\n".join(["# doc-first gating governance report", "", "```text", output_text, "```", ""]),
            encoding="utf-8",
        )

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
