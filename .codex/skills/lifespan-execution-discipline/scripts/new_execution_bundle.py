"""按新仓模板生成 execution 四件套，并可选回填索引。"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import re
import sys


REPO_ROOT = Path(__file__).resolve().parents[4]
EXECUTION_ROOT = REPO_ROOT / "docs" / "03-execution"
EVIDENCE_ROOT = EXECUTION_ROOT / "evidence"
RECORD_ROOT = EXECUTION_ROOT / "records"
CONCLUSION_CATALOG = EXECUTION_ROOT / "00-conclusion-catalog-20260409.md"
EVIDENCE_CATALOG = EVIDENCE_ROOT / "00-evidence-catalog-20260409.md"
CARD_CATALOG = EXECUTION_ROOT / "22-card-catalog-20260409.md"
COMPLETION_LEDGER = EXECUTION_ROOT / "77-system-completion-ledger-20260409.md"

TEMPLATE_PATHS = {
    "card": EXECUTION_ROOT / "00-card-template-20260409.md",
    "evidence": EXECUTION_ROOT / "00-evidence-template-20260409.md",
    "record": EXECUTION_ROOT / "00-record-template-20260409.md",
    "conclusion": EXECUTION_ROOT / "00-conclusion-template-20260409.md",
}


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数。"""

    parser = argparse.ArgumentParser(description="按模板生成 card / evidence / record / conclusion 四件套。")
    parser.add_argument("--number", required=True, help="执行卡编号，例如 02")
    parser.add_argument("--slug", required=True, help="文件名 slug，例如 governance-check")
    parser.add_argument("--title", required=True, help="展示标题，例如 治理检查修复")
    parser.add_argument("--date", default=datetime.today().strftime("%Y%m%d"), help="日期，格式 YYYYMMDD")
    parser.add_argument("--status", default="草稿", help="卡片状态，默认 草稿")
    parser.add_argument("--register", action="store_true", help="生成后顺手回填目录索引")
    parser.add_argument("--set-current-card", action="store_true", help="同步当前待施工卡与当前下一锤")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不写文件")
    return parser


def load_template(template_key: str) -> str:
    """读取模板内容。"""

    return TEMPLATE_PATHS[template_key].read_text(encoding="utf-8")


def build_targets(args: argparse.Namespace) -> dict[str, Path]:
    """构造目标文件路径。"""

    base = f"{args.number}-{args.slug}"
    return {
        "card": EXECUTION_ROOT / f"{base}-card-{args.date}.md",
        "evidence": EVIDENCE_ROOT / f"{base}-evidence-{args.date}.md",
        "record": RECORD_ROOT / f"{base}-record-{args.date}.md",
        "conclusion": EXECUTION_ROOT / f"{base}-conclusion-{args.date}.md",
    }


def render_template(template_key: str, args: argparse.Namespace) -> str:
    """按仓库模板渲染最小元信息。"""

    text = load_template(template_key)
    label_map = {
        "card": "卡片编号",
        "evidence": "证据编号",
        "record": "记录编号",
        "conclusion": "结论编号",
    }
    formatted_date = f"{args.date[:4]}-{args.date[4:6]}-{args.date[6:8]}"
    text = text.replace("<待填写>", args.number)
    text = text.replace("<yyyy-mm-dd>", formatted_date)
    text = text.replace("状态：`草稿`", f"状态：`{args.status}`")
    if template_key == "card":
        text = text.replace("# 执行卡模板", f"# {args.title}")
    if template_key == "evidence":
        text = text.replace("# 证据模板", f"# {args.title} 证据")
    if template_key == "record":
        text = text.replace("# 记录模板", f"# {args.title} 记录")
    if template_key == "conclusion":
        text = text.replace("# 结论模板", f"# {args.title} 结论")
    if f"{label_map[template_key]}：`{args.number}`" not in text:
        text = f"{label_map[template_key]}：`{args.number}`\n" + text
    return text


def find_section_bounds(lines: list[str], section_title: str) -> tuple[int, int]:
    """定位指定标题的区间边界。"""

    heading_index = -1
    heading_level = 0
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith("#"):
            continue
        marker, _, title = stripped.partition(" ")
        if title == section_title:
            heading_index = index
            heading_level = len(marker)
            break
    if heading_index == -1:
        raise ValueError(f"未找到分栏标题：{section_title}")

    section_end = len(lines)
    for index in range(heading_index + 1, len(lines)):
        stripped = lines[index].strip()
        if not stripped.startswith("#"):
            continue
        marker, _, _ = stripped.partition(" ")
        if len(marker) <= heading_level:
            section_end = index
            break
    return heading_index, section_end


def insert_numbered_entry(catalog_path: Path, section_title: str, entry_name: str, dry_run: bool) -> None:
    """向目录分栏插入新的编号项。"""

    text = catalog_path.read_text(encoding="utf-8")
    if f"`{entry_name}`" in text:
        print(f"- 已存在，跳过：{catalog_path.name} -> {entry_name}")
        return

    lines = text.splitlines()
    _, section_end = find_section_bounds(lines, section_title)

    last_number = 0
    insert_index = section_end
    pattern = re.compile(r"^(\d+)\.\s")
    for index in range(section_end - 1, -1, -1):
        stripped = lines[index].strip()
        if stripped.startswith("#"):
            break
        match = pattern.match(stripped)
        if match:
            last_number = int(match.group(1))
            insert_index = index + 1
            break

    new_line = f"{last_number + 1}. `{entry_name}`"
    print(f"- 预填索引：{catalog_path.name} [{section_title}] += {entry_name}")
    if dry_run:
        return

    lines.insert(insert_index, new_line)
    catalog_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def replace_first_match(text: str, pattern: str, replacement: str) -> str:
    """替换第一处匹配。"""

    new_text, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count == 0:
        raise ValueError(f"未找到需要替换的模式：{pattern}")
    return new_text


def sync_current_card(card_name: str, dry_run: bool) -> None:
    """同步卡目录与完工账本的当前施工卡。"""

    updates = [
        (
            CARD_CATALOG,
            [
                (r"(^1\.\s当前下一锤：)`[^`]+`", rf"\1`{card_name}`"),
                (r"(^2\.\s当前待施工卡：)`[^`]+`", rf"\1`{card_name}`"),
            ],
        ),
        (
            COMPLETION_LEDGER,
            [
                (r"(^1\.\s当前下一锤：)`[^`]+`", rf"\1`{card_name}`"),
            ],
        ),
    ]

    for path, replacements in updates:
        text = path.read_text(encoding="utf-8")
        new_text = text
        for pattern, replacement in replacements:
            new_text = replace_first_match(new_text, pattern, replacement)
        print(f"- 同步当前施工卡：{path.name} -> {card_name}")
        if not dry_run:
            path.write_text(new_text, encoding="utf-8")


def register_indexes(targets: dict[str, Path], dry_run: bool, *, set_current_card: bool) -> None:
    """回填结论目录、证据目录、卡目录与当前卡。"""

    print("开始回填索引：")
    insert_numbered_entry(CONCLUSION_CATALOG, "当前正式结论", targets["conclusion"].name, dry_run)
    insert_numbered_entry(EVIDENCE_CATALOG, "当前证据目录", targets["evidence"].name, dry_run)
    insert_numbered_entry(CARD_CATALOG, "卡片总表", targets["card"].name, dry_run)
    if set_current_card:
        sync_current_card(targets["card"].name, dry_run)


def write_bundle(args: argparse.Namespace) -> int:
    """执行生成流程。"""

    targets = build_targets(args)
    collisions = [path for path in targets.values() if path.exists()]
    if collisions:
        print("以下目标文件已存在，停止生成：", file=sys.stderr)
        for path in collisions:
            print(f"- {path}", file=sys.stderr)
        return 1

    rendered = {key: render_template(key, args) for key in targets}

    print("将生成以下文件：")
    for key, path in targets.items():
        print(f"- {key}: {path}")

    if args.dry_run:
        print("dry-run 模式，不写文件。")
        if args.register:
            register_indexes(targets, dry_run=True, set_current_card=args.set_current_card)
        return 0

    for path in targets.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    for key, content in rendered.items():
        targets[key].write_text(content, encoding="utf-8")

    if args.register:
        register_indexes(targets, dry_run=False, set_current_card=args.set_current_card)

    print("四件套已生成。")
    return 0


def main() -> int:
    """程序入口。"""

    parser = build_parser()
    args = parser.parse_args()
    return write_bundle(args)


if __name__ == "__main__":
    raise SystemExit(main())
