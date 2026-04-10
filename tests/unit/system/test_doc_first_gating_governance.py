"""覆盖文档先行与历史账本约束门禁检查器。"""

from __future__ import annotations

import sys
from pathlib import Path


SCRIPT_ROOT = Path(__file__).resolve().parents[3] / "scripts" / "system"
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from check_doc_first_gating_governance import run_check  # noqa: E402


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _bootstrap_repo(tmp_path: Path, *, valid_card: bool) -> Path:
    repo_root = tmp_path / "repo"
    _write(
        repo_root / "docs" / "03-execution" / "B-card-catalog-20260409.md",
        "\n".join(
            [
                "# 卡片目录",
                "",
                "1. 当前下一锤：`03-doc-first-gating-checker-card-20260409.md`",
                "2. 当前待施工卡：`03-doc-first-gating-checker-card-20260409.md`",
                "",
            ]
        ),
    )

    if valid_card:
        _write(repo_root / "docs" / "01-design" / "04-doc-first-gating-checker-charter-20260409.md", "# charter\n")
        _write(repo_root / "docs" / "02-spec" / "04-doc-first-gating-checker-spec-20260409.md", "# spec\n")
        _write(
            repo_root / "docs" / "03-execution" / "03-doc-first-gating-checker-card-20260409.md",
            "\n".join(
                [
                    "# 文档先行硬门禁检查器",
                    "",
                    "卡片编号：`03`",
                    "日期：`2026-04-09`",
                    "状态：`执行中`",
                    "",
                    "## 需求",
                    "",
                    "- 问题：仓库已经有文档先行原则，但还没有真正的硬门禁检查器。",
                    "- 目标结果：新增正式治理检查器，在进入正式实现前卡住缺文档和缺账本约束声明的任务。",
                    "- 为什么现在做：在继续正式实现前，必须先把治理门禁写实。",
                    "",
                    "## 设计输入",
                    "",
                    "- 设计文档：`docs/01-design/04-doc-first-gating-checker-charter-20260409.md`",
                    "- 规格文档：`docs/02-spec/04-doc-first-gating-checker-spec-20260409.md`",
                    "",
                    "## 任务分解",
                    "",
                    "1. 新增文档先行检查器。",
                    "2. 把它接入开发治理入口。",
                    "",
                    "## 历史账本约束",
                    "",
                    "- 实体锚点：标的默认按 `asset_type + code`，不让 `run_id` 或名称接管主语义。",
                    "- 业务自然键：在实体锚点之上叠加 `trade_date`、状态键或窗口键。",
                    "- 批量建仓：首次 full bootstrap 要有明确入口。",
                    "- 增量更新：后续日更或批次增量要有明确判定逻辑。",
                    "- 断点续跑：checkpoint 或等价续跑锚点必须落表。",
                    "- 审计账本：run、时间戳、source、status/summary 必须保留。",
                    "",
                ]
            ),
        )
    else:
        _write(
            repo_root / "docs" / "03-execution" / "03-doc-first-gating-checker-card-20260409.md",
            "\n".join(
                [
                    "# 文档先行硬门禁检查器",
                    "",
                    "## 需求",
                    "",
                    "- 问题：",
                    "- 目标结果：",
                    "- 为什么现在做：",
                    "",
                    "## 设计输入",
                    "",
                    "- 设计文档：",
                    "- 规格文档：",
                    "",
                    "## 任务分解",
                    "",
                    "1. 切片 1：",
                    "2. 切片 2：",
                    "",
                    "## 历史账本约束",
                    "",
                    "- 实体锚点：",
                    "- 业务自然键：",
                    "- 批量建仓：",
                    "- 增量更新：",
                    "- 断点续跑：",
                    "- 审计账本：",
                    "",
                ]
            ),
        )

    return repo_root


def test_doc_first_gating_passes_for_valid_current_card(tmp_path: Path) -> None:
    repo_root = _bootstrap_repo(tmp_path, valid_card=True)

    lines, ok = run_check(repo_root, paths=["scripts/system/check_doc_first_gating_governance.py"])

    assert ok is True
    assert any("通过" in line for line in lines)


def test_doc_first_gating_fails_for_placeholder_card(tmp_path: Path) -> None:
    repo_root = _bootstrap_repo(tmp_path, valid_card=False)

    lines, ok = run_check(repo_root, paths=["src/mlq/core/paths.py"])

    assert ok is False
    assert any("占位" in line or "缺少" in line for line in lines)
