# 文档先行硬门禁检查器 记录

记录编号：`03`
日期：`2026-04-09`

## 对应卡片

- `docs/03-execution/03-doc-first-gating-checker-card-20260409.md`

## 对应证据

- `docs/03-execution/evidence/03-doc-first-gating-checker-evidence-20260409.md`

## 实施摘要

1. 新增 `docs/01-design/04-doc-first-gating-checker-charter-20260409.md` 和 `docs/02-spec/04-doc-first-gating-checker-spec-20260409.md`，把文档先行硬门禁的目标、触发范围和卡片要求写成正式设计/规格。
2. 实现 `scripts/system/check_doc_first_gating_governance.py`，检查当前待施工卡是否具备需求、设计、规格和非占位任务分解。
3. 把新检查器串入 `scripts/system/check_development_governance.py`，并扩大 `check_entry_freshness_governance.py` 的治理入口触发范围。
4. 同步刷新 `AGENTS.md`、`README.md`、`pyproject.toml`、`docs/README.md`、`scripts/README.md` 与 `.codex` skill，统一仓库入口口径。
5. 新增最小单元测试，验证有效卡通过、占位卡失败。

## 偏离项与风险

- 当前门禁聚焦“当前待施工卡”，没有回溯扫描所有历史卡片；这是本轮有意保持最小可执行的取舍。
- `position` 的正式卡尚未打开，因此卡目录仍把 `03-doc-first-gating-checker-card-20260409.md` 保持为当前施工卡。
