# 执行文档目录治理回迁与固化 记录

记录编号：`68`
日期：`2026-04-15`

## 做了什么
1. 开 `68` 并把当前施工位切到执行文档目录治理回迁。
2. 盘点 `docs/03-execution/README.md`、`docs/README.md`、`docs/02-spec/00-repo-layout-and-docflow-spec-20260409.md` 与执行脚本，确认规则意图始终是 `evidence/`、`records/` 分离，真正失守的是索引检查与人工落档。
3. 修复 `.codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py`，让它同时兼容当前 `B-card-catalog-20260409.md` 的 `正式执行卡` 分栏标题。
4. 强化 `.codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py`，新增 `execution-layout` 检查并补上“只检查工作树真实存在文件”的过滤。
5. 把根目录错放的 `38-42`、`44`、`50-67` evidence / record 回迁到 `docs/03-execution/evidence/` 与 `docs/03-execution/records/`。
6. 更新 `00-conclusion-catalog`、`A/B/C`、`00-evidence-catalog`、`Ω`、`README.md`、`AGENTS.md`、`pyproject.toml` 与 `docs` 规范，使最新锚点推进到 `68`，当前待施工卡恢复到 `80`。

## 偏离项
- `44` 在根目录与子目录各有一份 evidence / record，且内容不一致；本次以根目录完整正式稿替换子目录占位稿，而不是简单保留较新的空模板版本。
- 新增的 `execution-layout` 检查最初误把“已删除待提交文件”当作仍在根目录存在；已在同一卡内补上工作树存在性过滤并复测通过。

## 备注

1. 本次治理后，后续新增执行四件套应优先走 bundle 脚本，不再允许手工把 `evidence / record` 落回根目录。
2. `68` 收口后，当前主线恢复为 `80 -> 81 -> 82 -> 83 -> 84 -> 85 -> 86 -> 100 -> 105`。

## 记录结构图
```mermaid
flowchart LR
    STEP["68 施工步骤"] --> SCRIPT["bundle/checker 修复"]
    SCRIPT --> MOVE["evidence/record 回迁"]
    MOVE --> DEV["偏离项处理"]
    DEV --> NOTE["恢复 80"]
    NOTE --> CON["68 结论引用"]
```
