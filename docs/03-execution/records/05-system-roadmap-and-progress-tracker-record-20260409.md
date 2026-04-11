# 系统级路线图与进度跟踪器 记录

记录编号：`05`
日期：`2026-04-09`

## 对应卡片

- `docs/03-execution/05-system-roadmap-and-progress-tracker-card-20260409.md`

## 对应证据

- `docs/03-execution/evidence/05-system-roadmap-and-progress-tracker-evidence-20260409.md`

## 实施摘要

1. 新增路线图设计宪章，明确为什么需要系统级总视图与进度跟踪器。
2. 新增路线图规格，冻结状态枚举、栏目要求和刷新时机。
3. 新增系统级总路线图，把当前进度、各模块状态、下一锤、阻塞项和里程碑写成正式仓库文档。
4. 同步刷新 `AGENTS.md`、`README.md`、`docs/README.md` 与 `pyproject.toml`，让入口层直接暴露路线图入口。
5. 重新跑治理检查和执行索引检查，确认当前卡与入口联动口径一致。

## 偏离项与风险

- 当前模块状态仍然属于人工裁定版看板，不是自动从代码和数据库里采样生成；后续如果需要，可以再开卡做自动化状态采集。
- 当前路线图主要服务“系统级推进与施工顺序”，不替代具体模块的 design/spec/card。

## 流程图

```mermaid
flowchart LR
    CHARTER[路线图 charter/spec] --> ROADMAP[系统级总路线图]
    ROADMAP --> ENTRY[AGENTS.md/README.md 入口更新]
    ENTRY --> CHECK[治理+索引检查通过]
    CHECK --> OK[05卡收口]
```
