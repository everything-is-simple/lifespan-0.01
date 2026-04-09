# 模块经验沉淀与执行入口字母化 记录

记录编号：`04`
日期：`2026-04-09`

## 对应卡片

- `docs/03-execution/04-module-lessons-and-execution-index-rename-card-20260409.md`

## 对应证据

- `docs/03-execution/evidence/04-module-lessons-and-execution-index-rename-evidence-20260409.md`

## 实施摘要

1. 基于老系统 `battle-tested-lessons-all-modules-and-mainline-bridging-20260408.md`，为新仓正式模块新增一组模块经验文档。
2. 给 `docs/01-design/modules/` 增加总入口，并同步刷新 `README.md`、`docs/README.md`、`AGENTS.md`。
3. 将执行区 3 个关键入口文件重命名为 `A-execution-reading-order`、`B-card-catalog`、`C-system-completion-ledger`。
4. 修复 `.codex` 执行脚本、治理脚本、测试与说明文档中的旧文件名引用。
5. 重新跑治理检查、执行索引检查和最小测试，并确认旧文件名已无残留引用。

## 偏离项与风险

- Windows 下固定 `pytest` basetemp` 仍存在偶发目录回收抖动；本轮沿用仓库既有做法，先清理 `H:\Lifespan-temp\pytest-tmp` 再跑测试。
- `portfolio_plan` 文档目前属于根据老系统 position/system 主线经验外推形成的正式边界草底，后续还需要专门执行卡继续细化正式表结构。
