# 全模块 checkpoint / dirty queue 对齐规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `28-system-wide-checkpoint-and-dirty-queue-alignment-card-20260411.md` 及其后续 evidence / record / conclusion。

## 目标

以 `data` 为标准，为全模块冻结正式 `queue / checkpoint / replay / resume` 语义。

## 必答问题

1. 每个模块的稳定实体锚点是什么。
2. 每个模块的 dirty/work queue 长什么样。
3. 每个模块的 checkpoint ledger 长什么样。
4. checkpoint 如何与 bounded replay / resume 对接。
5. `run_id` 在该模块里只保留哪些审计职责。
6. 为什么后续整改必须先于 `105-system-runtime-orchestration-bootstrap`。

## 范围

1. `data`
2. `malf`
3. `structure`
4. `filter`
5. `alpha`
6. `position`
7. `portfolio_plan`
8. `trade`
9. `system`

## 验收

1. `28` 能明确裁决所有本地库都必须以 data-grade 对齐。
2. 自然数顺序下的后续卡 `29-34` 被正式写入执行索引。
3. 入口文件与执行索引不再残留旧 `28/29` 口径。
