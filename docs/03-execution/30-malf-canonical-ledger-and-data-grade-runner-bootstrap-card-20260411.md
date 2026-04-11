# malf canonical ledger and data-grade runner bootstrap

卡片编号：`30`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  你要求 `malf` 数据库像 `data` 一样，既能一次性批量建仓，也能每日增量更新并支持断点续跑。当前 `malf` 只有有限 checkpoint，不具备完整 data-grade dirty queue。
- 目标结果：
  建立 canonical malf 的正式表族、bounded runner、dirty/work queue、checkpoint ledger 与 replay/resume 契约。
- 为什么现在做：
  没有 data-grade runner，canonical malf 就只是文档真值，不能成为正式上游。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/malf/07-malf-canonical-ledger-and-data-grade-runner-bootstrap-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/malf/07-malf-canonical-ledger-and-data-grade-runner-bootstrap-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/29-malf-semantic-canonical-contract-freeze-conclusion-20260411.md`

## 任务分解

1. 建立 canonical malf 正式表族与 bounded runner。
2. 冻结 batch build、daily incremental、dirty/work queue、checkpoint、resume 语义。
3. 回填 `30` 的 execution 文档与索引。

## 实现边界

- 范围内：
  - `docs/01-design/modules/malf/07-*`
  - `docs/02-spec/modules/malf/07-*`
  - `docs/03-execution/30-*`
  - `docs/03-execution/evidence/30-*`
  - `docs/03-execution/records/30-*`
  - `src/mlq/malf/*`
  - `scripts/malf/*`
  - `tests/unit/malf/*`
- 范围外：
  - 下游 rebind
  - trade/system 逻辑

## 历史账本约束

- 实体锚点：
  以 `code + timeframe + semantic_stage + trade_date` 作为 canonical malf 主锚点。
- 业务自然键：
  以 `code + timeframe + trade_date + semantic_stage` 作为业务自然键；`run_id` 只做审计。
- 批量建仓：
  首次全量扫描目标窗口，建立 canonical malf 正式表族与初始 queue/checkpoint。
- 增量更新：
  后续只按 dirty/work queue 和新增日期推进，不默认重跑整仓历史。
- 断点续跑：
  runner 中断后必须能从 queue + checkpoint 恢复。
- 审计账本：
  审计落在 `malf` 正式 `run / queue / checkpoint / replay` 账本与 `30` 的 evidence / record / conclusion。

## 收口标准

1. `malf` 具备 data-grade 批量建仓、增量更新与断点续跑能力。
2. canonical runner 成为正式入口。
3. 为 `31-32` 提供可信上游。
