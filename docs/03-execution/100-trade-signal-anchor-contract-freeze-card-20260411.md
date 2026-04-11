# trade signal anchor contract freeze

卡片编号：`100`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  在 canonical malf 完成前冻结 `trade` 锚点会建立在不可信上游之上；现在这张卡必须后移到 malf 卡组之后。
- 目标结果：
  在 canonical malf 已被 `32` 裁决通过后，冻结 `signal_low / last_higher_low` 的跨模块透传合同。
- 为什么现在做：
  `100` 是恢复 trade 链路的第一张卡，但前提是 `32` 已完成。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/06-trade-signal-anchor-contract-freeze-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/06-trade-signal-anchor-contract-freeze-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/32-downstream-truthfulness-revalidation-after-malf-canonicalization-conclusion-20260411.md`

## 任务分解

1. 冻结 `signal_low / last_higher_low` 的正式来源层。
2. 明确 `alpha -> position -> trade` 的桥接。
3. 回填 `100` 的 execution 文档与索引。

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/06-*`
  - `docs/02-spec/modules/system/06-*`
  - `docs/03-execution/100-*`
- 范围外：
  - 回测推进循环
  - live/runtime 语义

## 历史账本约束

- 实体锚点：
  `formal_signal_nk / position_candidate_nk / leg_nk`
- 业务自然键：
  以正式 NK 透传，`run_id` 只做审计。
- 批量建仓：
  对既有 bounded 窗口允许历史补齐透传字段。
- 增量更新：
  新信号只做单向透传。
- 断点续跑：
  中断后允许按正式自然键幂等补写。
- 审计账本：
  审计落在相关模块 run 表与 `100` execution 文档。

## 收口标准

1. `signal_low / last_higher_low` 合同成立。
2. `101-103` 的正式输入成立。
