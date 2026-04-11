# position entry t+1 open reference price correction

卡片编号：`101`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前 `position` 使用 `T+1 close` 作为参考价，与业务模型的 `T+1 open` 入场不一致。
- 目标结果：
  将 `position` 正式参考价切换到 `market_base.none` 的 `T+1 open`。
- 为什么现在做：
  `100` 已冻结信号锚点后，这张卡才有稳定上游可接。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/07-position-entry-t-plus-1-open-reference-price-correction-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/07-position-entry-t-plus-1-open-reference-price-correction-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/100-trade-signal-anchor-contract-freeze-conclusion-20260411.md`

## 任务分解

1. 冻结 `T+1 open` 的正式参考价口径。
2. 回填 `101` 文档与索引。
3. 为 `35-36` 提供 entry price 基线。
