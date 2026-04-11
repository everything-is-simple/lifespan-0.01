# trade exit pnl ledger bootstrap

卡片编号：`102`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前 `trade` 没有正式退出账本，无法沉淀部分退出、全退出与 realized pnl。
- 目标结果：
  建立最小 `trade` 退出账本，作为后续 progression runner 的正式写入目标。
- 为什么现在做：
  `101` 修正 entry price 后，这张卡才能稳定定义 `1R` 与退出账本。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/08-trade-exit-pnl-ledger-bootstrap-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/08-trade-exit-pnl-ledger-bootstrap-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/101-position-entry-t-plus-1-open-reference-price-correction-conclusion-20260411.md`

## 任务分解

1. 冻结最小退出账本表族与自然键。
2. 说明 `1R` 半仓与尾仓退出如何共同落账。
3. 回填 `102` 文档与索引。
