# trade exit pnl ledger bootstrap

卡片编号：`31`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前 `trade` 没有正式退出账本，无法沉淀部分退出、全退出与 realized pnl。
- 目标结果：
  建立最小 `trade` 退出账本，作为后续 progression runner 的正式写入目标。
- 为什么现在做：
  没有退出账本，就算后面写了推进循环，也没有正式历史账本可以落表。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/08-trade-exit-pnl-ledger-bootstrap-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/08-trade-exit-pnl-ledger-bootstrap-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/30-position-entry-t-plus-1-open-reference-price-correction-conclusion-20260411.md`

## 任务分解

1. 冻结最小退出账本表族与自然键。
2. 说明 `1R` 半仓与尾仓退出如何共同落账。
3. 回填 `31` 文档与索引。

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/08-*`
  - `docs/02-spec/modules/system/08-*`
  - `docs/03-execution/31-*`
  - `docs/03-execution/evidence/31-*`
  - `docs/03-execution/records/31-*`
- 范围外：
  - 逐日推进逻辑
  - 收益评估或策略分析

## 历史账本约束

- 实体锚点：
  以 `leg_nk` 为主锚点，以 `leg_nk + exit_seq` 为退出锚点。
- 业务自然键：
  `trade_leg_exit` 使用 `leg_nk + exit_seq`；`run_id` 只做审计。
- 批量建仓：
  首次建仓时允许为空表 bootstrap，不要求历史全回填。
- 增量更新：
  每次新增退出只追加新的 `exit_seq`。
- 断点续跑：
  中断后按 `leg_nk + exit_seq` 幂等补写。
- 审计账本：
  审计落在 `trade_run` 与 `31` 的 evidence / record / conclusion。

## 收口标准

1. 最小退出账本合同成立。
2. `1R` 半仓与尾仓退出都能落入同一正式账本。
3. 执行索引回填完成。
4. 能作为 `32` 的正式输入。
