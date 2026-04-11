# trade backtest progression runner

卡片编号：`103`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前系统没有把 open leg 逐日推进到 closed 的正式回测引擎。
- 目标结果：
  实现基于日线 OHLC 的最小 progression runner，支持快速失败、`1R` 半仓和 `break_last_higher_low`。
- 为什么现在做：
  只有在 `100-102` 把锚点、entry price 和 exit ledger 都冻结后，这张卡才具备稳定输入。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/09-trade-backtest-progression-runner-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/09-trade-backtest-progression-runner-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/102-trade-exit-pnl-ledger-bootstrap-conclusion-20260411.md`
