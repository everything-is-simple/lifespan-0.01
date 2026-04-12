# mainline real-data smoke regression

卡片编号：`104`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  在 `103` 完成前跑全主线真实数据 smoke 没有意义，因为 `trade` 还不会真实推进到 exit/pnl。
- 目标结果：
  用 1-2 只真实股票跑通 `alpha -> position -> trade -> system` 的 bounded 主线回归。
- 为什么现在做：
  这是 trade 完整回测链条落地后的真实数据复核卡。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/10-mainline-real-data-smoke-regression-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/10-mainline-real-data-smoke-regression-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/103-trade-backtest-progression-runner-conclusion-20260411.md`

## 流程图

```mermaid
flowchart LR
    REAL[真实股票 1-2只 bounded] --> ALPHA[alpha]
    ALPHA --> POS[position]
    POS --> TRADE[trade]
    TRADE --> SYS[system readout]
    SYS --> EV[evidence 导出摘要]
```
