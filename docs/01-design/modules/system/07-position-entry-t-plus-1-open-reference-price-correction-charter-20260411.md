# position entry t+1 open reference price correction 设计宪章

日期：`2026-04-11`
状态：`待执行`

## 背景

当前 `position` 参考价使用的是 `T+1 close`，而业务模型要求的是 `T+1 open`。这会直接污染 `R` 的大小、仓位 sizing 和后续短线出场边界。

## 设计目标

1. 把 `position` 的入场参考价从 `T+1 close` 改为 `T+1 open`。
2. 保持正式执行口径仍然是 `market_base.stock_daily_adjusted(adjust_method='none')`。
3. 对缺失开盘价或无下一交易日的情况给出明确处理语义。
