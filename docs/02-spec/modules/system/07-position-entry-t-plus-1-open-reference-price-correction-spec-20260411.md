# position entry t+1 open reference price correction 规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `30-position-entry-t-plus-1-open-reference-price-correction-card-20260411.md` 及其后续 evidence / record / conclusion。

## 正式口径

1. `position` 参考价来自 `market_base.stock_daily_adjusted(adjust_method='none')`。
2. 参考价字段读取 `T+1` 交易日的 `open`。
3. 不允许自动 fallback 到 `close`。

## 异常语义

1. 若没有 `T+1` 交易日，则本次信号不物化为正式候选。
2. 若 `T+1 open` 为 `NULL`，必须落审计原因。
