# position entry t+1 open reference price correction 规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `101-position-entry-t-plus-1-open-reference-price-correction-card-20260411.md` 及其后续 evidence / record / conclusion。

## 流程图

```mermaid
flowchart LR
    SIG[alpha signal T日] --> REF[T+1 open 参考价 none]
    REF --> SIZE[position sizing]
    SIZE --> TRADE[trade_position_leg 入场]
```
