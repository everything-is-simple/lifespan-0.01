# mainline real-data smoke regression 规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `104-mainline-real-data-smoke-regression-card-20260411.md` 及其后续 evidence / record / conclusion。

## 流程图

```mermaid
flowchart LR
    REAL[真实股票 1-2只 bounded] --> ALPHA[alpha]
    ALPHA --> POS[position]
    POS --> TRADE[trade]
    TRADE --> SYS[system readout]
    SYS --> EV[evidence 导出摘要]
```
