# mainline real data smoke regression 记录

记录编号：`104`
日期：`2026-04-11`

## 做了什么

1. 步骤 1
2. 步骤 2

## 偏离项

- 无，或说明偏离原因

## 备注

- 备注 1
- 备注 2

## 流程图

```mermaid
flowchart LR
    REAL[真实 market_base 全量] --> SMOKE[主链 smoke runner]
    SMOKE --> PIPE[data/malf/structure/filter/alpha/position/trade]
    PIPE --> OK[104 真实数据 smoke 回归收口]
```
