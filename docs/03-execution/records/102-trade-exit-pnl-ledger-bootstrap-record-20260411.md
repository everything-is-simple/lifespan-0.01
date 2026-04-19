# trade exit pnl ledger bootstrap 记录

记录编号：`102`
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
    CARRY[trade_carry_snapshot] --> EXIT[退出事件触发]
    EXIT --> PNL[trade_exit_pnl_ledger]
    PNL --> AUDIT[PnL 审计账本]
    AUDIT --> OK[102卡收口]
```
