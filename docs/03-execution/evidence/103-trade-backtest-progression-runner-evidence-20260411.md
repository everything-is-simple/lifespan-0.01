# trade backtest progression runner 证据

证据编号：`103`
日期：`2026-04-11`

## 命令

```text
<commands here>
```

## 关键结果

- 结果 1
- 结果 2

## 产物

- 产物路径 1
- 产物路径 2

## 证据流图

```mermaid
flowchart LR
    HIST[历史 market_base] --> BT[trade backtest progression runner]
    BT --> PLAN[trade_execution_plan 逐日推进]
    PLAN --> PNL[pnl 账本累计]
    PNL --> OK[103卡收口]
```
