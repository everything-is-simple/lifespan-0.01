# position entry t-plus-1 open reference price correction 证据

证据编号：`101`
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
    SIG[formal signal T日] --> POS[position entry 参考价]
    POS -->|T+1开盘修正| OPEN[T+1 open price 锚点]
    OPEN --> EXEC[execution_plan 参考价修正]
    EXEC --> OK[101卡收口]
```
