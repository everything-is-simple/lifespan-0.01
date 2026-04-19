# mainline real data smoke regression 证据

证据编号：`104`
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
    REAL[真实 market_base 全量] --> SMOKE[主链 smoke runner]
    SMOKE --> DATA[data/malf/structure/filter/alpha]
    DATA --> MAIN[position/trade/system]
    MAIN --> OK[104 真实数据 smoke 回归收口]
```
