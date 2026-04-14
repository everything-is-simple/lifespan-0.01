# 执行阅读顺序

日期：`2026-04-09`  
状态：`持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `52-portfolio-plan-official-ledger-family-and-natural-key-freeze-conclusion-20260414.md`
5. `53-portfolio-plan-capacity-decision-ledger-hardening-card-20260413.md`
6. `54-portfolio-plan-data-grade-checkpoint-replay-and-freshness-card-20260413.md`
7. `55-pre-trade-upstream-data-grade-baseline-gate-card-20260413.md`
8. `100-trade-signal-anchor-contract-freeze-card-20260411.md`
9. `101-position-entry-t-plus-1-open-reference-price-correction-card-20260411.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `52`。
2. 当前正式主线待施工卡已切到 `53`，并顺排进入 `54 -> 55`。
3. `29-52` 已完成并生效，当前主线后续卡组调整为：
   - `53-55 portfolio_plan quality / hardening / acceptance`
   - `100-105 trade/system 收口`
4. `52` 已作为“portfolio_plan 官方账本族与自然键已冻结，并继续冻结 `100-105` 直到 `55`”的正式收口卡归档。

## 阅读顺序图

```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡片目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["52 最新结论锚点"]
    ANC --> G53["53 当前待施工卡"]
    G53 --> G54["54"]
    G54 --> G55["55"]
    G55 --> NEXT["100 下一锚"]
    NEXT --> POST["101-105 后续卡组"]
```
