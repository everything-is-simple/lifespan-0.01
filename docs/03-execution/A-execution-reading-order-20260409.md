# 执行阅读顺序

日期：`2026-04-09`  
状态：`持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `44-structure-filter-official-ledger-replay-smoke-hardening-conclusion-20260413.md`
5. `45-alpha-formal-signal-producer-hardening-before-position-card-20260413.md`
6. `46-pre-position-upstream-acceptance-gate-card-20260413.md`
7. `47-position-malf-context-driven-sizing-and-batch-contract-card-20260413.md`
8. `48-position-risk-budget-and-capacity-ledger-hardening-card-20260413.md`
9. `49-position-batched-entry-trim-and-partial-exit-contract-card-20260413.md`
10. `50-position-data-grade-checkpoint-and-replay-runner-card-20260413.md`
11. `51-pre-portfolio-plan-position-acceptance-gate-card-20260413.md`
12. `52-portfolio-plan-official-ledger-family-and-natural-key-freeze-card-20260413.md`
13. `53-portfolio-plan-capacity-decision-ledger-hardening-card-20260413.md`
14. `54-portfolio-plan-data-grade-checkpoint-replay-and-freshness-card-20260413.md`
15. `55-pre-trade-upstream-data-grade-baseline-gate-card-20260413.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `44`。
2. 当前正式主线待施工卡已切到 `45`，并顺排进入 `46 -> 47 -> 48 -> 49 -> 50 -> 51 -> 52 -> 53 -> 54 -> 55`。
3. `29-44` 已完成并生效，当前主线后续卡组调整为：
   - `45-46 alpha official hardening / acceptance`
   - `47-51 position quality / hardening / acceptance`
   - `52-55 portfolio_plan quality / hardening / acceptance`
   - `100-105 trade/system 收口`
4. `44` 已作为“structure/filter official replay / smoke 已硬化，可继续进入 `45`”的正式收口卡归档。

## 阅读顺序图

```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡片目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["44 最新结论锚点"]
    ANC --> G45["45 当前待施工卡"]
    G45 --> G46["46"]
    G46 --> G47["47"]
    G47 --> G48["48"]
    G48 --> G49["49"]
    G49 --> G50["50"]
    G50 --> G51["51"]
    G51 --> G52["52"]
    G52 --> G53["53"]
    G53 --> G54["54"]
    G54 --> G55["55"]
    G55 --> NEXT["100 下一锚"]
    NEXT --> POST["101-105 后续卡组"]
```
