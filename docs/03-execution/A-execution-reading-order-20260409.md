# 执行阅读顺序

`日期：2026-04-09`
`状态：持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `75-raw-base-weekly-monthly-timeframe-ledger-bootstrap-conclusion-20260416.md`
5. `74-market-base-batched-bootstrap-governance-conclusion-20260416.md`
6. `73-market-base-backward-full-history-backfill-conclusion-20260416.md`
7. `72-historical-objective-profile-backfill-execution-conclusion-20260415.md`
8. `71-tushare-objective-source-ledger-and-profile-materialization-conclusion-20260415.md`
9. `76-raw-base-day-week-month-ledger-split-migration-card-20260417.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `75`。
2. 当前正式待施工卡已切到 `76-raw-base-day-week-month-ledger-split-migration-card-20260417.md`，用于把 `raw/base` 从单库多 timeframe 迁到日周月分库；本卡收口后再恢复到 `80`。
3. `73` 已完成 `market_base.stock_daily_adjusted(backward)` 全历史补齐，`74` 已完成 `raw/base` 分批建仓治理，`75` 已完成单库周月账本扩展；`76` 正在把 week/month 改成从 day 官方库派生并迁出独立物理库。
4. `76` 收口后，主线继续恢复到 `80-84` 的 official middle-ledger 分段建库、`85` 的 `2026 YTD` 正式增量对齐、`86` 的 official middle-ledger cutover gate，`100-105` 仍然只在 `76` 与 `86` 放行后恢复。

## 阅读顺序图

```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["75 最新结论锚点"]
    ANC --> G76["76 当前待施工卡"]
    G76 --> G80["80-86 后续卡组"]
    G80 --> NEXT["100-105 后置恢复"]
```
