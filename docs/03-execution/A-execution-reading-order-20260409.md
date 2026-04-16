# 执行阅读顺序

`日期：2026-04-09`
`状态：持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `74-market-base-batched-bootstrap-governance-conclusion-20260416.md`
5. `73-market-base-backward-full-history-backfill-conclusion-20260416.md`
6. `72-historical-objective-profile-backfill-execution-conclusion-20260415.md`
7. `71-tushare-objective-source-ledger-and-profile-materialization-conclusion-20260415.md`
8. `75-raw-base-weekly-monthly-timeframe-ledger-bootstrap-card-20260416.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `74`。
2. 当前正式待施工卡临时切到 `75`，用于补齐 `raw/base` 周月线正式账本；本卡收口后再恢复到 `80`。
3. `73` 已完成 `market_base.stock_daily_adjusted(backward)` 全历史补齐，`74` 已完成 `raw/base` 分批建仓治理；`75` 正在把 `data` 模块扩展为 `day/week/month` 通用正式入口。
4. `75` 收口后，主线继续恢复到 `80-84` 的 official middle-ledger 分段建库、`85` 的 `2026 YTD` 正式增量对齐、`86` 的 official middle-ledger cutover gate，`100-105` 仍只在 `86` 放行后恢复。

## 阅读顺序图
```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["74 最新结论锚点"]
    ANC --> G75["75 当前待施工卡"]
    G75 --> G80["80-86 后续卡组"]
    G80 --> NEXT["100-105 后置恢复"]
```
