# 执行阅读顺序

`日期：2026-04-09`
`状态：持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `76-raw-base-day-week-month-ledger-split-migration-conclusion-20260417.md`
5. `75-raw-base-weekly-monthly-timeframe-ledger-bootstrap-conclusion-20260416.md`
6. `74-market-base-batched-bootstrap-governance-conclusion-20260416.md`
7. `73-market-base-backward-full-history-backfill-conclusion-20260416.md`
8. `72-historical-objective-profile-backfill-execution-conclusion-20260415.md`
9. `77-raw-base-timeframe-split-tail-completion-card-20260418.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `76`。
2. 当前正式待施工卡已切到 `77-raw-base-timeframe-split-tail-completion-card-20260418.md`，用于完成 `index/block week/month` 迁移与旧 `day` 库 `week/month` purge 尾收口。
3. `76` 已完成六库路径契约、`stock week/month raw/base` 新库迁移、`month dirty/audit` 尾巴收口，并确认所有日线正式仍在 `raw_market.duckdb / market_base.duckdb`。
4. 当前未收口事实是：新 `week/month` 库只完成了 `stock`，`index/block week/month` 仍留在旧 `day` 库；`77` 收口后才恢复 `80-86`。
5. `100-105` 仍然只在 `77` 与 `86` 放行后恢复。

## 阅读顺序图

```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["76 最新结论锚点"]
    ANC --> G77["77 当前待施工卡"]
    G77 --> G80["80-86 后续卡组"]
    G80 --> NEXT["100-105 后置恢复"]
```
