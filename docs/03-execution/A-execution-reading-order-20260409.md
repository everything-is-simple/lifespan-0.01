# 执行阅读顺序

`日期：2026-04-09`
`状态：持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `77-raw-base-timeframe-split-tail-completion-conclusion-20260418.md`
5. `76-raw-base-day-week-month-ledger-split-migration-conclusion-20260417.md`
6. `75-raw-base-weekly-monthly-timeframe-ledger-bootstrap-conclusion-20260416.md`
7. `74-market-base-batched-bootstrap-governance-conclusion-20260416.md`
8. `73-market-base-backward-full-history-backfill-conclusion-20260416.md`
9. `80-mainline-middle-ledger-2011-2013-bootstrap-card-20260414.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `77`。
2. `77` 已完成六库尾收口：`index/block week/month raw/base` 已迁入新库，旧 `day` 库 `week/month` 价格表与 audit/dirty 尾巴已 purge，且 day bootstrap 已收窄为 day-only。
3. 当前正式待施工卡已切回 `80-mainline-middle-ledger-2011-2013-bootstrap-card-20260414.md`。
4. `80-86` 现在恢复为主线 official middle-ledger resume 卡组。
5. `100-105` 仍然只在 `86` 放行后恢复。

## 阅读顺序图

```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["77 最新结论锚点"]
    ANC --> G80["80 当前待施工卡"]
    G80 --> G86["80-86 后续卡组"]
    G86 --> NEXT["100-105 后置恢复"]
```
