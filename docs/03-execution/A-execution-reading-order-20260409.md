# 执行阅读顺序

`日期：2026-04-09`
`状态：持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `59-mainline-middle-ledger-2010-truthfulness-gate-conclusion-20260414.md`
5. `60-mainline-middle-ledger-2011-2013-bootstrap-card-20260414.md`
6. `64-mainline-middle-ledger-2023-2025-bootstrap-card-20260414.md`
7. `65-mainline-middle-ledger-2026-ytd-incremental-alignment-card-20260414.md`
8. `66-pre-trade-middle-ledger-official-cutover-gate-card-20260414.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `59`。
2. 当前正式主线待施工卡已切到 `60`，并顺排进入 `61 -> 62 -> 63 -> 64 -> 65 -> 66`。
3. `29-59` 已完成并生效；当前主线后续卡组调整为：
   - `60-64`：按三年窗口推进正式中间库初始建库
   - `65`：`2026 YTD` 正式增量对齐
   - `66`：official middle-ledger cutover gate
   - `100-105`：只在 `66` 放行后恢复
4. `59` 的正式裁决是“`2010` pilot 已足以作为 `60-65` 的真实正式库 middle-ledger 模板，但模板路径必须锁定为 `malf replay + structure/filter checkpoint_queue + alpha bounded full-window`”。

## 阅读顺序图
```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["59 最新结论锚点"]
    ANC --> G60["60 当前待施工卡"]
    G60 --> G65["61-65"]
    G65 --> G66["66"]
    G66 --> NEXT["100 下一锤"]
```
