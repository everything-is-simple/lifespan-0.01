# 执行阅读顺序

`日期：2026-04-09`
`状态：持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `64-alpha-stage-percentile-decision-matrix-integration-conclusion-20260415.md`
5. `63-wave-life-official-ledger-truthfulness-and-bootstrap-conclusion-20260415.md`
6. `62-filter-pre-trigger-boundary-and-authority-reset-conclusion-20260415.md`
7. `61-structure-filter-tail-coverage-truthfulness-rectification-conclusion-20260415.md`
8. `60-mainline-rectification-batch-registration-and-scope-freeze-conclusion-20260415.md`
9. `65-formal-signal-admission-boundary-reallocation-card-20260415.md`
10. `66-mainline-rectification-resume-gate-card-20260415.md`
11. `80-mainline-middle-ledger-2011-2013-bootstrap-card-20260414.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `64`。
2. 当前正式主线待施工卡已切到 `65`，并顺排进入 `66`。
3. `29-64` 已完成并生效；当前主线后续卡组调整为：
   - `60-66`：主线整改、覆盖真值、职责边界与恢复闸门
   - `80-84`：整改后按三年窗口恢复正式中间库初始建库
   - `85`：整改后 `2026 YTD` 正式增量对齐
   - `86`：整改后 official middle-ledger cutover gate
   - `100-105`：只在 `86` 放行后恢复
4. `59` 的正式裁决仍是 `2010` pilot 的 truthfulness gate，但经 `61` 收紧后，只能被解释为 truthfulness，不得再外推成 completeness；`80-86` 的 structure/filter 历史建库默认必须走 bounded full-window。

## 阅读顺序图
```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["64 最新结论锚点"]
    ANC --> G60["65 当前待施工卡"]
    G60 --> G65["66 恢复闸门"]
    G65 --> G66["80-86"]
    G66 --> NEXT["100 下一锤"]
```
