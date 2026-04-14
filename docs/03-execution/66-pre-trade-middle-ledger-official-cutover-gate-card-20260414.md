# pre-trade middle-ledger official cutover gate 卡
`卡号`：`66`
`日期`：`2026-04-14`
`状态`：`待施工`

## 需求

- 问题：即使 `56-65` 全部执行，也仍需要正式裁决真实正式库是否已经从 bridge-v1 切到 canonical mainline。
- 目标结果：给出 official middle-ledger cutover 的正式 gate，并决定是否恢复 `100`。
- 为什么现在做：`100-105` 不能建立在“可能已经切换”的状态之上。

## 设计输入

- 设计文档：`docs/01-design/modules/system/17-official-middle-ledger-phased-bootstrap-and-real-data-pilot-charter-20260414.md`
- 规格文档：`docs/02-spec/modules/system/17-official-middle-ledger-phased-bootstrap-and-real-data-pilot-spec-20260414.md`

## 任务分解

1. 审计 `2010 ~ 2026 YTD` 的 official middle-ledger 覆盖情况。
2. 核对 `structure / filter / alpha` 正式 run summary 是否已默认绑定 canonical。
3. 裁决是否允许恢复 `100-105`。

## 实现边界

- 范围内：official middle-ledger cutover gate，以及 `100` 放行 / 阻断裁决。
- 范围外：`trade / system` 实施与 bridge-v1 物理删除。

## 历史账本约束

- 实体锚点：沿用正式实体锚点。
- 业务自然键：沿用正式自然键。
- 批量建仓：本卡只审计 `57-64` 是否完整闭环，不新增历史建仓。
- 增量更新：本卡只审计 `65` 是否完成当前年份对齐。
- 断点续跑：本卡只审计 queue/checkpoint/freshness 是否闭环，不新增旁路执行逻辑。
- 审计账本：所有窗口卡的 evidence / record / conclusion 与正式 run summary 共同构成本卡审计输入。

## 收口标准

1. official middle-ledger 覆盖 `2010 ~ 当前年份`。
2. 默认主线已从 bridge-v1 切到 canonical。
3. `100` 是否放行有明确裁决。
4. evidence / record / conclusion 闭环。

## 卡片结构图

```mermaid
flowchart LR
    W56["56-65 official bootstrap"] --> G66["66 cutover gate"]
    G66 --> C100["allow 100"]
    G66 --> BLOCK["open more upstream cards"]
```
