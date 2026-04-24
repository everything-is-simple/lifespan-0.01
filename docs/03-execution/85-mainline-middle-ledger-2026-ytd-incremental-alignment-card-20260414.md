# mainline middle-ledger 2026 ytd incremental alignment 卡
`卡号`：`85`
`日期`：`2026-04-14`
`状态`：`待施工`

## 需求

- 问题：补齐历史三年窗口后，仍需要把正式库推进到当前年份，才能谈真实 cutover。
- 目标结果：以正式增量方式对齐 `2026-01-01 ~ 当前正式 market_base 最大 trade_date`。
- 为什么现在做：只有推进到当前年份，official middle-ledger cutover 才有现实意义。

## 设计输入

- 设计文档：`docs/01-design/modules/system/17-official-middle-ledger-phased-bootstrap-and-real-data-pilot-charter-20260414.md`
- 规格文档：`docs/02-spec/modules/system/17-official-middle-ledger-phased-bootstrap-and-real-data-pilot-spec-20260414.md`

## 任务分解

1. 使用既有 checkpoint / queue 对齐 `2026 YTD` canonical `malf`。
2. 对齐 `2026 YTD` downstream `structure / filter / alpha`。
3. 输出截至当前年份的正式 row-count、scope-count 与 freshness 摘要。

## 实现边界

- 范围内：`2026 YTD` 增量对齐。
- 范围外：`trade / system` 恢复。

## 历史账本约束

- 实体锚点：沿用正式实体锚点。
- 业务自然键：沿用正式自然键。
- 批量建仓：本卡以前提为 `57-64` 已完成，不重复历史窗口建仓。
- 增量更新：本卡正式承担 `2026 YTD` 对齐。
- 断点续跑：必须以 queue/checkpoint/resume 为默认路径，不允许用旁路补写代替正式增量。
- 审计账本：正式 run summary、freshness 摘要与 execution evidence / record / conclusion 共同审计。

## 收口标准

1. `2026 YTD` 已推进到当前正式 `market_base` 最大 `trade_date`。
2. canonical/downstream 增量路径可复验。
3. freshness 与 checkpoint 摘要可审计。
4. 为 `66` 提供 official cutover 输入。

## 卡片结构图

```mermaid
flowchart LR
    CP["window checkpoints"] --> Y65["2026 YTD alignment"]
    Y65 --> FR["freshness / summary"]
```
