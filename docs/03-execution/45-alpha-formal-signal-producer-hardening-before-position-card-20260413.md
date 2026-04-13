# alpha formal signal producer 在进入 position 前硬化

卡片编号：`45`
日期：`2026-04-13`
状态：`草稿`

## 需求

- 问题：
- 问题：
  `41/42` 已收口 detector 与 family，但 `alpha formal signal` 仍没有被正式证明达到进入 `position` 的稳定 producer 标准；如果跳过这一步，`100` 会在不稳定 producer 上冻结 trade anchor。
- 目标结果：
- 目标结果：
  明确 `alpha formal signal` 的正式输入、正式输出、compat-only 过渡字段与 replay/rematerialize 边界，并裁决它是否已经可以进入 `position`。
- 为什么现在做：
- 为什么现在做：
  `44` 先硬化 `structure / filter`，`45` 则把 `alpha` 的最终输出稳定性收口，作为 `46` integrated acceptance 的直接输入。

## 设计输入

- 设计文档：
- 设计文档：
  - `docs/01-design/modules/alpha/06-alpha-formal-signal-producer-hardening-before-position-charter-20260413.md`
- 规格文档：
- 规格文档：
  - `docs/02-spec/modules/alpha/06-alpha-formal-signal-producer-hardening-before-position-spec-20260413.md`
  - `docs/03-execution/41-alpha-pas-five-trigger-canonical-detector-conclusion-20260413.md`
  - `docs/03-execution/42-alpha-family-role-and-malf-alignment-conclusion-20260413.md`

## 任务分解

1. 盘点 `alpha formal signal` 当前 producer 的正式输入/输出边界。
2. 冻结 family 正式解释键与 formal signal 正式输出的关系。
3. 裁决 `alpha formal signal` 是否已达到进入 `46` 的稳定 producer 标准。

## 实现边界

- 范围内：
- 范围内：
  - `alpha formal signal` producer 稳定性
  - `docs/03-execution/45-*`
- 范围外：
- 范围外：
  - `position`
  - `100 signal anchor freeze`
  - `trade / system`

## 历史账本约束

- 实体锚点：
- 实体锚点：
  `asset_type + code + timeframe='D'`
- 业务自然键：
- 业务自然键：
  `formal_signal_event_nk + contract version + source_context_fingerprint`
- 批量建仓：
- 批量建仓：
  bounded bootstrap / historical backfill
- 增量更新：
- 增量更新：
  由 `alpha trigger checkpoint + upstream fingerprint` 驱动
- 断点续跑：
- 断点续跑：
  `queue + checkpoint + rematerialize`
- 审计账本：
- 审计账本：
  `alpha_formal_signal_run / event / run_event`

## 收口标准

1. `alpha formal signal` 的正式 producer 边界写清
2. compat-only 与正式字段区分写清
3. evidence / record / conclusion 写完
4. 明确是否允许进入 `46`

## 卡片结构图

```mermaid
flowchart LR
    DET["41 detector"] --> FAM["42 family"]
    FAM --> SIG["45 formal signal producer"]
    SIG --> G["45 结论"]
```
