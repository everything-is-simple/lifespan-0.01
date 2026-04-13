# structure/filter 官方 ledger replay 与 smoke 硬化

卡片编号：`44`
日期：`2026-04-13`
状态：`草稿`

## 需求

- 问题：
- 问题：
  `structure / filter` 虽已在 `35` 完成 queue/checkpoint 对齐，但这仍主要是 canonical downstream 级别的逻辑对齐，还没有像 `data / malf` 一样在官方本地 ledger 上证明 replay / smoke / audit 的运行质量。
- 目标结果：
- 目标结果：
  把 `structure / filter` 的官方本地 ledger 路径、默认 queue 运行口径、replay/resume 与真实 smoke 证据硬化到可作为 `position` 稳定上游的程度。
- 为什么现在做：
- 为什么现在做：
  `43` 已把进入 `position` 前的质量闸门冻结为正式前置条件；`44` 是其中针对 `structure / filter` 的第一张实质硬化卡。

## 设计输入

- 设计文档：
- 设计文档：
  - `docs/01-design/modules/system/13-structure-filter-official-ledger-replay-smoke-hardening-charter-20260413.md`
- 规格文档：
- 规格文档：
  - `docs/02-spec/modules/system/13-structure-filter-official-ledger-replay-smoke-hardening-spec-20260413.md`
  - `docs/03-execution/35-downstream-data-grade-checkpoint-alignment-after-malf-conclusion-20260412.md`
  - `docs/03-execution/38-structure-filter-mainline-legacy-malf-semantic-purge-conclusion-20260413.md`

## 任务分解

1. 盘点 `structure / filter` 当前官方本地 ledger 路径与默认运行口径。
2. 建立或验证 replay / resume / smoke 的真实官方库证据。
3. 裁决 `structure / filter` 是否已达到进入 `45` 的稳定上游标准。

## 实现边界

- 范围内：
- 范围内：
  - `structure / filter` 官方 ledger 路径与 replay/smoke 证据
  - `docs/03-execution/44-*`
- 范围外：
- 范围外：
  - `alpha formal signal producer`
  - `position`
  - `100-105`

## 历史账本约束

- 实体锚点：
- 实体锚点：
  `asset_type + code + timeframe='D'`
- 业务自然键：
- 业务自然键：
  `snapshot_date or bar_dt + contract version + source_fingerprint`
- 批量建仓：
- 批量建仓：
  bounded bootstrap 与官方库 bootstrap
- 增量更新：
- 增量更新：
  由 canonical upstream checkpoint 驱动 queue
- 断点续跑：
- 断点续跑：
  `work_queue + checkpoint + replay/resume`
- 审计账本：
- 审计账本：
  `structure_run / filter_run / checkpoint / snapshot / run_snapshot`

## 收口标准

1. `structure / filter` 官方 ledger 路径与运行口径写清
2. replay / smoke 证据写完
3. 记录与结论写完
4. 明确是否允许进入 `45`

## 卡片结构图

```mermaid
flowchart LR
    MALF["canonical malf"] --> STR["structure official ledger"]
    STR --> FLT["filter official ledger"]
    STR --> R["replay"]
    FLT --> S["smoke"]
    R --> G["44 结论"]
    S --> G
```
