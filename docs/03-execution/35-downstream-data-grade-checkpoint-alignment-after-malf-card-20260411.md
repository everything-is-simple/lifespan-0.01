# downstream data-grade checkpoint alignment after malf

卡片编号：`35`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  canonical `malf` 已有 `work_queue + checkpoint + tail replay`，但 `structure / filter / alpha` 仍主要依赖 bounded 窗口物化，整链没有围绕 `malf` 的 replay 边界做增量运转。
- 目标结果：
  让 `structure / filter / alpha` 对齐到 data-grade 的 queue/checkpoint/replay 语义。
- 为什么现在做：
  不解决这一层，`malf` 只能是语义中心，不能成为下游真实运转中心。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/malf/12-downstream-data-grade-checkpoint-alignment-after-malf-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/malf/12-downstream-data-grade-checkpoint-alignment-after-malf-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/32-downstream-truthfulness-revalidation-after-malf-canonicalization-conclusion-20260411.md`

## 续跑图

```mermaid
flowchart LR
    MQ[malf checkpoint] --> SQ[structure checkpoint]
    SQ --> FQ[filter checkpoint]
    FQ --> AQ[alpha checkpoint]
```

## 任务分解

1. 为 `structure / filter / alpha` 冻结稳定实体锚点、自然键和最小表族。
2. 增补 `work_queue / checkpoint / replay` 表与 runner 契约。
3. 明确下游 dirty 单元如何由 canonical `malf` source advanced 驱动。
4. 补齐 replay/resume 的单元测试或可复现命令。
5. 回填 `35` 的 evidence / record / conclusion 与索引账本。

## 实现边界

- 范围内：
  - `docs/01-design/modules/malf/12-*`
  - `docs/02-spec/modules/malf/12-*`
  - `docs/03-execution/35-*`
  - `src/mlq/structure/`
  - `src/mlq/filter/`
  - `src/mlq/alpha/`
- 范围外：
  - `trade / system` live orchestration
  - 波段寿命概率 sidecar

## 历史账本约束

- 实体锚点：
  - 下游默认以 `asset_type + code + timeframe` 为脏单元锚点。
- 业务自然键：
  - 各模块正式 `snapshot_nk / signal_nk` 保持业务真值；queue/checkpoint 不替代业务自然键。
- 批量建仓：
  - 初次 bootstrap 允许全窗口补建 queue/checkpoint 与历史快照。
- 增量更新：
  - 日增量必须由 canonical `malf` source advanced 驱动挂账，不再默认全窗口重跑。
- 断点续跑：
  - 每个模块都必须声明 `last_completed_bar_dt / tail_start_bar_dt / tail_confirm_until_dt` 或等价边界。
- 审计账本：
  - 审计落在各模块 `run / work_queue / checkpoint` 与 `35` execution 闭环文档。

## 收口标准

1. `structure / filter / alpha` 已具备正式 queue/checkpoint/replay 机制。
2. 有证据证明下游增量范围服从 canonical `malf` replay 边界。
3. `conclusion` 明确 `malf` 已升级为下游增量运转中心。
