# structure filter alpha rebind to canonical malf

卡片编号：`31`
日期：`2026-04-11`
状态：`已完成`

## 需求

- 问题：
  即使 canonical malf 落地，如果 `structure / filter / alpha` 仍继续依赖 bridge-v1 近似输出，下游主线依旧不可信。`PAS` 虽不再是顶层模块，但作为 `alpha` 内部能力，同样需要重绑。
- 目标结果：
  把 `structure / filter / alpha` 的正式上游切换到 canonical malf。
- 为什么现在做：
  这是恢复下游业务可信度的第一张重绑卡。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/malf/08-structure-filter-alpha-rebind-to-canonical-malf-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/malf/08-structure-filter-alpha-rebind-to-canonical-malf-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/30-malf-canonical-ledger-and-data-grade-runner-bootstrap-conclusion-20260411.md`

## 任务分解

1. 把 `structure` 的正式上游切到 canonical malf。
2. 把 `filter / alpha / PAS` 的上游合同切到 canonical malf。
3. 清理 bridge-v1 在正式主线中的临时真值地位。

## 实现边界

- 范围内：
  - `docs/01-design/modules/malf/08-*`
  - `docs/02-spec/modules/malf/08-*`
  - `docs/03-execution/31-*`
  - `docs/03-execution/evidence/31-*`
  - `docs/03-execution/records/31-*`
  - `src/mlq/structure/*`
  - `src/mlq/filter/*`
  - `src/mlq/alpha/*`
  - `scripts/structure/*`
  - `scripts/filter/*`
  - `scripts/alpha/*`
- 范围外：
  - position/trade/system
  - live/runtime 语义

## 历史账本约束

- 实体锚点：
  `structure / filter / alpha` 各自仍保持本模块稳定业务锚点，但上游来源锚点切换为 canonical malf。
- 业务自然键：
  不以 `run_id` 代替下游业务键；上游引用只指向 canonical malf 正式 NK。
- 批量建仓：
  首次允许对下游 bounded window 全量重物化以完成上游重绑。
- 增量更新：
  后续只沿 canonical malf 增量事实更新。
- 断点续跑：
  各模块必须按 `28` 的统一约束支持 queue/checkpoint/resume。
- 审计账本：
  审计落在相关模块正式 run 表与 `31` 的 evidence / record / conclusion。

## 收口标准

1. `structure / filter / alpha` 正式依赖 canonical malf。
2. `PAS` 相关内部能力不再借 bridge-v1 近似上游运行。
3. 为 `32` 的 truthfulness revalidation 提供前提。
