# malf semantic canonical contract freeze

卡片编号：`29`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前 `malf` 的边界定义已经更新到最新口径，但实际 snapshot/materialization 仍然主要是 bridge-v1 近似实现。若不先冻结 canonical malf 语义，下游再推进都缺少可信上游。
- 目标结果：
  冻结最新 `malf core` 正式语义、bridge-v1 兼容范围与对下游的正式输出边界。
- 为什么现在做：
  这是继续推进整个系统之前的前置真值卡。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/malf/06-malf-semantic-canonical-contract-freeze-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/malf/06-malf-semantic-canonical-contract-freeze-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/23-malf-pure-semantic-ledger-boundary-freeze-conclusion-20260411.md`
  - `docs/03-execution/24-malf-mechanism-layer-break-confirmation-and-stats-sidecar-conclusion-20260411.md`
  - `docs/03-execution/25-malf-mechanism-ledger-bootstrap-and-downstream-sidecar-integration-conclusion-20260411.md`
  - `docs/03-execution/28-system-wide-checkpoint-and-dirty-queue-alignment-conclusion-20260411.md`

## 任务分解

1. 冻结 canonical malf 的正式语义原语与推进边界。
2. 明确 bridge-v1 哪些字段只属于兼容层，哪些不得再当作 malf 真值。
3. 写清 canonical malf 向 `structure / filter / alpha` 输出的正式合同。

## 实现边界

- 范围内：
  - `docs/01-design/modules/malf/06-*`
  - `docs/02-spec/modules/malf/06-*`
  - `docs/03-execution/29-*`
  - `docs/03-execution/evidence/29-*`
  - `docs/03-execution/records/29-*`
- 范围外：
  - 直接实现 canonical runner
  - 直接改写下游模块代码

## 历史账本约束

- 实体锚点：
  以 `asset_type + code + timeframe + signal_date + semantic_contract_version` 作为 canonical malf 语义实体锚点。
- 业务自然键：
  以 `code + timeframe + signal_date + progression_stage` 作为核心语义自然键；`run_id` 只做审计。
- 批量建仓：
  首次允许对既有语义样本全量定义 canonical 字段与桥接边界。
- 增量更新：
  后续只按新 bar、新阶段或新 dirty scope 增量推进语义定义。
- 断点续跑：
  允许按正式自然键幂等补写 contract freeze 产物，不允许靠 rerun 猜旧语义。
- 审计账本：
  审计落在 `29` 的 evidence / record / conclusion。

## 收口标准

1. canonical malf 语义被正式冻结。
2. bridge-v1 被明确降级为兼容层，而不是正式真值。
3. `30-32` 的正式输入成立。
