# 全模块 checkpoint / dirty queue 对齐卡

卡片编号：`28`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前仓库虽然已经通过 `21` 把历史账本约束写成硬规则，但真实实现深度仍然明显分档：
  - `data`：已经做到 `file/request/instrument checkpoint + dirty queue + replay`
  - `malf`：只有 `instrument + timeframe checkpoint`
  - `structure / filter / alpha / position / portfolio_plan / trade / system`：多数仍停留在 `run_id + 自然键 upsert + reused/rematerialized`
  这与“所有本地库都要按 data-grade 对齐”的正式要求不一致。
- 目标结果：
  以 `data` 为标准，统一所有正式模块的 `queue / checkpoint / replay / resume` 契约，并把 `run_id` 收回到审计角色。
- 为什么现在做：
  如果不先统一这条基线，后续 `29-34` 都会继续建立在续跑颗粒度不一致的地基上。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/05-system-wide-checkpoint-and-dirty-queue-alignment-charter-20260411.md`
  - `docs/01-design/modules/system/00-system-module-lessons-20260409.md`
- 规格文档：
  - `docs/02-spec/modules/system/05-system-wide-checkpoint-and-dirty-queue-alignment-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/17-raw-base-strong-checkpoint-and-dirty-materialization-conclusion-20260410.md`
  - `docs/03-execution/21-system-ledger-incremental-governance-hardening-conclusion-20260410.md`
  - `docs/03-execution/25-malf-mechanism-ledger-bootstrap-and-downstream-sidecar-integration-conclusion-20260411.md`
  - `docs/03-execution/27-system-mainline-bounded-acceptance-readout-and-audit-bootstrap-conclusion-20260411.md`

## 任务分解

1. 盘点 `data / malf / structure / filter / alpha / position / portfolio_plan / trade / system` 当前的 queue、checkpoint、resume、dirty scope 与 replay 语义。
2. 为所有未达标模块冻结 data-grade 契约：稳定实体锚点、dirty/work queue、checkpoint ledger、bounded replay 与 resume 规则。
3. 明确 `28 -> 29 -> 30 -> 31 -> 32 -> 100 -> 101 -> 102 -> 103 -> 104 -> 105` 的自然数施工顺序，并把 malf 卡组放在 trade/system 卡组之前。
4. 回填 `28` 的 evidence / record / conclusion，并同步治理入口与执行索引。

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/05-*`
  - `docs/02-spec/modules/system/05-*`
  - `docs/03-execution/28-*`
  - `docs/03-execution/evidence/28-*`
  - `docs/03-execution/records/28-*`
  - 执行索引与入口文件
- 范围外：
  - 直接实现 `29-38` 的业务代码
  - broker / account lifecycle
  - live orchestration / live runtime
  - 任何绕开正式账本的临时脚本补丁

## 历史账本约束

- 实体锚点：
  以 `module_name + runner_name + scope_nk + contract_version` 作为 queue/checkpoint 实体锚点；标的类模块默认锚到 `asset_type + code + timeframe/stage/snapshot_date`。
- 业务自然键：
  以 `module_name + scope_nk + dirty_reason` 锚定 dirty/work queue，以 `module_name + scope_nk + checkpoint_stage` 锚定 checkpoint ledger；`run_id` 只做审计。
- 批量建仓：
  首次建仓时允许按官方 scope 全量扫描，生成 queue / checkpoint 初始账本与 run 审计。
- 增量更新：
  后续增量只允许按 dirty/work queue 与 checkpoint scope 推进，不默认重扫整仓历史。
- 断点续跑：
  任一官方 runner 中断后，必须能从 queue + checkpoint 恢复，而不是只靠 rerun + reused/rematerialized 模拟续跑。
- 审计账本：
  审计通过各模块正式 `run / queue / checkpoint / replay` 账本与 `28` 的 evidence / record / conclusion 留痕。

## 收口标准

1. `28` 正式裁决“所有本地库都必须以 data-grade checkpoint + dirty queue 为标准”。
2. 执行索引切换到自然数顺排，当前施工卡为 `28`，后续卡顺次为 `29-38`。
3. `29-32` 明确是 malf 优先卡组；`100-105` 必须排在其后。
4. `105-system-runtime-orchestration-bootstrap` 明确变成最后一张后置卡，而不是当前卡。
5. `28` 的 evidence / record / conclusion 与入口文件更新完整。
