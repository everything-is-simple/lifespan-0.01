# system runtime / orchestration bootstrap 卡

卡片编号：`34`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  `27` 已经把主链结果上收到官方 `system` readout / audit 账本，但仓库仍缺正式 `system runtime / orchestration` 契约，用来回答“这次 bounded window 应该先跑哪些 official runner、哪些 step 可 reuse、失败后从哪里续跑、最终关联到哪个 `system_mainline_snapshot`”。
- 目标结果：
  建立 `system` 最小 runtime / orchestration bootstrap，把既有 official runner 串成一条纯本地、bounded、可续跑、可审计的 orchestration 主线。
- 为什么现在做：
  当前 `34` 是自然数顺排中的最后一张后置卡；只有在 `28-33` 先把 checkpoint、价格锚点、exit/pnl 与真实数据 smoke 夯实后，才适合进入 orchestration。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/04-system-runtime-orchestration-bootstrap-charter-20260411.md`
  - `docs/01-design/modules/system/00-system-module-lessons-20260409.md`
- 规格文档：
  - `docs/02-spec/modules/system/04-system-runtime-orchestration-bootstrap-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/27-system-mainline-bounded-acceptance-readout-and-audit-bootstrap-conclusion-20260411.md`

## 任务分解

1. 冻结 `system runtime / orchestration` 的最小正式对象、输入边界与 step 生命周期，明确只编排 official runner。
2. 建立最小 orchestration 表族、bounded runner 与 checkpoint / resume 机制，把 `structure -> filter -> alpha -> position -> portfolio_plan -> trade -> system readout` 串成正式本地主线。
3. 跑一组 bounded orchestration 验证，确认 step 级 `planned / running / completed / reused / failed / skipped` 审计与最终 `system_mainline_snapshot` bridge 成立。
4. 回填 `34` 的 evidence / record / conclusion，并裁决后续是否继续开更重 runtime / live orchestration 卡。

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/04-*`
  - `docs/02-spec/modules/system/04-*`
  - `docs/03-execution/34-*`
  - `docs/03-execution/evidence/34-*`
  - `docs/03-execution/records/34-*`
  - `src/mlq/system/*`
  - `scripts/system/*`
  - `tests/unit/system/*`
  - 执行索引与入口文件
- 范围外：
  - broker / account lifecycle adapter
  - live trading orchestration
  - filled / pnl / slippage / reconciliation 全量 runtime
  - 回写上游模块或重算既有业务事实

## 历史账本约束

- 实体锚点：
  以 `portfolio_id + orchestration_scene + bounded_window_end + orchestration_contract_version` 作为 `system_orchestration_run / checkpoint` 锚点，以 `run_id + step_seq` 或 `run_id + step_module + step_scope` 作为 step 锚点。
- 业务自然键：
  以 `orchestration_scene + portfolio_id + bounded_window_end` 作为 orchestration 主自然键，以 `run_id + step_seq` 或 `run_id + step_module + step_scope` 作为 step 自然键，以 `run_id + system_mainline_snapshot_nk` 作为最终 snapshot bridge 自然键。
- 批量建仓：
  首次对目标 `portfolio_id + bounded_window` 按固定 step 顺序全量生成 orchestration plan、step ledger 与 checkpoint 初始状态。
- 增量更新：
  后续只按新的 bounded window 或新的 step scope 增量推进，不默认重跑整仓历史。
- 断点续跑：
  orchestration 中断后，允许从最近 checkpoint 对应的 step 继续，而不是默认从 `structure` 重跑整条链。
- 审计账本：
  审计通过 `system_orchestration_run / system_orchestration_step / system_orchestration_checkpoint / system_orchestration_run_snapshot` 与 `34` 的 evidence / record / conclusion 留痕。

## 收口标准

1. `system runtime / orchestration` 最小正式表族、bounded runner 与 checkpoint 语义正式成立。
2. 有一组 bounded orchestration 验证证明 step 审计、reuse 和 resume 真实成立。
3. `34` 的 evidence / record / conclusion 与执行索引回填完整。
4. 能明确裁决后续是否继续开更重 runtime / live orchestration 卡。
