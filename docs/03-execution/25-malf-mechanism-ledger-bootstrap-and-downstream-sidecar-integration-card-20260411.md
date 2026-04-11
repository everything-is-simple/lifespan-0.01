# malf 机制层 sidecar 账本 bootstrap 与下游接入

卡片编号：`25`
日期：`2026-04-11`
状态：`完成`

## 需求

- 问题：
  `24` 号卡虽然已经冻结了 `pivot-confirmed break` 与 `same-timeframe stats sidecar` 的正式边界，但当前仓内仍没有对应的正式账本表族、bounded runner、checkpoint 和最小下游接入；结果是这些能力仍停留在结论层，无法进入可续跑、可复算、可审计的正式实现。
- 目标结果：
  在不伪称 pure semantic canonical runner 已落地的前提下，新增 bridge-era 的 `malf` 机制层表族与 bounded runner，物化 `pivot_confirmed_break_ledger / same_timeframe_stats_profile / same_timeframe_stats_snapshot`，补齐 checkpoint，并完成 `structure / filter` 的最小只读 sidecar 接入与单测。
- 为什么现在做：
  如果这一步不执行，23/24 号卡就只能停在文档冻结层，无法验证 sidecar 合同、无法沉淀历史账本、也无法为后续 pure semantic canonical runner 提供过渡实现与 replay 基线。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/malf/04-malf-mechanism-layer-break-confirmation-and-same-timeframe-stats-sidecar-charter-20260411.md`
  - `docs/01-design/modules/malf/05-malf-mechanism-layer-ledger-bootstrap-charter-20260411.md`
  - `docs/01-design/modules/structure/01-structure-formal-snapshot-charter-20260409.md`
  - `docs/01-design/modules/filter/01-filter-formal-snapshot-charter-20260409.md`
- 规格文档：
  - `docs/02-spec/modules/malf/04-malf-mechanism-layer-break-confirmation-and-same-timeframe-stats-sidecar-spec-20260411.md`
  - `docs/02-spec/modules/malf/05-malf-mechanism-layer-ledger-bootstrap-spec-20260411.md`
  - `docs/02-spec/modules/structure/01-structure-formal-snapshot-spec-20260409.md`
  - `docs/02-spec/modules/filter/01-filter-formal-snapshot-spec-20260409.md`
- 当前锚点结论：
  - `docs/03-execution/24-malf-mechanism-layer-break-confirmation-and-stats-sidecar-conclusion-20260411.md`

## 任务分解

1. 为 `malf` 新增机制层表族：`malf_mechanism_run / checkpoint / pivot_confirmed_break_ledger / same_timeframe_stats_profile / same_timeframe_stats_snapshot`。
2. 新增 `run_malf_mechanism_build(...)` 与 `scripts/malf/run_malf_mechanism_build.py`，补齐 bounded 运行、增量续跑与 summary。
3. 以 bridge v1 输入实现最小 break 确认与同级别 stats 计算，不回写 `malf core`。
4. 为 `structure / filter` 增加最小只读 sidecar 接入字段和 runner 读取路径，但不改写其现有硬判断逻辑。
5. 补单测、运行证据、执行四件套与索引回填。

## 实现边界

- 范围内：
  - `src/mlq/malf/*`
  - `scripts/malf/*`
  - `src/mlq/structure/*`
  - `src/mlq/filter/*`
  - `tests/unit/malf/*`
  - `tests/unit/structure/*`
  - `tests/unit/filter/*`
  - `docs/01-design/modules/malf/*`
  - `docs/02-spec/modules/malf/*`
  - `docs/03-execution/25-*`
  - `docs/03-execution/evidence/25-*`
  - `docs/03-execution/records/25-*`
  - 执行索引与仓库入口文件
- 范围外：
  - `alpha / position / trade` 的 sidecar 消费改造
  - pure semantic canonical runner
  - 多级别背景系统
  - 动作接口

## 历史账本约束

- 实体锚点：
  `pivot_confirmed_break_ledger` 与 `same_timeframe_stats_snapshot` 以 `instrument + timeframe` 为主锚；`same_timeframe_stats_profile` 以 `universe + timeframe` 为主锚。
- 业务自然键：
  `pivot_confirmed_break_ledger` 使用 `instrument + timeframe + guard_pivot_id + trigger_bar_dt`；`same_timeframe_stats_profile` 使用 `universe + timeframe + regime_family + metric_name + sample_version`；`same_timeframe_stats_snapshot` 使用 `instrument + timeframe + asof_bar_dt + sample_version + stats_contract_version`。
- 批量建仓：
  先读取 bridge v1 `pas_context_snapshot + structure_candidate_snapshot`，再批量物化 break ledger、stats profile、stats snapshot，并将最小 sidecar 读数接入 `structure / filter`。
- 增量更新：
  仅对目标 `instrument + timeframe` 及新触达日期续算，依赖 checkpoint 控制增量边界；不跨级别混样本。
- 断点续跑：
  checkpoint 必须记到 `instrument + timeframe + last_signal_date / last_asof_date`；replay 需要支持按 `instrument`、时间窗口与 `run_id` 重放。
- 审计账本：
  每次运行都必须落 `malf_mechanism_run.summary_json`、执行 evidence/record/conclusion，并让 `structure / filter` 保留 sidecar 审计引用字段。

## 收口标准

1. 机制层表族、runner、checkpoint 已正式落地。
2. `structure / filter` 已具备最小只读 sidecar 接入。
3. 目标单测通过，且治理检查通过。
4. 不伪称 pure semantic canonical runner 已完成。
5. `25` 号四件套与索引回填完成。
