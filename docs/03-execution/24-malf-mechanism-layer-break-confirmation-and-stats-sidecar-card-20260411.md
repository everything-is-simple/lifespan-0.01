# malf 机制层 break 确认与同级别统计 sidecar 卡

卡号：`24`
日期：`2026-04-11`
状态：`完成`

## 需求

 - 问题：
   `23` 号卡虽然已经把 `malf core` 收缩为纯语义走势账本，但 `pivot-confirmed break` 与 `same-timeframe stats sidecar` 仍停留在“下一步再说”的悬空状态，导致下游仍可能把它们各自私有化、重新写回 `malf core`，或者误当成新趋势确认条件。
 - 目标结果：
   在不回退 `23` 号纯语义 core 的前提下，正式冻结 `pivot-confirmed break` 的机制层身份、与 `break 触发 / 新推进确认` 的关系，以及 `same-timeframe stats sidecar` 的实体、自然键、批量建仓、增量更新和只读消费边界，并同步明确 `malf -> structure -> filter` 的正式读取顺序。
 - 为什么现在做：
   如果这一步不先收口，后续 `structure / filter` 会继续围绕各自私有字段扩张，最终又把统计、上下文或动作接口倒灌回 `malf`，使 `23` 号卡刚冻结的纯语义边界失效。

## 设计输入

 - 设计文档：
   - `docs/01-design/modules/malf/03-malf-pure-semantic-structure-ledger-charter-20260411.md`
   - `docs/01-design/modules/malf/04-malf-mechanism-layer-break-confirmation-and-same-timeframe-stats-sidecar-charter-20260411.md`
   - `docs/01-design/modules/structure/01-structure-formal-snapshot-charter-20260409.md`
   - `docs/01-design/modules/filter/01-filter-formal-snapshot-charter-20260409.md`
 - 规格文档：
   - `docs/02-spec/modules/malf/03-malf-pure-semantic-structure-ledger-spec-20260411.md`
   - `docs/02-spec/modules/malf/04-malf-mechanism-layer-break-confirmation-and-same-timeframe-stats-sidecar-spec-20260411.md`
   - `docs/02-spec/modules/structure/01-structure-formal-snapshot-spec-20260409.md`
   - `docs/02-spec/modules/filter/01-filter-formal-snapshot-spec-20260409.md`
 - 当前锚点结论：
   - `docs/03-execution/23-malf-pure-semantic-ledger-boundary-freeze-conclusion-20260411.md`

## 任务分解

1. 新增 `04` 号 malf mechanism layer design/spec，正式冻结 `pivot-confirmed break` 与 `same-timeframe stats sidecar` 的边界。
2. 把 `pivot-confirmed break` 与 `break trigger / new progression confirmation` 的关系写成正式三段式，不允许回写 `malf core`。
3. 冻结 `same_timeframe_stats_profile / same_timeframe_stats_snapshot` 的正式实体、自然键与只读消费方式。
4. 同步修订 `structure / filter` 的角色声明，明确它们只能按只读机制层 sidecar 消费这些能力。
5. 回填 `24` 号 card/evidence/record/conclusion 与执行索引、入口文件。

## 实现边界

 - 范围内：
   - `docs/01-design/modules/malf/*`
   - `docs/02-spec/modules/malf/*`
   - `docs/01-design/modules/structure/01-structure-formal-snapshot-charter-20260409.md`
   - `docs/01-design/modules/filter/01-filter-formal-snapshot-charter-20260409.md`
   - `docs/02-spec/modules/structure/01-structure-formal-snapshot-spec-20260409.md`
   - `docs/02-spec/modules/filter/01-filter-formal-snapshot-spec-20260409.md`
   - `docs/03-execution/24-*`
   - `docs/03-execution/evidence/24-*`
   - `docs/03-execution/records/24-*`
   - `docs/03-execution/00-conclusion-catalog-20260409.md`
   - `docs/03-execution/A-execution-reading-order-20260409.md`
   - `docs/03-execution/B-card-catalog-20260409.md`
   - `docs/03-execution/C-system-completion-ledger-20260409.md`
   - `docs/03-execution/evidence/00-evidence-catalog-20260409.md`
   - `AGENTS.md`
   - `README.md`
   - `pyproject.toml`
 - 范围外：
   - 新增 canonical mechanism runner 代码实现
   - 回测、胜率、收益、动作建议
   - 多级别 `context` 回写 `malf core`
   - 改写 `src/mlq/malf` 或 `scripts/malf/run_malf_snapshot_build.py`

## 历史账本约束

 - 实体锚点：
   `pivot-confirmed break` 事件与 `same-timeframe stats snapshot` 继续以 `instrument + timeframe` 为主锚；统计分布层以 `universe + timeframe` 为主锚。
 - 业务自然键：
   `pivot_confirmed_break_ledger` 以 `instrument + timeframe + guard_pivot_id + trigger_bar_dt` 为自然键；`same_timeframe_stats_profile` 以 `universe + timeframe + regime_family + metric_name + sample_version` 为自然键；`same_timeframe_stats_snapshot` 以 `instrument + timeframe + asof_bar_dt + sample_version + stats_contract_version` 为自然键。
 - 批量建仓：
   正式合同冻结为“先构造同级别 `malf core`，再派生 break 确认与 stats sidecar”；本卡不新增代码实现。
 - 增量更新：
   正式合同冻结为“仅对上游 `state / wave / progress` 发生变化的 `instrument + timeframe` 续算 sidecar，不跨级别混样本”。
 - 断点续跑：
   break 确认续跑必须能按 `guard_pivot_id + trigger_bar_dt` 重放；stats 续跑必须能按 `instrument + timeframe + asof_bar_dt + sample_version` 重放。
 - 审计账本：
   当前继续通过 `card / evidence / record / conclusion` 留痕；未来若实现正式机制层 runner，必须另开卡补齐 run/output 审计表族。

## 收口标准

1. `pivot-confirmed break` 是否进入 `malf core` 被正式写清。
2. `same-timeframe stats sidecar` 的边界被正式写清。
3. 不再把统计、背景或动作重新塞回 `malf core`。
4. `structure / filter` 的只读消费顺序被正式写清。
5. `24` 号四件套、执行索引与入口文件完成同步。
