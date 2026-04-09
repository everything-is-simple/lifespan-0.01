# alpha trigger ledger 与五表族最小物化
卡片编号：`12`
日期：`2026-04-09`
状态：`已完成`

## 需求
- 问题：
  `11` 已经收口 `structure / filter` 最小官方 snapshot，并让 `alpha` 默认切到官方上游。当前真正空着的，不是 `position` 再往回补，而是 `alpha` 内部正式中间账本还太薄：`formal signal` 已成立，但 `trigger ledger` 仍未在新仓正式沉淀为可长期累积、可复算、可审计的历史事实层。
- 目标结果：
  为新仓开出 `alpha` 内部下一层最小正式账本，只先做：
  `alpha_trigger_run / alpha_trigger_event / alpha_trigger_run_event`
  与五家族共享最小 contract、bounded runner、一次真实写入 `H:\Lifespan-data` 的 official pilot，以及 `inserted / reused / rematerialized` 复跑验证。
- 为什么现在做：
  `11` 解决的是 `alpha` 上游接口，
  `12` 应该解决的是 `alpha` 自己内部中间事实的正式永续化。
  如果这一步继续后推，主线就会重新滑回“围着 `position` 打转”或“过早跳 trade / system”，而不是先把 `alpha` 遗产最值得保住的一层沉淀成新仓正式资产。

## 设计输入

- `docs/01-design/modules/alpha/00-alpha-module-lessons-20260409.md`
- `docs/01-design/modules/alpha/01-alpha-formal-signal-output-charter-20260409.md`
- `docs/01-design/modules/alpha/02-alpha-trigger-ledger-and-five-table-family-minimal-materialization-charter-20260409.md`
- `docs/02-spec/modules/alpha/01-alpha-formal-signal-output-and-producer-spec-20260409.md`
- `docs/02-spec/modules/alpha/02-alpha-trigger-ledger-and-five-table-family-minimal-materialization-spec-20260409.md`
- `docs/03-execution/11-structure-filter-formal-contract-and-minimal-snapshot-conclusion-20260409.md`
- `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\04-pas-five-trigger-ledger-and-incremental-materialization-reset-20260408.md`
- `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\05-pas-full-market-five-trigger-ledger-backfill-reset-20260408.md`
- `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\06-pas-code-ledger-reset-and-2010-pilot-20260408.md`
- `G:\EmotionQuant-gamma\gene\03-execution\09-phase-g6-bof-pb-cpb-conditioning-card-20260316.md`
- `G:\EmotionQuant-gamma\gene\03-execution\16-phase-gx5-two-b-window-semantics-refactor-card-20260317.md`
- `G:\EmotionQuant-gamma\gene\03-execution\17-phase-gx6-123-three-condition-refactor-card-20260317.md`

## 任务分解

1. 冻结 `alpha trigger ledger` 最小正式合同。
   - 明确 `alpha_trigger_run / event / run_event` 三表、自然键、审计字段与 `inserted / reused / rematerialized` 动作口径。
   - 明确五家族共享最小 contract 如何容纳 `bof / tst / pb / cpb / bpb`。
2. 建立 bounded materialization runner 与正式 pilot 口径。
   - 只允许 bounded window / bounded instrument slice 方式落库。
   - 本轮必须真实写入 `H:\Lifespan-data\alpha\alpha.duckdb`，不允许停留在 temp-only。
3. 验证 `alpha_trigger_event -> alpha_formal_signal_event` 的正式上游关系。
   - 证明正式 trigger 事实能被 `formal signal` 稳定引用。
   - 证明重复运行时能正确区分 `inserted / reused / rematerialized`。

## 实现边界

- 范围内：
  - `alpha_trigger_run / alpha_trigger_event / alpha_trigger_run_event`
  - 五家族共享最小字段组与自然键
  - bounded runner 与脚本入口
  - 一次真实写入 `H:\Lifespan-data` 的 official pilot
  - bounded evidence、正式库 readout、rerun 审计
- 范围外：
  - 回头扩 `position`
  - 直接开工 `trade / system`
  - 五家族全部细节专表一次性补齐
  - full-market 全历史正式回填
  - 把 research sidecar 直接抬升为长期正式契约

## 收口标准

1. `alpha trigger ledger` 最小三表与 bounded runner 成立。
2. 正式 pilot 真实写入 `H:\Lifespan-data\alpha\alpha.duckdb`。
3. 复跑验证能给出 `inserted / reused / rematerialized` 明确统计。
4. `alpha_trigger_event -> alpha_formal_signal_event` 对接关系成立。
5. 证据写完。
6. 记录写完。
7. 结论写完。
