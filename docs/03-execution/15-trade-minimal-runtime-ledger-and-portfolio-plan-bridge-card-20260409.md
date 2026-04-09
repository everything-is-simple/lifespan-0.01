# trade 最小 runtime 账本与 portfolio_plan 桥接
卡片编号：`15`
日期：`2026-04-09`
状态：`待施工`

## 需求
- 问题：
  `14` 已经把 `portfolio_plan` 最小账本与 `position -> portfolio_plan` 官方桥接立住，但新仓主链仍停在“只有组合裁决、没有交易执行层主语”的状态。
  当前真正缺的，不再是继续扩 `portfolio_plan`，而是 `trade_runtime` 还没有最小正式表族、最小官方 bridge 和最小 bounded pilot。
- 目标结果：
  为新仓开出 `trade` 的最小正式 runtime 账本，只先做：
  `trade_run / trade_execution_plan / trade_position_leg / trade_carry_snapshot / trade_run_execution_plan`
  与 `portfolio_plan -> trade` 最小桥接合同、bounded runner、一次真实写入 `H:\Lifespan-data` 的 official pilot，以及 `inserted / reused / rematerialized` 的复跑验证。
- 为什么现在做：
  `portfolio_plan` 已经正式回答组合层的 `admitted / blocked / trimmed`，继续停在组合计划层的边际收益开始下降。
  如果 `trade` 迟迟不开工，后续 `system` 仍然看不到 entry、open leg、carry 与退出 policy 的最小正式解释层。

## 设计输入

- `docs/01-design/modules/trade/00-trade-module-lessons-20260409.md`
- `docs/01-design/modules/trade/01-trade-minimal-runtime-ledger-and-portfolio-plan-bridge-charter-20260409.md`
- `docs/01-design/modules/portfolio_plan/00-portfolio-plan-module-lessons-20260409.md`
- `docs/01-design/modules/portfolio_plan/01-portfolio-plan-minimal-ledger-and-position-bridge-charter-20260409.md`
- `docs/02-spec/modules/trade/01-trade-minimal-runtime-ledger-and-portfolio-plan-bridge-spec-20260409.md`
- `docs/02-spec/modules/portfolio_plan/01-portfolio-plan-minimal-ledger-and-position-bridge-spec-20260409.md`
- `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
- `docs/03-execution/14-portfolio-plan-minimal-ledger-and-position-bridge-conclusion-20260409.md`
- `G:\。backups\MarketLifespan-Quant\docs\02-spec\modules\trade\01-trade-spec-20260320.md`
- `G:\。backups\MarketLifespan-Quant\docs\02-spec\modules\trade\02-trade-pas-execution-kernel-spec-20260320.md`
- `G:\。backups\MarketLifespan-Quant\docs\01-design\modules\system\123-trade-carry-and-retained-open-leg-contract-reset-charter-20260407.md`
- `G:\。backups\MarketLifespan-Quant\docs\02-spec\modules\system\123-trade-carry-and-retained-open-leg-contract-reset-spec-20260407.md`
- `G:\。backups\MarketLifespan-Quant\docs\03-execution\295-trade-carry-and-retained-open-leg-contract-reset-conclusion-20260407.md`
- `G:\。backups\MarketLifespan-Quant\docs\03-execution\296-retained-carry-bounded-formal-acceptance-conclusion-20260407.md`
- `G:\。backups\EmotionQuant-gamma\positioning\README.md`
- `G:\。backups\EmotionQuant-gamma\normandy\90-archive\v0.01-alpha-provenance-execution-plan-20260311.md`

## 任务分解

1. 冻结 `trade_runtime` 最小共享 contract 与执行层最小五表。
   - 明确 `trade_run / trade_execution_plan / trade_position_leg / trade_carry_snapshot / trade_run_execution_plan` 的自然键、审计字段与动作口径。
   - 明确 `planned_entry / blocked_upstream / planned_carry` 的最小执行层语义。
2. 建立 `portfolio_plan -> trade` 的最小正式桥接。
   - 只允许消费官方 `portfolio_plan` 账本输出。
   - 明确 `plan_snapshot_nk / admitted_weight / plan_status / carry_source_status` 的最小字段组。
3. 冻结最小 execution policy 口径。
   - 本轮必须把 `T+1 开盘入场 / 1R / 半仓止盈 / 快速失败 / trailing / 时间止损` 写成正式 policy 字段。
   - 不允许继续只留在旧仓文字描述里。
4. 建立 bounded trade runner 与正式 pilot 口径。
   - 本轮必须真实写入 `H:\Lifespan-data\trade\trade_runtime.duckdb`。
   - 不允许停留在 temp-only，也不允许顺手开 `system`。
5. 证明执行层也支持 `inserted / reused / rematerialized`。
   - 至少一次 unchanged rerun
   - 至少一次受控输入变化 rerun

## 实现边界

- 范围内：
  - `trade_run / trade_execution_plan / trade_position_leg / trade_carry_snapshot / trade_run_execution_plan`
  - `portfolio_plan -> trade` 最小共享桥接字段
  - execution policy 最小冻结
  - bounded runner 与脚本入口
  - 一次真实写入 `H:\Lifespan-data` 的 official pilot
  - 执行层 `planned_entry / blocked_upstream / planned_carry` 最小 readout
- 范围外：
  - 回头改写 `portfolio_plan`
  - 直接开 `system`
  - 通用 broker API / session / partial fill 仿真
  - 完整 replay / benchmark / 多账户治理

## 收口标准

1. `trade_runtime` 最小五表成立。
2. 官方 `portfolio_plan -> trade` 桥接成立。
3. execution policy 最小口径已冻结入正式字段。
4. 正式 pilot 真实写入 `H:\Lifespan-data\trade\trade_runtime.duckdb`。
5. 至少一轮 bounded readout 能回答哪些计划进入了 `planned_entry / blocked_upstream / planned_carry`。
6. 复跑验证能给出 `inserted / reused / rematerialized` 明确统计。
7. 证据写完。
8. 记录写完。
9. 结论写完。
