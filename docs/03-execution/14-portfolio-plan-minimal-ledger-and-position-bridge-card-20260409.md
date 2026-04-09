# portfolio_plan 最小账本与 position 桥接
卡片编号：`14`
日期：`2026-04-09`
状态：`已完成`

## 需求
- 问题：
  `13` 已经把 `alpha` 的三级正式账本补齐，`position` 也有了官方 runner。
  当前主链真正缺的，不再是继续围绕 `alpha` 或 `position` 打补丁，而是 `portfolio_plan` 这一层还没有最小正式主语、最小正式桥接和最小正式审计链。
- 目标结果：
  为新仓开出 `portfolio_plan` 的最小正式账本，只先做：
  `portfolio_plan_run / portfolio_plan_snapshot / portfolio_plan_run_snapshot`
  与 `position -> portfolio_plan` 最小桥接合同、bounded runner、一次真实写入 `H:\Lifespan-data` 的 official pilot，以及 `inserted / reused / rematerialized` 的复跑验证。
- 为什么现在做：
  `alpha -> position` 的官方桥接已经成立，继续深挖 `alpha` 的边际收益开始下降。
  如果 `portfolio_plan` 迟迟不开工，主链会继续停在“只有单标计划、没有组合裁决”的状态，`trade / system` 也没有稳定上游。

## 设计输入

- `docs/01-design/modules/portfolio_plan/00-portfolio-plan-module-lessons-20260409.md`
- `docs/01-design/modules/portfolio_plan/01-portfolio-plan-minimal-ledger-and-position-bridge-charter-20260409.md`
- `docs/01-design/modules/position/00-position-module-lessons-20260409.md`
- `docs/01-design/modules/position/01-position-funding-management-and-exit-charter-20260409.md`
- `docs/02-spec/modules/portfolio_plan/01-portfolio-plan-minimal-ledger-and-position-bridge-spec-20260409.md`
- `docs/02-spec/modules/position/01-position-funding-management-and-exit-spec-20260409.md`
- `docs/02-spec/modules/position/02-alpha-to-position-formal-signal-bridge-spec-20260409.md`
- `docs/02-spec/modules/position/03-position-formal-signal-runner-spec-20260409.md`
- `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
- `docs/03-execution/09-position-formal-signal-runner-and-bounded-validation-conclusion-20260409.md`
- `docs/03-execution/13-alpha-five-table-family-shared-contract-and-family-ledger-bootstrap-conclusion-20260409.md`
- `G:\MarketLifespan-Quant\docs\01-design\modules\system\121-position-real-portfolio-capacity-and-total-cap-reset-charter-20260407.md`
- `G:\MarketLifespan-Quant\docs\02-spec\modules\system\121-position-real-portfolio-capacity-and-total-cap-reset-spec-20260407.md`
- `G:\MarketLifespan-Quant\docs\03-execution\293-position-real-portfolio-capacity-and-total-cap-reset-card-20260407.md`
- `G:\MarketLifespan-Quant\docs\03-execution\293-position-real-portfolio-capacity-and-total-cap-reset-conclusion-20260407.md`

## 任务分解

1. 冻结 `portfolio_plan` 最小共享 contract 与组合层最小三表。
   - 明确 `portfolio_plan_run / snapshot / run_snapshot` 的自然键、审计字段与动作口径。
   - 明确 `admitted / blocked / trimmed` 的最小组合层语义。
2. 建立 `position -> portfolio_plan` 的最小正式桥接。
   - 只允许消费官方 `position` 账本输出。
   - 明确 `requested_weight / admitted_weight / trimmed_weight / portfolio_gross_remaining_weight` 的最小字段组。
3. 建立 bounded portfolio runner 与正式 pilot 口径。
   - 本轮必须真实写入 `H:\Lifespan-data\portfolio_plan\portfolio_plan.duckdb`。
   - 不允许停留在 temp-only，也不允许顺手开 `trade`。
4. 证明组合层也支持 `inserted / reused / rematerialized`。
   - 至少一次 unchanged rerun
   - 至少一次受控输入变化 rerun

## 实现边界

- 范围内：
  - `portfolio_plan_run / portfolio_plan_snapshot / portfolio_plan_run_snapshot`
  - `position -> portfolio_plan` 最小共享桥接字段
  - bounded runner 与脚本入口
  - 一次真实写入 `H:\Lifespan-data` 的 official pilot
  - 组合层 `admitted / blocked / trimmed` 最小 readout
- 范围外：
  - 回头改写 `alpha` 或 `position`
  - 直接开工 `trade / system`
  - 完整组合回测体系
  - 多账户、多组合簇治理

## 收口标准

1. `portfolio_plan` 最小三表成立。
2. 官方 `position -> portfolio_plan` 桥接成立。
3. 正式 pilot 真实写入 `H:\Lifespan-data\portfolio_plan\portfolio_plan.duckdb`。
4. 至少一轮 bounded readout 能回答哪些计划 `admitted / blocked / trimmed`。
5. 复跑验证能给出 `inserted / reused / rematerialized` 明确统计。
6. 证据写完。
7. 记录写完。
8. 结论写完。
