# portfolio_plan 最小账本与 position 桥接设计章程

日期：`2026-04-09`
状态：`生效中`

## 问题

`13` 已经把 `alpha` 的 `trigger ledger / family ledger / formal signal` 三级正式账本层补齐，`position` 也已经具备官方 runner 与正式落账能力。

当前主链真正空着的，不再是继续深挖 `alpha`，而是：

1. 单标的 `position` 计划还没有被正式收编成组合层账本。
2. 组合级 `blocked / admitted / trimmed` 仍缺少最小正式解释层。
3. 如果继续把组合容量挤在 `position` 占位字段里，后续 `trade / system` 仍然看不到“为什么这次组合整体挡掉了某些机会”。

## 设计输入

本章程建立在下面这些已冻结来源之上：

1. `docs/01-design/modules/portfolio_plan/00-portfolio-plan-module-lessons-20260409.md`
2. `docs/01-design/modules/position/00-position-module-lessons-20260409.md`
3. `docs/01-design/modules/position/01-position-funding-management-and-exit-charter-20260409.md`
4. `docs/02-spec/modules/position/01-position-funding-management-and-exit-spec-20260409.md`
5. `docs/02-spec/modules/position/02-alpha-to-position-formal-signal-bridge-spec-20260409.md`
6. `docs/02-spec/modules/position/03-position-formal-signal-runner-spec-20260409.md`
7. `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
8. `docs/03-execution/09-position-formal-signal-runner-and-bounded-validation-conclusion-20260409.md`
9. `docs/03-execution/13-alpha-five-table-family-shared-contract-and-family-ledger-bootstrap-conclusion-20260409.md`
10. `G:\MarketLifespan-Quant\docs\01-design\modules\system\121-position-real-portfolio-capacity-and-total-cap-reset-charter-20260407.md`
11. `G:\MarketLifespan-Quant\docs\02-spec\modules\system\121-position-real-portfolio-capacity-and-total-cap-reset-spec-20260407.md`
12. `G:\MarketLifespan-Quant\docs\03-execution\293-position-real-portfolio-capacity-and-total-cap-reset-card-20260407.md`
13. `G:\MarketLifespan-Quant\docs\03-execution\293-position-real-portfolio-capacity-and-total-cap-reset-conclusion-20260407.md`

## 设计目标

本轮只冻结一件事：

把 `position` 的单标的正式计划，桥接成 `portfolio_plan` 的最小组合层正式账本，并先用 bounded pilot 证明：

1. 组合层容量与配额可以正式落账。
2. `position -> portfolio_plan` 的官方桥接可以成立。
3. 组合层 `blocked / admitted / trimmed` 可以留下可审计读数。

## 裁决一：下一锤切到 `portfolio_plan`，不继续停留在 `alpha`

当前主线最自然的下一张卡不是：

1. 回头继续补 `alpha` 的更细 payload / trace
2. 直接开 `trade`
3. 直接开 `system`

而是先把 `position` 之上的组合层最小正式账本立住。

原因是：

1. `alpha` 已经具备共享 trigger、family 与 formal signal 的最小正式分层。
2. `position` 已经能稳定消费官方 `alpha formal signal`。
3. 当前真正还薄的，是 `portfolio_plan` 这一层根本还没有正式主语和正式 runner。

## 裁决二：`portfolio_plan` 的主语是“组合计划裁决”，不是成交事实，也不是单标的 sizing

`portfolio_plan` 在新仓中的正式主语固定为：

`把一批 position 计划放到同一组合约束下后，回答哪些被 admitted、哪些被 blocked、哪些需要 trim，以及各自占用和剩余了多少组合容量。`

因此它回答的是：

1. 这一批单标的计划在组合里如何协调
2. 当前组合容量还剩多少
3. 哪些单标计划因为组合层约束被挡掉或被压缩

它不回答的是：

1. `alpha` 是否触发
2. 单标的 `position` 最多能做多大
3. `trade` 如何真实成交
4. `system` 如何汇总全链读数

## 裁决三：本轮先冻结最小三表，不一次性补齐全套组合回测与容量研究

本轮先正式冻结：

1. `portfolio_plan_run`
2. `portfolio_plan_snapshot`
3. `portfolio_plan_run_snapshot`

这三表足以先回答：

1. 本次组合层运行覆盖了哪些 `position` 候选
2. 哪些样本在组合层被 `admitted / blocked / trimmed`
3. 组合容量用了多少、还剩多少

本轮不宣称：

1. 组合回测体系已经全部正式化
2. 多组合、多策略簇、多账户分层已经建立
3. `trade` 已经能够消费最终组合计划

## 裁决四：`portfolio_plan` 只能消费官方 `position` 输出，不回读 `alpha` 内部过程

当前桥接方向固定为：

`alpha formal signal -> position -> portfolio_plan`

因此：

1. `portfolio_plan` 只允许消费官方 `position` 账本输出。
2. `portfolio_plan` 不允许为了组合裁决方便而回读 `alpha trigger / family / formal signal` 内部过程。
3. 组合层 blocked / admitted 必须在自己的账本层解释，不再把组合语义塞回 `position`。

## 裁决五：本轮先做 bounded pilot，不开 `trade`

本轮正式目标不是让组合计划直接变成订单，而是先让组合层历史账本存在。

因此：

1. pilot 必须真实写入 `H:\Lifespan-data\portfolio_plan\portfolio_plan.duckdb`
2. 只允许 bounded date window / bounded instrument slice / bounded candidate scope
3. 不允许把 `portfolio_plan` 本轮结果直接伪装成 `trade` 成交或持仓事实

## 裁决六：组合层也必须保留 blocked / admitted / trimmed 审计链

本轮正式拒绝“组合层挡掉了就直接消失”的旧习惯。

最小组合账本必须能显式回答：

1. 这次计划为什么 admitted
2. 这次计划为什么 blocked
3. 这次计划为什么被 trim
4. 本次组合运行结束后剩余组合容量是多少

## 模块边界

### 范围内

1. `position -> portfolio_plan` 最小桥接合同
2. 组合层最小 run / snapshot / run_snapshot 三表
3. admitted / blocked / trimmed 审计口径
4. bounded runner 与 bounded pilot

### 范围外

1. 回头改写 `position`
2. 直接开 `trade` 成交账本
3. 完整组合回测系统
4. 多账户、多组合簇全量治理

## 一句话收口

`14` 号卡要做的不是把组合层一次做完，而是先把 `position` 之上的最小组合计划账本立住，让新仓第一次正式回答“这些单标计划放进同一组合后，谁被放行、谁被挡掉、谁被压缩，以及组合容量还剩多少”。`
