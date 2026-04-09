# trade 最小 runtime 账本与 portfolio_plan 桥接设计章程

日期：`2026-04-09`
状态：`生效中`

## 问题

`14` 已经把 `portfolio_plan` 的最小正式账本与 `position -> portfolio_plan` 官方桥接立住。

当前主链真正空着的，不再是继续围绕 `portfolio_plan` 打补丁，而是：

1. `portfolio_plan` 给出的组合层裁决，还没有被正式接进 `trade_runtime`。
2. 新仓还没有一个能解释 `entry / carry / exit` 的最小官方 `trade` 账本入口。
3. 如果继续拖着不做，后续 `system` 只能看到组合计划，看不到真实交易意图、持仓延续与退出口径。

同时，老仓已经留下了两类必须吸收的硬经验：

1. 交易方式本身不是“通用券商模拟器”，而是有明确口径：
   `T+0 信号记录 / T+1 开盘入场 / 1R / 半仓止盈 / 快速失败 / trailing / 时间止损`
2. 没有 `carry_snapshot / retained open leg`，滚动 replay 与后续 sizing 会被人为切断。

## 设计输入

本章程建立在下面这些已冻结来源之上：

1. `docs/01-design/modules/trade/00-trade-module-lessons-20260409.md`
2. `docs/01-design/modules/portfolio_plan/00-portfolio-plan-module-lessons-20260409.md`
3. `docs/01-design/modules/portfolio_plan/01-portfolio-plan-minimal-ledger-and-position-bridge-charter-20260409.md`
4. `docs/02-spec/modules/portfolio_plan/01-portfolio-plan-minimal-ledger-and-position-bridge-spec-20260409.md`
5. `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
6. `docs/03-execution/14-portfolio-plan-minimal-ledger-and-position-bridge-conclusion-20260409.md`
7. `G:\。backups\MarketLifespan-Quant\docs\02-spec\modules\trade\01-trade-spec-20260320.md`
8. `G:\。backups\MarketLifespan-Quant\docs\02-spec\modules\trade\02-trade-pas-execution-kernel-spec-20260320.md`
9. `G:\。backups\MarketLifespan-Quant\docs\01-design\modules\system\123-trade-carry-and-retained-open-leg-contract-reset-charter-20260407.md`
10. `G:\。backups\MarketLifespan-Quant\docs\02-spec\modules\system\123-trade-carry-and-retained-open-leg-contract-reset-spec-20260407.md`
11. `G:\。backups\MarketLifespan-Quant\docs\03-execution\295-trade-carry-and-retained-open-leg-contract-reset-conclusion-20260407.md`
12. `G:\。backups\MarketLifespan-Quant\docs\03-execution\296-retained-carry-bounded-formal-acceptance-conclusion-20260407.md`
13. `G:\。backups\EmotionQuant-gamma\positioning\README.md`
14. `G:\。backups\EmotionQuant-gamma\normandy\90-archive\v0.01-alpha-provenance-execution-plan-20260311.md`

## 设计目标

本轮只冻结一件事：

把 `portfolio_plan` 的最小组合裁决，桥接成 `trade` 的最小官方 runtime 账本，并先用 bounded pilot 证明：

1. `portfolio_plan -> trade` 的官方桥接可以成立。
2. `trade_runtime` 至少已经能落下 entry 意图、open leg 与 carry 快照。
3. 老仓已验证的交易方式，可以先作为正式 policy 口径冻结，而不是继续散落在旧实验文档里。

## 裁决一：下一锤切到 `trade`，不直接跳 `system`

当前主线最自然的下一张卡不是：

1. 回头继续扩 `portfolio_plan`
2. 直接开 `system`
3. 重新打开 `alpha provenance`

而是先把 `portfolio_plan` 之下的 `trade` 最小正式账本立住。

原因是：

1. `portfolio_plan` 已经能正式回答组合层 `admitted / blocked / trimmed`。
2. 但系统主链还没有官方交易执行层，组合裁决没有下游主语。
3. `system` 若没有 `trade` 事实层，只能汇总计划，无法解释真实持仓与退出。

## 裁决二：`trade` 的主语是“执行事实与持仓延续”，不是组合研究，也不是通用 broker API

`trade` 在新仓中的正式主语固定为：

`把 portfolio_plan 已裁决的计划，转成统一执行意图、持仓腿与 carry 延续事实。`

因此它回答的是：

1. 哪个组合计划进入了交易执行层
2. 下一交易日准备如何入场
3. 当前 open leg 还剩多少
4. 哪些持仓需要继续 carry 到下一 run

它不回答的是：

1. `alpha` 是否触发
2. `portfolio_plan` 如何做组合容量协调
3. 券商适配、实盘会话与通用订单路由
4. `system` 的最终总装 readout

## 裁决三：老仓的“出手方式”先冻结成 policy，不先扩成全功能回测引擎

本轮明确吸收并冻结下面这些交易语义：

1. `T+0` 信号记录，`T+1 开盘` 入场
2. `1R = entry_open - signal_low`
3. `1R` 先减半
4. `T+1 收盘 < T+0 最低价` 触发快速失败
5. 剩余仓位进入 `higher-low break` trailing 管理
6. 连续 `2` 天不创新高，则第 `3` 天开盘按时间止损退出

但本轮不宣称：

1. 已完成全市场 full replay
2. 已完成券商仿真
3. 已完成多腿连续加仓体系

## 裁决四：carry 必须从第一张 trade 卡就成为正式事实

本轮正式拒绝“窗口结束时自动假设平仓”。

最小 `trade` 账本必须显式保留：

1. `trade_position_leg`
2. `trade_carry_snapshot`
3. `carry_source_status`

否则：

1. 下一轮 `position` 无法继续消费真实持仓
2. `trim / reduce` 会再次退回公式层存在、事实层缺失
3. `system` 仍然看不到真实交易延续

## 裁决五：本轮先建立最小 runtime 账本，不一次做完 exit/replay 全家桶

本轮先冻结最小主语和最小表族，优先让主链成立：

1. `trade_run`
2. `trade_execution_plan`
3. `trade_position_leg`
4. `trade_carry_snapshot`
5. `trade_run_execution_plan`

这足以先回答：

1. 哪些组合计划进入 trade
2. 本轮准备怎么执行
3. 哪些腿仍处于 open
4. 下一轮可消费的 carry 是什么

本轮不要求：

1. 完整 `exit_decision_event`
2. 全量 `replay_trade`
3. `system` 总装验收

## 模块边界

### 范围内

1. `portfolio_plan -> trade` 最小桥接合同
2. `trade_runtime` 最小 run / execution plan / open leg / carry snapshot
3. `inserted / reused / rematerialized` 的最小 rerun 审计
4. bounded runner 与 bounded pilot

### 范围外

1. 回头改写 `portfolio_plan`
2. 重开 `alpha` 或 `position` 研究问题
3. 通用 broker API / session / partial fill 仿真
4. `system` 总装读数

## 一句话收口

`15` 号卡要做的，不是一次做完整个 trade 引擎，而是先把 `portfolio_plan` 之下的最小执行账本、open leg 与 carry 主语正式立住，并把老仓已经验证过的出手方式冻结成新仓可施工的官方口径。
