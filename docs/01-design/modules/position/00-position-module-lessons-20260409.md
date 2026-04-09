# position 模块经验冻结

日期：`2026-04-09`
状态：`生效中`

## 当前职责

- 把 formal signal 变成允许仓位、容量与资金管理事实
- 回答“这次可以做多大”
- 保留被阻断样本的审计链

## 必守边界

1. `position` 只消费 formal signal 和上下文，不负责发现 trigger。
2. `position` 给的是允许持仓与风险门控，不直接替代 `trade` 做执行解释。
3. blocked 场景也必须保留 audit candidate，而不是直接消失。

## 已验证坑点

1. 早期把离散倍数直接当最终仓位合同，导致上下文解释断裂。
2. `trim_to_context_cap` 如果只停留在公式里、不显式落表，下游会失去解释能力。
3. 没有把 `final_allowed_position_weight` 明确下发时，risk gate 很难解释“为什么这次被挡掉”。

## 新系统施工前提

1. position 账本要按资金管理方式分表，不再把所有方案混在一张大表里。
2. 单标的资金管理与组合容量要清楚分层，组合层留给 `portfolio_plan`。
3. 长期 position 事实要逐步对齐 code-ledger 口径。

## 来源

1. 老系统总表 `battle-tested-lessons-all-modules-and-mainline-bridging-20260408.md`
2. 老系统 `position` 章程与 `system 120/121/122` 系列章程
