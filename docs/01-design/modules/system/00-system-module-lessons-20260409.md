# system 模块经验冻结

日期：`2026-04-09`
状态：`生效中`

## 当前职责

- 汇总 `malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade` 整条链
- 形成组合级读数、报告、复用与治理入口
- 负责编排、审计、冻结与系统级解释

## 必守边界

1. `system` 是消费层与汇总层，不拥有 trigger、filter、position、trade 的底层定义权。
2. `system` 报告不能只给 child run id，必须能解释 carry、blocked、filled 与组合级行为。
3. 结果复用必须有 identity 和 reuse 审计，不能把一次导出的报告当成永久天然真相。

## 已验证坑点

1. “模块都能跑”不等于“system 主线已成立”，必须有真实 bounded acceptance。
2. 研究验证主线基本闭环，不等于 live-ready。
3. 没有 broker / account lifecycle 与 adapter 前，不能把 system 说成已经上线就绪。

## 新系统施工前提

1. 所有系统级解释都必须建立在上游正式账本边界清楚的前提上。
2. 先稳主线解释与治理，不要让 `system` 反向定义下游事实。
3. 更高一层的后续重点应是 broker / account lifecycle pilot，而不是继续口径混写。

## 来源

1. 老系统总表 `battle-tested-lessons-all-modules-and-mainline-bridging-20260408.md`
2. 老系统 `system` 主线冻结与组合 acceptance 章程
