# 进入 portfolio_plan 前的 position acceptance gate 规格

`生效日期`：`2026-04-13`
`状态`：`Active`

## 1. 接受前提

`51` 必须读取以下结论作为前置输入：

1. `47` 语义冻结完成
2. `48` risk/capacity ledger 完成
3. `49` batched entry/trim/partial-exit contract 完成
4. `50` data-grade runner 完成

## 2. 接受结果

`51` 只允许给出两类结果：

1. `accepted_for_portfolio_plan_and_trade_recovery`
2. `rejected_with_gap_list`

## 3. 强制检查项

1. `position_work_queue / checkpoint` 是否存在并可复跑。
2. risk budget 与 cap decomposition 是否能落回正式事实。
3. 分批进入与分批退出是否只是计划事实，没有越界成交。
4. `position` 对 `malf_context_4` 的消费是否稳定、显式、可审计。
5. 参考价与时间阶段是否保持当前系统定义。

