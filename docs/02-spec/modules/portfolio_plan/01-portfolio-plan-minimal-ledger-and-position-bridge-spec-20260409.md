# portfolio_plan 最小账本与 position 桥接规格

日期：`2026-04-09`
状态：`生效中`

## 适用范围

本规格用于冻结新仓 `portfolio_plan` 模块的最小正式输入、正式输出、自然键规则、增量规则与当前 bounded runner 合同。

本规格当前只覆盖：

1. `position -> portfolio_plan` 最小官方桥接
2. `portfolio_plan_run / snapshot / run_snapshot`
3. 组合级 `admitted / blocked / trimmed` 最小审计口径
4. bounded pilot 合同

本规格不代表：

1. 完整组合回测与绩效分析已经正式落齐
2. `trade / system` 已经能直接消费 `portfolio_plan`
3. 多组合、多账户、多资金池治理已经完成

## 正式输入

`portfolio_plan` 当前正式输入固定为三类：

1. 官方 `position` 账本输出
   - 至少提供：
     - `candidate_nk`
     - `instrument`
     - `policy_id`
     - `reference_trade_date`
     - `final_allowed_position_weight`
     - `position_action_decision`
     - `required_reduction_weight`
     - `candidate_status`
2. 组合层配置与容量合同
   - 至少提供：
     - `portfolio_id`
     - `portfolio_gross_cap_weight`
     - 可选的单批 admit 上限 / trim 策略
3. runner 自身 run 元数据
   - 包括：
     - `run_id`
     - `portfolio_plan_contract_version`
     - bounded window / bounded instrument scope

硬约束：

1. 不允许回读 `alpha trigger / family / formal signal` 内部过程。
2. 不允许把 `trade_runtime` 当前成交事实当成本轮必需输入。
3. 不允许把 `H:\Lifespan-temp` 中的临时聚合结果当正式组合账本。

## 正式输出

正式落点固定为：

`H:\Lifespan-data\portfolio_plan\portfolio_plan.duckdb`

当前 `v1` 最小正式表族固定为：

1. `portfolio_plan_run`
2. `portfolio_plan_snapshot`
3. `portfolio_plan_run_snapshot`

## 1. `portfolio_plan_run`

用途：

1. 记录一次 bounded portfolio planning run
2. 固定本次组合层运行的组合、范围、来源与摘要

最小字段：

1. `run_id`
2. `runner_name`
3. `runner_version`
4. `run_status`
5. `portfolio_id`
6. `signal_start_date`
7. `signal_end_date`
8. `bounded_candidate_count`
9. `admitted_count`
10. `blocked_count`
11. `trimmed_count`
12. `source_position_table`
13. `portfolio_plan_contract_version`
14. `started_at`
15. `completed_at`
16. `summary_json`

规则：

1. `run_id` 只做审计，不做组合计划主语义。
2. `summary_json` 必须能回答本次的 `admitted / blocked / trimmed` 统计与容量读数。
3. 非 `completed` run 不得被描述成正式验收证据。

## 2. `portfolio_plan_snapshot`

用途：

1. 保存组合层最小正式裁决事实
2. 成为 `position` 之上的组合层解释与协调层

最小字段：

1. `plan_snapshot_nk`
2. `candidate_nk`
3. `portfolio_id`
4. `instrument`
5. `reference_trade_date`
6. `position_action_decision`
7. `requested_weight`
8. `admitted_weight`
9. `trimmed_weight`
10. `plan_status`
11. `blocking_reason_code`
12. `portfolio_gross_cap_weight`
13. `portfolio_gross_used_weight`
14. `portfolio_gross_remaining_weight`
15. `portfolio_plan_contract_version`
16. `first_seen_run_id`
17. `last_materialized_run_id`
18. `created_at`
19. `updated_at`

状态枚举：

1. `admitted`
2. `blocked`
3. `trimmed`

自然键规则：

`v1` 固定由下面语义字段稳定拼出：

1. `candidate_nk`
2. `portfolio_id`
3. `reference_trade_date`
4. `portfolio_plan_contract_version`

规则：

1. 同一组合计划事实不得因为 `run_id` 变化而生成新主键。
2. 当组合容量输入或裁决规则变化导致结果变化时，必须显式记为 `rematerialized`。
3. blocked 样本也必须正式落表，不允许直接消失。

## 3. `portfolio_plan_run_snapshot`

用途：

1. 桥接某次 `run` 与本次触达的组合计划事实
2. 支持 bounded readout、resume 与 selective rebuild

最小字段：

1. `run_id`
2. `plan_snapshot_nk`
3. `candidate_nk`
4. `plan_status`
5. `materialization_action`
6. `recorded_at`

动作枚举：

1. `inserted`
2. `reused`
3. `rematerialized`

规则：

1. `run_snapshot` 是桥接层，不是新的组合计划主事实层。
2. 同一 `run_id + plan_snapshot_nk` 不得重复写入多行。
3. 该表必须支持每次 bounded pilot 的 per-action 统计。

## `position -> portfolio_plan` 最小桥接规则

当前固定桥接方向为：

`position_candidate_audit + position_capacity_snapshot + position_sizing_snapshot -> portfolio_plan_snapshot`

当前最小必需字段组为：

1. `candidate_nk`
2. `instrument`
3. `policy_id`
4. `reference_trade_date`
5. `candidate_status`
6. `position_action_decision`
7. `final_allowed_position_weight`
8. `required_reduction_weight`

字段职责冻结：

1. `position` 负责回答单标的最多能做多大。
2. `portfolio_plan` 负责回答放进同一组合后最终 admitted / blocked / trimmed 到多少。
3. `portfolio_plan` 不得回写 `position` 的 sizing 主语义。

## 最小裁决规则

当前 `v1` 的最小组合裁决规则固定为：

1. 若 `candidate_status != 'admitted'`，则 `plan_status='blocked'`
2. 若 `final_allowed_position_weight <= 0`，则 `plan_status='blocked'`
3. 若组合剩余容量足以容纳 `final_allowed_position_weight`，则：
   - `plan_status='admitted'`
   - `admitted_weight = final_allowed_position_weight`
   - `trimmed_weight = 0`
4. 若组合剩余容量大于 `0` 但不足以容纳 `final_allowed_position_weight`，则：
   - `plan_status='trimmed'`
   - `admitted_weight = portfolio_gross_remaining_weight`
   - `trimmed_weight = final_allowed_position_weight - admitted_weight`
5. 若组合剩余容量已为 `0`，则：
   - `plan_status='blocked'`
   - `blocking_reason_code='portfolio_capacity_exhausted'`

## 增量与 selective rebuild 规则

1. runner 必须支持 bounded window 执行。
2. runner 必须支持按 `portfolio_id`、`instrument` 与 candidate scope 裁切。
3. 同一 `plan_snapshot_nk` 重复命中时优先记为 `reused`。
4. 当组合容量合同或 position 输入变化导致计划结果变化时，必须显式记为 `rematerialized`。
5. 不允许为了 bounded pilot 方便而清空正式组合账本后重写。

## Portfolio Runner 合同

### Python 入口

正式 Python 入口固定命名为：

`run_portfolio_plan_build(...)`

### 脚本入口

正式脚本入口固定命名为：

`scripts/portfolio_plan/run_portfolio_plan_build.py`

### 最小参数

1. `run_id`
2. `portfolio_id`
3. `signal_start_date`
4. `signal_end_date`
5. `instruments`
6. `limit`
7. `source_position_table`
8. `portfolio_gross_cap_weight`
9. `summary_path`

### 明确禁止

1. 自动调用 `trade` runner
2. 自动写 `system`
3. 直接消费 `alpha` 内部未冻结过程
4. 把组合层 trim/block 结果重新塞回 `position` 充当主语义

## Bounded Evidence 要求

本卡完成时至少要留下：

1. design / spec / card 已齐备
2. 后续实现时至少有：
   - 单元测试
   - 一次真实写入 `H:\Lifespan-data` 的 bounded pilot
   - 一次正式库 readout
   - 一次 `position -> portfolio_plan` 桥接验证

## 当前明确不做

1. `trade` 成交与持仓账本
2. 多组合、多账户、全市场容量编排
3. 完整组合回测体系
4. 回头改写 `position` 或 `alpha`

## 一句话收口

`14` 的最小正式合同不是把组合层全部做完，而是先让 `position` 之上的组合计划裁决有一个可复用、可复物化、可审计的正式账本入口。`
