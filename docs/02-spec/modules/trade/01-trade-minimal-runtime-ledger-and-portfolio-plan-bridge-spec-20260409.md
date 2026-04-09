# trade 最小 runtime 账本与 portfolio_plan 桥接规格

日期：`2026-04-09`
状态：`生效中`

## 适用范围

本规格用于冻结新仓 `trade` 模块的最小正式输入、正式输出、自然键规则、增量规则与当前 bounded runner 合同。

本规格当前只覆盖：

1. `portfolio_plan -> trade` 最小官方桥接
2. `trade_run / trade_execution_plan / trade_position_leg / trade_carry_snapshot / trade_run_execution_plan`
3. `T+1 开盘入场 / 1R / 半仓止盈 / 快速失败 / trailing / 时间止损` 的最小 policy 冻结
4. bounded pilot 合同

本规格不代表：

1. 完整 broker 模拟、会话管理与部分成交状态机已经落齐
2. `system` 已能直接消费全部 `trade` 细节
3. full replay、multi-account、做空/对冲已经完成

## 正式输入

`trade` 当前正式输入固定为三类：

1. 官方 `portfolio_plan` 账本输出
   - 至少提供：
     - `plan_snapshot_nk`
     - `candidate_nk`
     - `portfolio_id`
     - `instrument`
     - `reference_trade_date`
     - `requested_weight`
     - `admitted_weight`
     - `trimmed_weight`
     - `plan_status`
     - `blocking_reason_code`
2. `market_base.stock_daily_adjusted`
   - 至少提供：
     - `instrument`
     - `trade_date`
     - `open`
     - `high`
     - `low`
     - `close`
     - `adjust_method`
3. 上一轮官方 `trade_carry_snapshot`
   - 至少提供：
     - `instrument`
     - `snapshot_date`
     - `current_position_weight`
     - `open_leg_count`
     - `carry_source_status`

硬约束：

1. 不允许回读 `alpha trigger / family / formal signal` 内部过程。
2. 不允许把 `position` 临时中间表当成 `trade` 官方输入。
3. 不允许把 `H:\Lifespan-temp` 的临时回测结果当正式执行账本。

## 正式输出

正式落点固定为：

`H:\Lifespan-data\trade\trade_runtime.duckdb`

当前 `v1` 最小正式表族固定为：

1. `trade_run`
2. `trade_execution_plan`
3. `trade_position_leg`
4. `trade_carry_snapshot`
5. `trade_run_execution_plan`

## 1. `trade_run`

用途：

1. 记录一次 bounded trade runtime run
2. 固定本次执行层运行的范围、来源与摘要

最小字段：

1. `run_id`
2. `runner_name`
3. `runner_version`
4. `run_status`
5. `portfolio_id`
6. `signal_start_date`
7. `signal_end_date`
8. `bounded_plan_count`
9. `planned_entry_count`
10. `blocked_upstream_count`
11. `carried_open_leg_count`
12. `source_portfolio_plan_table`
13. `trade_contract_version`
14. `started_at`
15. `completed_at`
16. `summary_json`

规则：

1. `run_id` 只做审计，不做交易事实主语义。
2. `summary_json` 必须能回答 `planned_entry / blocked_upstream / carried_open_leg` 统计。
3. 非 `completed` run 不得被描述成正式验收证据。

## 2. `trade_execution_plan`

用途：

1. 保存组合计划进入 trade 后的最小统一执行意图
2. 成为后续 open leg、exit 与 replay 的上游主语

最小字段：

1. `execution_plan_nk`
2. `plan_snapshot_nk`
3. `candidate_nk`
4. `portfolio_id`
5. `instrument`
6. `signal_date`
7. `planned_entry_trade_date`
8. `execution_action`
9. `execution_status`
10. `requested_weight`
11. `planned_entry_weight`
12. `trimmed_weight`
13. `carry_source_status`
14. `entry_timing_policy`
15. `risk_unit_policy`
16. `take_profit_policy`
17. `fast_failure_policy`
18. `trailing_stop_policy`
19. `time_stop_policy`
20. `trade_contract_version`
21. `first_seen_run_id`
22. `last_materialized_run_id`
23. `created_at`
24. `updated_at`

动作枚举：

1. `enter`
2. `block_upstream`
3. `carry_forward`

状态枚举：

1. `planned_entry`
2. `blocked_upstream`
3. `planned_carry`

自然键规则：

`v1` 固定由下面语义字段稳定拼出：

1. `plan_snapshot_nk`
2. `planned_entry_trade_date`
3. `trade_contract_version`

规则：

1. 同一执行事实不得因为 `run_id` 变化而生成新主键。
2. 当上游 `portfolio_plan` 裁决或执行 policy 变化导致结果变化时，必须显式记为 `rematerialized`。
3. `blocked_upstream` 样本也必须正式落表，不允许直接消失。

## 3. `trade_position_leg`

用途：

1. 保存进入 trade 后形成的最小 open leg 事实
2. 为后续 carry 与 exit 留下正式主语

最小字段：

1. `position_leg_nk`
2. `execution_plan_nk`
3. `instrument`
4. `portfolio_id`
5. `leg_role`
6. `entry_trade_date`
7. `entry_weight`
8. `remaining_weight`
9. `leg_status`
10. `carry_eligible`
11. `trade_contract_version`
12. `first_seen_run_id`
13. `last_materialized_run_id`
14. `created_at`
15. `updated_at`

状态枚举：

1. `open`
2. `planned_zero_fill`
3. `closed`

规则：

1. `v1` 默认 `leg_role='core'`。
2. `execution_action='enter'` 且 `planned_entry_weight > 0` 时，必须生成或复用 `open` leg。
3. `block_upstream` 样本不得伪造 open leg。

## 4. `trade_carry_snapshot`

用途：

1. 保存给定 `asof_date` 下、可被下一 run 继续消费的正式持仓快照
2. 解决滚动 run 不得自动假定平仓的问题

最小字段：

1. `carry_snapshot_nk`
2. `snapshot_date`
3. `instrument`
4. `portfolio_id`
5. `current_position_weight`
6. `open_leg_count`
7. `carry_source_leg_nk`
8. `carry_source_run_id`
9. `carry_source_status`
10. `trade_contract_version`
11. `created_at`
12. `updated_at`

状态枚举：

1. `retained_open_leg_ready`
2. `flat_after_prior_run`
3. `no_prior_trade_run`

规则：

1. 如存在 `open` leg，则必须显式落 `retained_open_leg_ready`。
2. 如无上一轮 formal trade run，则显式落 `no_prior_trade_run`。
3. 如上一轮存在 run 但无 open leg，则显式落 `flat_after_prior_run`。
4. 禁止静默回退成“假定当前无持仓”。

## 5. `trade_run_execution_plan`

用途：

1. 桥接某次 `run` 与本次触达的执行事实
2. 支持 bounded readout、resume 与 selective rebuild

最小字段：

1. `run_id`
2. `execution_plan_nk`
3. `execution_status`
4. `materialization_action`
5. `recorded_at`

动作枚举：

1. `inserted`
2. `reused`
3. `rematerialized`

规则：

1. 同一 `run_id + execution_plan_nk` 不得重复写入多行。
2. 该表必须支持每次 bounded pilot 的 per-action 统计。

## `portfolio_plan -> trade` 最小桥接规则

当前固定桥接方向为：

`portfolio_plan_snapshot -> trade_execution_plan -> trade_position_leg -> trade_carry_snapshot`

当前最小裁决规则固定为：

1. 若 `plan_status='blocked'`，则：
   - `execution_action='block_upstream'`
   - `execution_status='blocked_upstream'`
   - 不生成 open leg
2. 若 `plan_status in ('admitted', 'trimmed')` 且 `admitted_weight > 0`，则：
   - `execution_action='enter'`
   - `execution_status='planned_entry'`
   - `planned_entry_weight = admitted_weight`
3. 下一交易日固定为 `planned_entry_trade_date`
4. 若存在上一轮 open leg，则必须额外生成或更新对应 `trade_carry_snapshot`

字段职责冻结：

1. `portfolio_plan` 负责回答组合层最终放行了多少。
2. `trade` 负责回答这些放行结果如何进入执行与持仓延续。
3. `trade` 不得回写 `portfolio_plan` 主语义。

## 最小 policy 冻结

当前 `v1` 固定冻结下面这些 execution policy 文本口径：

1. `entry_timing_policy = t_plus_1_open`
2. `risk_unit_policy = entry_open_minus_signal_low`
3. `take_profit_policy = half_at_1r`
4. `fast_failure_policy = t1_close_below_signal_low_then_t2_open_exit`
5. `trailing_stop_policy = break_last_higher_low`
6. `time_stop_policy = no_new_high_for_2_days_then_day_3_open_exit`

说明：

1. `v1` 首批实现只要求把这些 policy 冻结进正式执行计划。
2. 不要求本卡一次完成全部真实 exit 物化。
3. 后续 exit/replay 扩展必须沿用本轮冻结口径，不得再回到模糊描述。

## 增量与 selective rebuild 规则

1. runner 必须支持 bounded window 执行。
2. runner 必须支持按 `portfolio_id`、`instrument` 与 plan scope 裁切。
3. 同一 `execution_plan_nk` 重复命中时优先记为 `reused`。
4. 当上游计划或 policy 变化导致执行事实变化时，必须显式记为 `rematerialized`。
5. 不允许为了 bounded pilot 方便而清空正式 `trade_runtime` 后重写。

## Trade Runner 合同

### Python 入口

正式 Python 入口固定命名为：

`run_trade_runtime_build(...)`

### 脚本入口

正式脚本入口固定命名为：

`scripts/trade/run_trade_runtime_build.py`

### 最小参数

1. `run_id`
2. `portfolio_id`
3. `signal_start_date`
4. `signal_end_date`
5. `instruments`
6. `limit`
7. `source_portfolio_plan_table`
8. `market_price_table`
9. `summary_path`

### 明确禁止

1. 自动调用 `system`
2. 自动回读 `alpha` 内部过程
3. 把 `trade` 结果重新塞回 `portfolio_plan` 充当组合层主语义
4. 把 `trade_runtime` 扩成通用 broker sandbox

## Bounded Evidence 要求

本卡完成时至少要留下：

1. design / spec / card 已齐备
2. 后续实现时至少有：
   - 单元测试
   - 一次真实写入 `H:\Lifespan-data\trade\trade_runtime.duckdb` 的 bounded pilot
   - 一次正式库 readout
   - 一次 `portfolio_plan -> trade` 桥接验证
   - 一次 `inserted / reused / rematerialized` 复跑验证

## 当前明确不做

1. `system` 总装读数
2. 券商适配、会话状态与部分成交事件流
3. full replay / benchmark
4. 多账户、多组合簇调度

## 一句话收口

`15` 的最小正式合同不是把 trade 全部做完，而是先让 `portfolio_plan` 之下的执行意图、open leg 与 carry 快照有一个可复用、可复物化、可审计的正式账本入口。
