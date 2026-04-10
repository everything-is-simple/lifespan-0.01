# malf 模块 market_base 到最小语义 snapshot 桥接规格

日期：`2026-04-10`
状态：`生效中`

## 适用范围

本规格冻结新仓 `malf` 的最小正式上游合同，当前只覆盖：

1. `malf_run`
2. `pas_context_snapshot`
3. `structure_candidate_snapshot`
4. `malf_run_context_snapshot`
5. `malf_run_structure_snapshot`
6. `run_malf_snapshot_build(...)`
7. `scripts/malf/run_malf_snapshot_build.py`

## 正式输入

当前正式输入固定为：

`market_base.stock_daily_adjusted`

最小必需字段：

1. `code`
2. `name`
3. `trade_date`
4. `adjust_method`
5. `open`
6. `high`
7. `low`
8. `close`

默认输入范围固定为 `adjust_method = backward` 的股票日线。

## 正式输出

### 1. `malf_run`

用途：

1. 记录一次 bounded `market_base -> malf` 物化
2. 固定来源、窗口、版本与摘要

最小字段：

1. `run_id`
2. `runner_name`
3. `runner_version`
4. `run_status`
5. `signal_start_date`
6. `signal_end_date`
7. `bounded_instrument_count`
8. `source_price_table`
9. `adjust_method`
10. `malf_contract_version`
11. `started_at`
12. `completed_at`
13. `summary_json`

### 2. `pas_context_snapshot`

用途：

1. 作为 `structure / filter / alpha` 的最小市场上下文层

最小字段：

1. `context_nk`
2. `entity_code`
3. `entity_name`
4. `signal_date`
5. `asof_date`
6. `source_context_nk`
7. `malf_context_4`
8. `lifecycle_rank_high`
9. `lifecycle_rank_total`
10. `calc_date`
11. `adjust_method`
12. `first_seen_run_id`
13. `last_materialized_run_id`

自然键规则：

`context_nk = entity_code + entity_name + signal_date + asof_date + malf_contract_version`

`malf_context_4` 最小枚举固定为：

1. `BULL_MAINSTREAM`
2. `BEAR_MAINSTREAM`
3. `RANGE_BALANCED`
4. `RECOVERY_MAINSTREAM`

### 3. `structure_candidate_snapshot`

用途：

1. 作为 `structure` 的最小官方候选事实输入

最小字段：

1. `candidate_nk`
2. `instrument`
3. `instrument_name`
4. `signal_date`
5. `asof_date`
6. `new_high_count`
7. `new_low_count`
8. `refresh_density`
9. `advancement_density`
10. `is_failed_extreme`
11. `failure_type`
12. `adjust_method`
13. `first_seen_run_id`
14. `last_materialized_run_id`

自然键规则：

`candidate_nk = instrument + instrument_name + signal_date + asof_date + malf_contract_version`

### 4. `malf_run_context_snapshot`

用途：

1. 记录一次 `run` 触达了哪些上下文快照

最小字段：

1. `run_id`
2. `context_nk`
3. `materialization_action`
4. `recorded_at`

### 5. `malf_run_structure_snapshot`

用途：

1. 记录一次 `run` 触达了哪些结构候选快照

最小字段：

1. `run_id`
2. `candidate_nk`
3. `materialization_action`
4. `recorded_at`

## 计算口径

本轮最小口径固定为：

1. `new_high_count`
   - 统计当前收盘价突破最近 `20 / 60 / 120` 个交易日历史高点的窗口个数
2. `new_low_count`
   - 统计当前收盘价跌破最近 `20 / 60 / 120` 个交易日历史低点的窗口个数
3. `refresh_density`
   - `new_high_count / 3`
4. `advancement_density`
   - 最近 `10` 个交易日上涨日占比
5. `is_failed_extreme`
   - 当日创更高窗口新高但收盘弱于开盘且弱于前收，或创更低窗口新低时为真
6. `failure_type`
   - `failed_extreme` 或 `failed_breakdown`
7. `malf_context_4`
   - 由 `20 / 60` 日均线相对位置与近 `20` 日涨跌幅共同裁决

## runner 合同

### Python 入口

`run_malf_snapshot_build(...)`

### 脚本入口

`scripts/malf/run_malf_snapshot_build.py`

### 最小参数

1. `signal_start_date`
2. `signal_end_date`
3. `instruments`
4. `limit`
5. `batch_size`
6. `adjust_method`
7. `run_id`
8. `summary_path`

## 下游对齐规则

`structure` 后续正式消费必须优先读取：

1. `pas_context_snapshot.entity_code / signal_date / asof_date / source_context_nk / malf_context_4 / lifecycle_rank_high / lifecycle_rank_total`
2. `structure_candidate_snapshot.instrument / signal_date / asof_date / new_high_count / new_low_count / refresh_density / advancement_density / is_failed_extreme / failure_type`

## 价格口径边界

1. `malf` 正式语义层固定读取 `backward`
2. `position / trade` 的执行与参考定价固定回到 `none`
3. `malf` 不负责股数计算与执行定价，不因执行口径而改写自己的语义输入

## 当前明确不做

1. 全量旧 `malf` 事件家族
2. `filter` 或 `alpha` 的私有判定逻辑
3. `alpha trigger candidate` 正式生产

## 一句话收口

`malf` 当前最小正式合同是：只消费官方 `market_base.stock_daily_adjusted(backward)`，稳定产出可被 `structure` 直接消费的上下文快照与结构候选快照，并把与执行层的价格边界分清。
