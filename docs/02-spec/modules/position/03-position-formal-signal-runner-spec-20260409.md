# position formal signal runner 与 reference price enrichment 规格

日期：`2026-04-09`
状态：`生效中`

## 目标

本页只冻结一件事：

`position` 如何从官方 `alpha formal signal` 做 bounded 读取，并用 `market_base` 补齐 `reference_trade_date / reference_price` 后，复用现有 `materialize_position_from_formal_signals(...)` 正式落账。

本页不负责：

1. 定义 `alpha` 全量正式表族
2. 定义 `trade / system` 如何消费 `position`
3. 把 `position` 扩成全市场 full replay runner

## 正式输入

### `alpha` 输入层

当前最小正式入口固定为：

1. 默认表名：`alpha_formal_signal_event`
2. 允许通过 runner 参数改成兼容表名
3. 必须至少能解析出桥接合同里冻结的最小字段组

当前允许两类列名口径：

1. 新口径
   - `signal_nk`
   - `instrument`
   - `pattern_code`
   - `formal_signal_status`
   - `trigger_admissible`
   - `source_trigger_event_nk`
   - `signal_contract_version`
2. 兼容旧口径
   - `signal_id -> signal_nk`
   - `code -> instrument`
   - `pattern -> pattern_code`
   - `admission_status -> formal_signal_status`
   - `filter_trigger_admissible -> trigger_admissible`
   - `source_pas_signal_id -> source_trigger_event_nk`
   - `source_pas_contract_version -> signal_contract_version`

### `market_base` 输入层

当前参考价 enrichment 固定读取：

1. 表名：`stock_daily_adjusted`
2. 价格列：`close`
3. 默认复权口径：`adjust_method = 'backward'`

## enrichment 规则

### `reference_trade_date`

`position` 不直接信任上游给出的参考成交日。
当前 `v1` 固定按下面规则补齐：

1. 优先取 `instrument + signal_date` 之后的第一个交易日
2. 默认条件为 `trade_date > signal_date`
3. 若开启 same-day fallback，则允许退回 `trade_date >= signal_date`

### `reference_price`

当前 `v1` 固定取 `reference_trade_date` 对应 `stock_daily_adjusted.close`。

## bounded runner 合同

`run_position_formal_signal_materialization(...)` 至少必须支持：

1. `policy_id`
2. `capital_base_value`
3. `signal_start_date`
4. `signal_end_date`
5. `instruments`
6. `limit`
7. `alpha_formal_signal_table`
8. `market_price_table`
9. `adjust_method`
10. `allow_same_day_price_fallback`
11. `summary_path`

默认行为：

1. 默认读取正式 `alpha.duckdb`
2. 默认读取正式 `market_base.duckdb`
3. 默认写入正式 `position.duckdb`
4. 只复用现有 `materialize_position_from_formal_signals(...)`
5. 不允许另起第二套 candidate / sizing / family snapshot 落表逻辑

## 缺价处理规则

若 `alpha formal signal` 找不到可用参考价，当前 `v1` 固定：

1. 该条信号不进入 `materialize_position_from_formal_signals(...)`
2. 在 runner summary 中计入 `missing_reference_price_count`
3. 不为这条缺价信号伪造 `reference_trade_date / reference_price`

## 正式输出

本页冻结的正式产物为：

1. `src/mlq/position/runner.py`
2. `scripts/position/run_position_formal_signal_materialization.py`
3. `PositionFormalSignalRunnerSummary`
4. bounded validation evidence

## 一句话收口

09 的正式合同是：`position` 只从官方 `alpha formal signal` 读 bounded 样本，用 `market_base` 补齐下一交易日参考价，再复用既有 materialization helper 落正式账本。
