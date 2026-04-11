# malf semantic canonical contract freeze 规格

适用执行卡：`29-malf-semantic-canonical-contract-freeze-card-20260411.md`

## 1. 输入与边界

### 1.1 唯一输入

`malf` 只允许消费本级别 `price bar` 序列。

正式价格输入来源：

- 股票：`market_base.stock_daily_adjusted`
- 研究口径：`adjust_method='backward'`

### 1.2 禁止输入

`malf` 正式核心禁止直接依赖：

- `MA20 / MA60 / ret20`
- 收益率标签
- 高级别 `context`
- 交易动作 / 执行接口 / 仓位建议

## 2. 正式账本

### 2.1 pivot_ledger

自然键：

- `asset_type + code + timeframe + pivot_bar_dt + pivot_type`

核心字段：

- `pivot_type` in `{H, L}`
- `pivot_bar_dt`
- `confirmed_at`
- `pivot_price`
- `prior_pivot_nk`

约束：

- `pivot_bar_dt` 是结构发生时间
- `confirmed_at` 是结构可见时间
- 任何下游使用 `pivot` 时，只允许使用 `confirmed_at <= asof_bar_dt` 的 pivot

### 2.2 wave_ledger

自然键：

- `asset_type + code + timeframe + wave_id`

核心字段：

- `direction` in `{up, down}`
- `major_state` in `{牛顺, 牛逆, 熊顺, 熊逆}`
- `start_bar_dt`
- `end_bar_dt`
- `active_flag`

约束：

- 每个 `wave` 只属于一个时间级别
- 极值推进计数必须绑定 `wave_id`

### 2.3 extreme_progress_ledger

自然键：

- `asset_type + code + timeframe + wave_id + extreme_seq`

核心字段：

- `extreme_type` in `{HH, LL}`
- `record_bar_dt`
- `record_price`
- `cumulative_count`
- `break_base_extreme_nk`

约束：

- `HH` 与 `LL` 只在当前 `wave` 内累计
- 不允许跨 `wave` 继承 `count`

### 2.4 state_snapshot

自然键：

- `asset_type + code + timeframe + asof_bar_dt`

核心字段：

- `major_state`
- `trend_direction`
- `last_confirmed_h`
- `last_confirmed_l`
- `last_valid_hl`
- `last_valid_lh`
- `current_hh_count`
- `current_ll_count`
- `wave_id`

裁决：

- `push_count`
- `pullback_count`
- `break_trigger`

不作为正式必需字段冻结；如后续保留，也只能是派生字段，不能参与主状态机。

### 2.5 same_level_stats

自然键：

- `universe + timeframe + major_state + metric_name + sample_version`

核心字段：

- `sample_size`
- `p10 / p25 / p50 / p75 / p90`
- `mean / std`

约束：

- 只统计本级别、已完成 `wave`
- 最小样本单位是 `wave`
- 禁止跨级别混样本

## 3. 状态机合同

### 3.1 结构职责

- `HH / LL`：推进
- `HL / LH`：守成
- `break`：失效与触发

### 3.2 状态定义

- `牛顺`：`HH` 持续出现，最后有效 `HL` 未破
- `熊顺`：`LL` 持续出现，最后有效 `LH` 未上破
- `牛逆`：多头旧结构失效后的向下过渡态
- `熊逆`：空头旧结构失效后的向上过渡态

### 3.3 转移定义

- `牛顺 -> 牛逆`：`break_last_HL`
- `熊顺 -> 熊逆`：`break_last_LH`
- `牛逆 -> 牛顺`：先 `break_last_LH`，再由新的 `HH` 推进确认
- `熊逆 -> 熊顺`：先 `break_last_HL`，再由新的 `LL` 推进确认

### 3.4 转折三阶段

这是结构语义，不是交易语义：

- `trigger`：发生 `break`
- `hold`：新方向未被立即打回
- `expand`：新方向已出现新的 `HH` 或 `LL`

裁决：

- `break` 不等于新趋势成立
- 只有 `expand` 才代表新方向被推进确认

## 4. 时间级别规则

月 / 周 / 日各自独立运行一套完整结构：

- 独立 pivot
- 独立 wave
- 独立 state
- 独立 stats

禁止：

- 用月线决定周线 `state`
- 用周线决定日线 `state`
- 共享不同级别的 `count`

如需高周期背景，只能在 `filter / alpha` 层以只读方式读取。

## 5. 排除项

以下内容不属于 `malf canonical v2`：

- `malf_context_4`
- `new_high_count / new_low_count`
- `execution_interface`
- `allowed_actions`
- `confidence`
- 仓位与执行建议

## 6. 对旧实现的裁决

旧 `bridge-v1` 表：

- `pas_context_snapshot`
- `structure_candidate_snapshot`

只允许作为兼容产物存在，不能再代表正式 `malf` 真值。

旧 `mechanism sidecar`：

- `pivot_confirmed_break_ledger`
- `same_timeframe_stats_*`

只允许作为历史兼容件保留，不能替代 canonical `malf core`。
