# malf canonical ledger and data-grade runner bootstrap 规格

适用执行卡：`30-malf-canonical-ledger-and-data-grade-runner-bootstrap-card-20260411.md`

## 1. 正式表

### 1.1 malf_canonical_run

记录 canonical `malf` runner 的审计元数据：

- `run_id`
- `runner_name`
- `runner_version`
- `run_status`
- `signal_start_date / signal_end_date`
- `bounded_scope_count`
- `canonical_contract_version`
- `summary_json`

### 1.2 malf_canonical_work_queue

自然键：

- `asset_type + code + timeframe + dirty_reason`

核心字段：

- `scope_nk`
- `queue_status`
- `dirty_reason`
- `source_last_trade_date`
- `enqueued_at`
- `last_claimed_run_id`

### 1.3 malf_canonical_checkpoint

自然键：

- `asset_type + code + timeframe`

核心字段：

- `last_completed_bar_dt`
- `tail_start_bar_dt`
- `tail_confirm_until_dt`
- `last_wave_id`
- `last_run_id`

约束：

- checkpoint 必须记录未确认尾巴
- 增量续跑时必须从 `tail_start_bar_dt` 回放

### 1.4 malf_pivot_ledger

见 `29` 规格。

补充：

- runner 只允许写入 `confirmed_at <= 当前处理 bar` 的 pivot

### 1.5 malf_wave_ledger

补充字段：

- `start_pivot_nk`
- `end_pivot_nk`
- `hh_count`
- `ll_count`
- `completed_at`

### 1.6 malf_extreme_progress_ledger

补充字段：

- `major_state`
- `trend_direction`

### 1.7 malf_state_snapshot

每个 `asset_type + code + timeframe + asof_bar_dt` 最多一行。

允许附加：

- `reversal_stage` in `{none, trigger, hold, expand}`

但该字段仍然是结构语义，不是交易语义。

### 1.8 malf_same_level_stats

统计对象：

- `hh_count`
- `ll_count`
- `wave_duration_bars`
- `wave_range_ratio`

样本：

- 只允许使用已完成 `wave`

## 2. pivot 确认规则

本卡必须把 pivot 可见时间写死，防止未来函数。

最小正式规则：

1. pivot 发生在 `pivot_bar_dt`
2. pivot 只有在确认窗口结束后才写入 `pivot_ledger`
3. `confirmed_at` 不能早于确认窗口的最后一根 bar
4. 任何 state / wave / break 判断只允许消费 `confirmed_at <= asof_bar_dt` 的 pivot

确认窗口的具体 bar 数可以实现上参数化，但默认值必须在代码和脚本里固定，并进入 summary。

## 3. runner 行为

### 3.1 批量建仓

runner 对 `code + timeframe` scope 做全历史回放：

`bars -> pivots -> waves -> extremes -> snapshots -> stats`

### 3.2 每日增量

runner 先比较 source 最新 bar 日期与 checkpoint：

- 无新 bar：不入队
- 有新 bar：对应 `code + timeframe` 入 dirty queue

### 3.3 断点续跑

如果 scope 已入队但上次未完成：

- 下次 run 必须继续 claim 同一 scope
- 从 `tail_start_bar_dt` 重新回放
- 完成后更新 checkpoint 并将 queue 标记为完成

## 4. 时间级别处理

本卡 runner 必须支持：

- `D`
- `W`
- `M`

原则：

- `W / M` 由日线 bar 正式聚合得到
- 聚合后按各自级别独立运行状态机
- 不允许把日线结构直接抄到周/月

## 5. 与旧表的关系

旧表：

- `pas_context_snapshot`
- `structure_candidate_snapshot`
- `pivot_confirmed_break_ledger`
- `same_timeframe_stats_*`

在 `31` 之前可继续保留，但：

- 不得再由 canonical runner 回写
- 不得再被声明为正式 `malf` 真值

## 6. 验收要求

本卡至少要证明：

1. canonical `malf` 表成功落表
2. `code + timeframe` dirty queue / checkpoint / resume 成立
3. `D / W / M` 三个级别能各自独立产出结构结果
4. 旧 bridge-v1 与 canonical v2 在同库并存但不混真值
