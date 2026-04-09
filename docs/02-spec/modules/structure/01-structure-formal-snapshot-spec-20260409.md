# structure 正式 snapshot 规格

日期：`2026-04-09`
状态：`生效中`

## 适用范围

本规格冻结新仓 `structure` 模块的最小正式输出合同。当前只覆盖：

1. `structure_run`
2. `structure_snapshot`
3. `structure_run_snapshot`
4. `run_structure_snapshot_build(...)`
5. `scripts/structure/run_structure_snapshot_build.py`

本规格不代表 `structure` 全部事件家族已经完成，也不代表 `filter / alpha` 已经完成正式对接实现。

## 正式输入

`structure` 当前正式输入固定为两类：

1. 官方 `malf / execution_context` 语义输入
   - 至少提供 `instrument / signal_date / asof_date / malf_context_4 / lifecycle_rank_high / lifecycle_rank_total`
2. 官方结构候选事实输入
   - 至少提供 `new_high_count / new_low_count / refresh_density / advancement_density / is_failed_extreme / failure_type`

硬约束：

1. 不允许 `structure` 直接承担 filter admission。
2. 不允许 `structure` 直接输出 formal signal。
3. 不允许为方便一次 bounded smoke 而把旧兼容字段继续当作长期官方输出。

## 正式输出

### 1. `structure_run`

用途：

1. 记录一次 `structure` bounded 物化运行
2. 固定来源、窗口、版本与摘要

最小字段：

1. `run_id`
2. `runner_name`
3. `runner_version`
4. `run_status`
5. `signal_start_date`
6. `signal_end_date`
7. `bounded_instrument_count`
8. `source_context_table`
9. `source_structure_input_table`
10. `structure_contract_version`
11. `started_at`
12. `completed_at`
13. `summary_json`

### 2. `structure_snapshot`

用途：

1. 作为 `filter / alpha` 的官方结构事实层
2. 回答当前这段中级波内部发生了什么结构推进或失败事实

最小字段：

1. `structure_snapshot_nk`
2. `instrument`
3. `signal_date`
4. `asof_date`
5. `malf_context_4`
6. `lifecycle_rank_high`
7. `lifecycle_rank_total`
8. `new_high_count`
9. `new_low_count`
10. `refresh_density`
11. `advancement_density`
12. `is_failed_extreme`
13. `failure_type`
14. `structure_progress_state`
15. `source_context_nk`
16. `structure_contract_version`
17. `first_seen_run_id`
18. `last_materialized_run_id`

`structure_progress_state` 当前最小枚举固定为：

1. `advancing`
2. `stalled`
3. `failed`
4. `unknown`

自然键规则：

`structure_snapshot_nk` 当前最小固定由下列字段拼出：

1. `instrument`
2. `signal_date`
3. `asof_date`
4. `source_context_nk`
5. `structure_contract_version`

### 3. `structure_run_snapshot`

用途：

1. 桥接一次 `run` 与本次触达的 `structure_snapshot`
2. 支持 bounded readout、resume 与审计

最小字段：

1. `run_id`
2. `structure_snapshot_nk`
3. `materialization_action`
4. `structure_progress_state`
5. `recorded_at`

`materialization_action` 枚举：

1. `inserted`
2. `reused`
3. `rematerialized`

## Producer Runner 合同

### Python 入口

`run_structure_snapshot_build(...)`

### 脚本入口

`scripts/structure/run_structure_snapshot_build.py`

### 最小参数

1. `run_id`
2. `signal_start_date`
3. `signal_end_date`
4. `instrument` 或 bounded instrument 列表
5. `limit`
6. `batch_size`
7. `source_context_table`
8. `source_structure_input_table`
9. `summary_path`

## Bounded Evidence 要求

本卡后续正式实现至少要留下：

1. 单元测试
2. bounded smoke
3. `structure_run / structure_snapshot / structure_run_snapshot` readout
4. `filter` 可消费的字段对接证据

## 当前明确不做

1. `structure` 全部 event 家族
2. `alpha` 五表族
3. `position / trade / system` 的直接结构消费

## 一句话收口

`structure` 当前最小正式目标不是更多兼容字段，而是一个可被 `filter / alpha` 稳定消费的官方 `snapshot` 事实层。`
