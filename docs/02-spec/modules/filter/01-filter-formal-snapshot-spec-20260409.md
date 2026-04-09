# filter 正式 snapshot 规格

日期：`2026-04-09`
状态：`生效中`

## 适用范围

本规格冻结新仓 `filter` 模块的最小正式准入输出合同。当前只覆盖：

1. `filter_run`
2. `filter_snapshot`
3. `filter_run_snapshot`
4. `run_filter_snapshot_build(...)`
5. `scripts/filter/run_filter_snapshot_build.py`

本规格不代表全部 filter 规则已经完成，也不代表 `alpha` 已经在本卡内改完全部消费实现。

## 正式输入

`filter` 当前正式输入固定为：

1. 官方 `structure_snapshot`
2. 官方 `execution_context` / `malf` 上下文

至少保证以下字段可读：

1. `structure_snapshot_nk`
2. `instrument`
3. `signal_date`
4. `asof_date`
5. `malf_context_4`
6. `lifecycle_rank_high`
7. `lifecycle_rank_total`
8. `structure_progress_state`
9. `is_failed_extreme`
10. `failure_type`

硬约束：

1. `filter` 不负责 trigger detection。
2. `filter` 不负责 formal signal 物化。
3. `filter` 不负责 `position / trade` 风险门。

## 正式输出

### 1. `filter_run`

用途：

1. 记录一次 bounded filter 物化运行
2. 固定输入来源、窗口、版本与摘要

最小字段：

1. `run_id`
2. `runner_name`
3. `runner_version`
4. `run_status`
5. `signal_start_date`
6. `signal_end_date`
7. `bounded_instrument_count`
8. `source_structure_table`
9. `source_context_table`
10. `filter_contract_version`
11. `started_at`
12. `completed_at`
13. `summary_json`

### 2. `filter_snapshot`

用途：

1. 作为 `alpha` 的官方 pre-trigger 准入层
2. 回答在当前结构与上下文下，是否允许进入 trigger 检测

最小字段：

1. `filter_snapshot_nk`
2. `structure_snapshot_nk`
3. `instrument`
4. `signal_date`
5. `asof_date`
6. `trigger_admissible`
7. `primary_blocking_condition`
8. `blocking_conditions_json`
9. `admission_notes`
10. `source_context_nk`
11. `filter_contract_version`
12. `first_seen_run_id`
13. `last_materialized_run_id`

自然键规则：

`filter_snapshot_nk` 当前最小固定由下列字段拼出：

1. `structure_snapshot_nk`
2. `source_context_nk`
3. `filter_contract_version`

### 3. `filter_run_snapshot`

用途：

1. 桥接一次 `run` 与本次触达的 `filter_snapshot`
2. 支持 bounded readout、resume 与审计

最小字段：

1. `run_id`
2. `filter_snapshot_nk`
3. `materialization_action`
4. `trigger_admissible`
5. `primary_blocking_condition`
6. `recorded_at`

`materialization_action` 枚举：

1. `inserted`
2. `reused`
3. `rematerialized`

## 下游对齐规则

`alpha` 后续正式消费必须优先读取：

1. `filter_snapshot.trigger_admissible`
2. `filter_snapshot.primary_blocking_condition`
3. `filter_snapshot.blocking_conditions_json`

不再默认回读旧 `scene / phenomenon / pas_context` 兼容准入字段作为长期官方输入。

## Producer Runner 合同

### Python 入口

`run_filter_snapshot_build(...)`

### 脚本入口

`scripts/filter/run_filter_snapshot_build.py`

### 最小参数

1. `run_id`
2. `signal_start_date`
3. `signal_end_date`
4. `instrument` 或 bounded instrument 列表
5. `limit`
6. `batch_size`
7. `source_structure_table`
8. `source_context_table`
9. `summary_path`

## Bounded Evidence 要求

本卡后续正式实现至少要留下：

1. 单元测试
2. bounded smoke
3. `filter_run / filter_snapshot / filter_run_snapshot` readout
4. `alpha` 可消费的字段对接证据

## 当前明确不做

1. 全量 filter rule backfill
2. `alpha` detector 私有 skip reason 全量外提
3. `position / trade` 风险门合并到本层

## 一句话收口

`filter` 当前最小正式目标不是更复杂的规则树，而是一个可被 `alpha` 优先消费的独立 pre-trigger 准入快照层。`
