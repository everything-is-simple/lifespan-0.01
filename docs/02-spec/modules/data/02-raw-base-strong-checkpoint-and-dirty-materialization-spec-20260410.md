# data 模块 raw/base 强断点与脏标的物化增强规格

日期：`2026-04-10`
状态：`生效中`

## 适用范围

本规格冻结 `data` 模块增强后的正式合同，覆盖：

1. `raw_ingest_run`
2. `raw_ingest_file`
3. `base_build_run`
4. `base_build_scope`
5. `base_build_action`
6. `base_dirty_instrument`
7. `run_tdx_stock_raw_ingest(...)` 增强版
8. `run_market_base_build(...)` 增强版

## raw 增强合同

### 1. `raw_ingest_run`

用途：

1. 记录一次官方 `raw` ingest run
2. 提供 run 级审计、续跑与异常定位锚点

最小字段：

1. `run_id`
2. `runner_name`
3. `runner_version`
4. `adjust_method`
5. `run_mode`
6. `source_root`
7. `candidate_file_count`
8. `processed_file_count`
9. `skipped_file_count`
10. `inserted_bar_count`
11. `reused_bar_count`
12. `rematerialized_bar_count`
13. `run_status`
14. `started_at`
15. `completed_at`
16. `summary_json`

### 2. `raw_ingest_file`

用途：

1. 记录每个文件在某次 run 中的动作与结果

最小字段：

1. `run_id`
2. `file_nk`
3. `code`
4. `name`
5. `adjust_method`
6. `source_path`
7. `fingerprint_mode`
8. `action`
9. `row_count`
10. `error_message`
11. `recorded_at`

动作枚举最小固定为：

1. `skipped_unchanged`
2. `inserted`
3. `reused`
4. `rematerialized`
5. `failed`

### 3. 文件指纹规则

正式策略：

1. 默认比较 `size + mtime`
2. 如果 `mtime` 变化但 `size` 不变，允许进入 `content_hash` 二次确认
3. 允许通过 runner 参数显式开启 `force_hash`

## base 增强合同

### 1. `base_dirty_instrument`

用途：

1. 记录需要重新物化到 `market_base` 的脏标的

最小字段：

1. `dirty_nk`
2. `code`
3. `adjust_method`
4. `dirty_reason`
5. `source_run_id`
6. `source_file_nk`
7. `dirty_status`
8. `first_marked_at`
9. `last_marked_at`
10. `last_consumed_run_id`

`dirty_status` 最小枚举：

1. `pending`
2. `consumed`
3. `failed`

### 2. `base_build_run`

用途：

1. 记录一次官方 `base` 物化 run

最小字段：

1. `run_id`
2. `runner_name`
3. `runner_version`
4. `adjust_method`
5. `build_mode`
6. `source_scope_kind`
7. `source_row_count`
8. `inserted_count`
9. `reused_count`
10. `rematerialized_count`
11. `consumed_dirty_count`
12. `run_status`
13. `started_at`
14. `completed_at`
15. `summary_json`

### 3. `base_build_scope`

用途：

1. 记录本次 build 的作用范围

最小字段：

1. `run_id`
2. `scope_type`
3. `scope_value`
4. `recorded_at`

`scope_type` 最小枚举：

1. `full`
2. `instrument`
3. `date_range`
4. `dirty_queue`

### 4. `base_build_action`

用途：

1. 记录每个 `(code, adjust_method)` 在某次 run 中的物化动作

最小字段：

1. `run_id`
2. `code`
3. `adjust_method`
4. `action`
5. `row_count`
6. `recorded_at`

动作枚举最小固定为：

1. `inserted`
2. `reused`
3. `rematerialized`
4. `failed`

## runner 模式

### `run_tdx_stock_raw_ingest(...)`

最小新增参数：

1. `run_mode`
2. `force_hash`
3. `continue_from_last_run`

### `run_market_base_build(...)`

最小新增参数：

1. `build_mode`
2. `consume_dirty_only`
3. `mark_clean_on_success`

### build_mode 规则

1. `full`
   - 忽略 dirty queue，直接按给定范围物化
2. `incremental`
   - 默认只消费 `base_dirty_instrument.pending`

## 约束增强

后续正式结构至少应补齐：

1. `stock_file_registry.file_nk` 唯一
2. `stock_daily_bar.bar_nk` 唯一
3. `stock_daily_adjusted(code, trade_date, adjust_method)` 唯一
4. `base_dirty_instrument.dirty_nk` 唯一
5. 关键业务列补 `NOT NULL`

## 当前明确不做

1. `malf` 输入合同改写
2. 复权因子账本
3. corporate action 账本
4. 分钟级或 tick 级 dirty queue

## 一句话收口

增强后的 `data` 正式合同应把 `raw` 提升为文件级运行账本，把 `base` 提升为脏标的增量账本，让全量建库和日常增量都具备显式 run 审计、强断点与可续跑能力。
