# data 模块指数与板块 raw/base 增量桥接规格

日期：`2026-04-10`
状态：`生效中`

## 适用范围

本规格冻结卡 `20` 的最小正式实现范围，覆盖：

1. `TDX txt(index/block) -> raw_market`
2. `raw_market(index/block) -> market_base`
3. 一次性批量建仓
4. 每日断点续传、增量更新、dirty queue 联动

## 正式来源

当前正式来源固定为：

1. `H:\tdx_offline_Data\index\{Backward-Adjusted,Forward-Adjusted,Non-Adjusted}\*.txt`
2. `H:\tdx_offline_Data\block\{Backward-Adjusted,Forward-Adjusted,Non-Adjusted}\*.txt`

编码规则继续按 `gbk` 解析，文件头和行格式与个股 TDX txt 一致。

## 正式 raw 表族

### 指数

1. `raw_market.index_file_registry`
2. `raw_market.index_daily_bar`

### 板块

1. `raw_market.block_file_registry`
2. `raw_market.block_daily_bar`

### 共享 raw 审计账本

以下共享表必须显式带 `asset_type`：

1. `raw_market.raw_ingest_run`
2. `raw_market.raw_ingest_file`

其中：

1. `asset_type` 取值固定为 `stock | index | block`
2. 历史 stock 旧行允许为空；查询兼容时按 `COALESCE(asset_type, 'stock')`

## 正式 market_base 表族

### 指数

1. `market_base.index_daily_adjusted`

### 板块

1. `market_base.block_daily_adjusted`

### 共享 base 审计账本

以下共享表必须显式带 `asset_type`：

1. `market_base.base_dirty_instrument`
2. `market_base.base_build_run`
3. `market_base.base_build_scope`
4. `market_base.base_build_action`

## Python 入口

### raw ingest

1. `run_tdx_stock_raw_ingest(...)`
2. `run_tdx_index_raw_ingest(...)`
3. `run_tdx_block_raw_ingest(...)`

三者必须共享同一套文件级增量契约：

1. 文件指纹优先 `size + mtime`
2. 必要时升级为 `content_hash`
3. 支持 `continue_from_last_run`
4. 对变更文件做行级 `inserted / reused / rematerialized`

### market_base build

1. `run_market_base_build(..., asset_type='stock' | 'index' | 'block')`

该入口必须：

1. 从对应 raw 表读取
2. 物化到对应 market_base 表
3. 支持 `full / incremental`
4. 支持按 `asset_type` 消费 dirty queue

## CLI 入口

1. `scripts/data/run_tdx_asset_raw_ingest.py`
   - 必须接受 `--asset-type`
2. `scripts/data/run_tdx_stock_raw_ingest.py`
   - 保留 stock 兼容入口
3. `scripts/data/run_market_base_build.py`
   - 必须接受 `--asset-type`

## 自然键与唯一性

### raw

1. `file_nk = asset_type + adjust_method + code + name + source_path`
2. `bar_nk = code + trade_date + adjust_method`

说明：

1. `bar_nk` 不强制带 `asset_type`，因为指数、板块、个股已进入各自独立表
2. `file_nk` 继续带 `asset_type`，便于跨表审计与共享账本串联

### market_base

1. `daily_bar_nk = code + trade_date + adjust_method`
2. `dirty_nk = asset_type + code + adjust_method`

说明：

1. `dirty_nk` 必须升级带 `asset_type`，否则 `stock/index/block` 同代码会冲突

## 增量规则

### 第一步：一次性批量建仓

对 `index` 与 `block`：

1. `run_tdx_asset_raw_ingest --asset-type <type> --run-mode full --limit 0`
2. `run_market_base_build --asset-type <type> --build-mode full --limit 0`

### 第二步：每日断点续传

对 `index` 与 `block`：

1. `run_tdx_asset_raw_ingest --asset-type <type> --run-mode incremental`
2. 若 raw 有新增或变更，必须标记对应 `base_dirty_instrument`
3. `run_market_base_build --asset-type <type> --build-mode incremental`

### dirty queue 规则

1. dirty queue 必须按 `asset_type + adjust_method + code` 精确消费
2. `dirty_queue` 模式下不得再被全局 `limit` 截断完整历史窗口

## bounded 实现最低要求

要进入正式收口，至少需要：

1. unit test 覆盖 `index/block` 的 raw ingest 与 base build
2. 真实 bounded 初始化 evidence
3. 至少一轮 replay 命中 `skipped_unchanged`
4. 至少一轮 dirty queue 增量物化 evidence

## 当前明确不做

1. `index/block` 的 TdxQuant official raw 源桥接
2. 板块成分、行业映射、主题分类等关系型账本
3. 下游模块把 `index/block` 直接作为正式输入

## 一句话收口

卡 `20` 的正式规格是：让 `index/block` 以与 `stock` 相同的 run/file ledger、dirty queue、断点续跑和增量物化规则进入独立的 `raw_market / market_base` 表族，并把共享审计账本升级为显式多 `asset_type` 契约。
