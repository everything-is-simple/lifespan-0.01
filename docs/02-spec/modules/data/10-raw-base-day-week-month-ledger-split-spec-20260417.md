# data 模块 raw/base 日周月分库迁移规格
`日期：2026-04-17`
`状态：草案`

## 适用范围

本规格覆盖 `76` 的最小正式实现：

1. `raw/base` 从单库多 timeframe 改为日周月分库
2. `week/month raw` 改为只读消费官方 `day raw`
3. `week/month base` 改为只读消费对应 `week/month raw`
4. 旧单库周月表在迁移校验通过后清除

## 官方数据库路径

### raw

1. `day raw`
   - `H:\Lifespan-data\raw\raw_market.duckdb`
2. `week raw`
   - `H:\Lifespan-data\raw\raw_market_week.duckdb`
3. `month raw`
   - `H:\Lifespan-data\raw\raw_market_month.duckdb`

### base

1. `day base`
   - `H:\Lifespan-data\base\market_base.duckdb`
2. `week base`
   - `H:\Lifespan-data\base\market_base_week.duckdb`
3. `month base`
   - `H:\Lifespan-data\base\market_base_month.duckdb`

## 官方表族放置规则

### 1. day raw

必须保留：

1. `stock_daily_bar / index_daily_bar / block_daily_bar`
2. `stock_file_registry / index_file_registry / block_file_registry`
3. `raw_ingest_run / raw_ingest_file`
4. `raw_tdxquant_*`
5. `tushare_objective_*`
6. `objective_profile_materialization_*`

禁止保留：

1. `stock_weekly_bar / stock_monthly_bar`
2. `index_weekly_bar / index_monthly_bar`
3. `block_weekly_bar / block_monthly_bar`

### 2. week raw

必须保留：

1. `stock_weekly_bar / index_weekly_bar / block_weekly_bar`
2. `raw_timeframe_build_run`
3. `raw_timeframe_build_scope`
4. `raw_timeframe_checkpoint`

禁止出现：

1. 任意 `*_daily_bar`
2. 任意 `*_monthly_bar`
3. `raw_tdxquant_*`
4. `tushare_objective_*`
5. `objective_profile_materialization_*`

### 3. month raw

必须保留：

1. `stock_monthly_bar / index_monthly_bar / block_monthly_bar`
2. `raw_timeframe_build_run`
3. `raw_timeframe_build_scope`
4. `raw_timeframe_checkpoint`

禁止出现：

1. 任意 `*_daily_bar`
2. 任意 `*_weekly_bar`
3. `raw_tdxquant_*`
4. `tushare_objective_*`
5. `objective_profile_materialization_*`

### 4. day base

必须保留：

1. `stock_daily_adjusted / index_daily_adjusted / block_daily_adjusted`
2. `base_dirty_instrument`
3. `base_build_run / base_build_scope / base_build_action`

禁止保留：

1. `stock_weekly_adjusted / stock_monthly_adjusted`
2. `index_weekly_adjusted / index_monthly_adjusted`
3. `block_weekly_adjusted / block_monthly_adjusted`

### 5. week/month base

`week base` 与 `month base` 各自只保留对应 timeframe 的 adjusted 表，以及对应的：

1. `base_dirty_instrument`
2. `base_build_run`
3. `base_build_scope`
4. `base_build_action`

## 业务主语义

### 实体锚点

1. day raw / base
   - `asset_type + code`
2. week/month raw / base
   - `asset_type + code`

### 业务自然键

1. price bar
   - `code + trade_date + adjust_method`
2. week/month raw checkpoint
   - `asset_type + timeframe + code + adjust_method`
3. week/month base dirty instrument
   - `asset_type + code + adjust_method`

## runner 契约

### 1. day raw

仍由：

1. `scripts/data/run_tdx_asset_raw_ingest.py`
2. `scripts/data/run_tdxquant_daily_raw_sync.py`
3. `scripts/data/run_tushare_objective_source_sync.py`
4. `scripts/data/run_tushare_objective_profile_materialization.py`

负责写入。

其中：

1. `run_tdx_asset_raw_ingest.py`
   - 正式收窄为 `timeframe='day'` 的 file-backed raw ingest
2. `run_tdxquant_daily_raw_sync.py`
   - 继续只负责 `stock daily none`
3. objective/profile 继续只落 `day raw`

### 2. week/month raw

必须新增正式 bounded runner，例如：

1. `scripts/data/run_raw_timeframe_build.py`

它的输入只能是：

1. 对应 `day raw` 官方库
2. `asset_type`
3. `timeframe in {week, month}`
4. `adjust_method`
5. 可选的 `instrument batch / checkpoint queue`

它不得回读：

1. `H:\tdx_offline_Data\*-day\*.txt`
2. 任意 `*-week / *-month` 目录

### 3. base

`scripts/data/run_market_base_build.py` 保留为正式 base runner，但必须按 timeframe 路由到对应物理库。

路由规则：

1. `timeframe='day'`
   - 读取 `raw_market.duckdb`
   - 写入 `market_base.duckdb`
2. `timeframe='week'`
   - 读取 `raw_market_week.duckdb`
   - 写入 `market_base_week.duckdb`
3. `timeframe='month'`
   - 读取 `raw_market_month.duckdb`
   - 写入 `market_base_month.duckdb`

## week/month 聚合规则

week/month raw 从 `day raw` 派生时，必须使用稳定聚合规则：

1. `week`
   - `to_period('W-FRI')`
2. `month`
   - `to_period('M')`
3. `trade_date`
   - 组内最后一个真实交易日
4. `open`
   - 首个真实交易日 `open`
5. `high`
   - 组内 `high` 最大值
6. `low`
   - 组内 `low` 最小值
7. `close`
   - 最后一个真实交易日 `close`
8. `volume / amount`
   - 组内求和

禁止把 `trade_date` 伪造为自然周末或自然月末的非交易日。

## 迁移顺序

1. 冻结旧单库周月路径
   - 停止继续运行旧 `75` 周月 txt 回补
2. 新增路径契约与 bootstrap
3. 先建 `raw_market_week.duckdb / raw_market_month.duckdb`
4. 再建 `market_base_week.duckdb / market_base_month.duckdb`
5. 做 parity 校验
   - `row_count`
   - `code_count`
   - `min/max trade_date`
   - spot-check code
6. 校验通过后，删除旧 `raw_market.duckdb / market_base.duckdb` 中的周月表与旧周月审计数据

## bounded 验证最低要求

1. 单测覆盖 week/month 从 `day raw` 派生，不再回读 `txt`
2. 单测覆盖六库路径路由
3. 单测覆盖 old day db purge 只删除周月表，不伤及 day 表和 objective/profile 表
4. 真实库验证：
   - `stock week raw/base` 全量落库
   - `stock month raw/base` 全量落库
   - day 库已不再包含周月 price 表

## 一句话收口

`76` 的正式规格是：保留现有 `raw_market.duckdb / market_base.duckdb` 作为 day 官方库，新建 week/month 官方库，并把 week/month price ledger 改造成只读消费 day 官方库的派生账本。
