# data 模块指数与板块 raw/base 增量桥接章程

日期：`2026-04-10`
状态：`生效中`

## 问题

卡 `17-19` 已经把个股链路冻结成可正式运行的两段：

1. `TDX / TdxQuant -> raw_market`
2. `raw_market -> market_base`

但当前正式实现仍只覆盖个股：

1. `H:\tdx_offline_Data\stock` 已可一次性建仓并做每日断点续传
2. `H:\tdx_offline_Data\index` 尚未进入正式 `raw/base` 账本
3. `H:\tdx_offline_Data\block` 尚未进入正式 `raw/base` 账本

这会让后续任何指数语义、板块语义、行业轮动或宽基对照分析继续依赖临时目录直读，而不是正式历史账本。

## 设计输入

1. `docs/01-design/modules/data/01-tdx-offline-raw-and-market-base-bridge-charter-20260410.md`
2. `docs/01-design/modules/data/02-raw-base-strong-checkpoint-and-dirty-materialization-charter-20260410.md`
3. `docs/01-design/modules/data/03-daily-raw-base-fq-incremental-update-source-selection-charter-20260410.md`
4. `docs/01-design/modules/data/04-tdxquant-daily-raw-source-ledger-bridge-charter-20260410.md`
5. `docs/02-spec/modules/data/01-tdx-offline-raw-and-market-base-bridge-spec-20260410.md`
6. `docs/02-spec/modules/data/02-raw-base-strong-checkpoint-and-dirty-materialization-spec-20260410.md`
7. `docs/02-spec/modules/data/03-daily-raw-base-fq-incremental-update-source-selection-spec-20260410.md`
8. `docs/03-execution/19-tdxquant-daily-raw-source-ledger-bridge-conclusion-20260410.md`
9. 用户提供的正式离线目录：
   - `H:\tdx_offline_Data\index`
   - `H:\tdx_offline_Data\block`

## 裁决

### 裁决一：卡 `20` 只补齐 TDX 离线 `index/block` 的正式 raw/base 账本，不在本卡引入新来源

本卡只接入：

1. `H:\tdx_offline_Data\index\{Backward-Adjusted,Forward-Adjusted,Non-Adjusted}\*.txt`
2. `H:\tdx_offline_Data\block\{Backward-Adjusted,Forward-Adjusted,Non-Adjusted}\*.txt`

不在本卡新增：

1. 指数或板块的网络抓取
2. 指数或板块的 TdxQuant official 日更桥接
3. 分钟线、tick、成分股快照等新形态数据

### 裁决二：`index/block` 继承个股同一套两步走契约

两步走固定为：

1. 第一步：一次性批量建仓
   - `TDX txt -> raw_market.{index/block}_file_registry / {index/block}_daily_bar`
   - `raw_market -> market_base.{index/block}_daily_adjusted`
2. 第二步：每日断点续传、增量更新
   - 先走文件级跳过与变更文件重读
   - 再走 `base_dirty_instrument` 驱动的增量物化

### 裁决三：`index/block` 不复用个股表名，而是进入各自正式表族

本卡接受为正式表族：

1. `raw_market.index_file_registry`
2. `raw_market.index_daily_bar`
3. `raw_market.block_file_registry`
4. `raw_market.block_daily_bar`
5. `market_base.index_daily_adjusted`
6. `market_base.block_daily_adjusted`

拒绝把指数、板块继续塞进 `stock_*` 表名下，以免未来在语义、审计与下游消费上持续歧义。

### 裁决四：run/file ledger 与 dirty/base build ledger 升级为多 `asset_type` 契约

为了避免维护三套分叉实现，本卡要求以下共享账本显式支持 `asset_type in {'stock','index','block'}`：

1. `raw_ingest_run`
2. `raw_ingest_file`
3. `base_dirty_instrument`
4. `base_build_run`
5. `base_build_scope`
6. `base_build_action`

历史 stock 行仍保留兼容，但新写入口必须明确写入 `asset_type`。

### 裁决五：入口脚本升级为“通用 raw ingest + 多 asset_type base build”

本卡接受：

1. 新增正式 raw 入口 `scripts/data/run_tdx_asset_raw_ingest.py`
   - 以 `--asset-type {stock,index,block}` 驱动同一套文件级 ingest 契约
2. 保留 `scripts/data/run_tdx_stock_raw_ingest.py` 作为 stock 兼容入口
3. `scripts/data/run_market_base_build.py` 增加 `--asset-type`

### 裁决六：价格口径继续沿用既有治理，不因指数/板块而改写

`market_base` 仍正式保存三套价格：

1. `adjust_method = none`
2. `adjust_method = backward`
3. `adjust_method = forward`

默认消费口径不变：

1. `malf -> structure -> filter -> alpha` 默认 `backward`
2. `position -> trade` 默认 `none`
3. `forward` 继续只保留给研究与展示

## 预期产出

本卡最少应产出：

1. 可正式运行的 `index/block` raw ingest 契约与 CLI 入口
2. 可正式运行的 `index/block` market_base 物化契约
3. 一次性批量建仓的 bounded 证据
4. 二次 replay 命中 `skipped_unchanged` 与 dirty queue 增量联动证据

## 模块边界

### 范围内

1. `index/block` 的 `txt -> raw_market -> market_base`
2. 多 `asset_type` 的 raw/base 共享账本升级
3. 对应脚本入口、测试与执行文档

### 范围外

1. 指数/板块 TdxQuant official 桥接
2. 板块成分、行业映射、主题分类等关系型账本
3. 下游模块把 `index/block` 直接作为正式输入

## 一句话收口

卡 `20` 的任务是把 `H:\tdx_offline_Data\index` 与 `H:\tdx_offline_Data\block` 正式接入和个股同等治理强度的 `raw_market / market_base` 历史账本，并冻结"一次性批量建仓 + 每日断点续传增量更新"的多 `asset_type` 契约。

## 流程图

```mermaid
flowchart LR
    IDX[tdx_offline index] --> RAW_I[raw_market index_daily_bar]
    BLK[tdx_offline block] --> RAW_B[raw_market block_daily_bar]
    RAW_I --> BASE_I[market_base index_daily_adjusted]
    RAW_B --> BASE_B[market_base block_daily_adjusted]
