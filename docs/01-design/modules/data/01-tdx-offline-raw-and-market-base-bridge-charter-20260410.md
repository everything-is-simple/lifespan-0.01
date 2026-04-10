# data 模块 TDX 离线行情进入 raw_market / market_base 章程

日期：`2026-04-10`
状态：`生效中`

## 问题

当前新仓虽然已经冻结了 `raw_market -> market_base` 的两级原则，但正式库实物并没有把本地 TDX 离线日线资产接进来：

1. `raw_market.duckdb` 尚不存在。
2. `market_base.stock_daily_adjusted` 只有样例级数据，不足以支撑正式主线。
3. 下游已经在跑 `structure -> filter -> alpha -> position -> portfolio_plan -> trade`，但上游 `data -> malf` 还没有官方入口。

如果继续往 `trade / system` 走，只会把主线建立在空上游之上。

## 设计输入

1. `docs/01-design/00-system-charter-20260409.md`
2. `docs/01-design/03-historical-ledger-shared-contract-charter-20260409.md`
3. `docs/01-design/modules/data/00-data-module-lessons-20260409.md`
4. `docs/02-spec/03-historical-ledger-shared-contract-spec-20260409.md`
5. 用户提供的 `H:\tdx_offline_Data`

## 裁决

### 裁决一：`data` 正式主线先固定为本地 TDX 离线日线

本轮 `data` 的正式上游固定为用户本机长期维护的离线目录：

`H:\tdx_offline_Data`

当前只把它当作官方主线入口，不再为了接主线额外引入网络抓取依赖。

### 裁决二：坚持 `raw_market -> market_base` 两级，不允许跳层

本轮必须同时建立：

1. `raw_market`
   - 记录文件级镜像、来源路径、文件更新时间与原始日线字段。
2. `market_base`
   - 从官方 `raw_market` 物化下游稳定消费的正式行情事实。

不允许把“直接从离线文本现读现算”的临时逻辑当成长期官方入口。

### 裁决三：自然键显式纳入 `code + name`

本轮数据层的实体锚点固定为：

1. 标的主语至少包含 `code + name`
2. 日线事实至少包含 `code + name + trade_date + adjust_method`
3. 文件级更新记忆至少包含 `code + name + source_file_mtime`

`run_id` 只承担审计职责，不能替代业务自然键。

### 裁决四：`market_base` 必须同时沉淀三套价格

本轮 `market_base.stock_daily_adjusted` 必须同时保存：

1. `adjust_method = none`
2. `adjust_method = backward`
3. `adjust_method = forward`

它们是同一历史账本中的三种正式价格视图，不允许只留一套后再让下游各自猜测。

### 裁决五：信号口径与执行口径分离冻结

当前正式口径固定为：

1. `malf -> structure -> filter -> alpha` 默认消费 `adjust_method = backward`
   - 目的：让结构语义、趋势语义、触发语义建立在可复算且跨除权连续的价格序列上。
2. `position -> trade` 默认消费 `adjust_method = none`
   - 目的：让参考成交价、计划成交价和股数计算回到真实未复权价格，避免复权价导致的股数失真。
3. `adjust_method = forward` 当前只作为研究与展示侧保留，不作为正式执行定价口径。

### 裁决六：增量更新以“文件级跳过 + 行级 upsert”为最小正式实现

TDX 离线文本以单文件承载单标的全量日线历史，因此本轮增量策略固定为：

1. 先按 `source_path / size / mtime` 判断文件是否变化。
2. 未变化文件直接跳过，不重复解析。
3. 已变化文件重新解析，并按自然键 upsert 日线事实。
4. 断点续跑时允许只重做变化文件，不要求全库重建。

## 模块边界

### 范围内

1. TDX 离线股票日线进入 `raw_market`
2. `raw_market -> market_base.stock_daily_adjusted`
3. `none / backward / forward` 三套价格正式落账
4. 对 `malf` 提供稳定可消费的股票日线合约

### 范围外

1. 网络行情拉取
2. 分钟级或 tick 级数据
3. 指数与板块的正式 `market_base` 下游表
4. 复权因子来源追溯还原

## 一句话收口

`data` 当前最重要的不是继续补治理债务，而是把本机 TDX 离线股票日线正式接入 `raw_market / market_base`，并把 `none / backward / forward` 三套价格一次性沉淀好，为 `malf` 和执行层分别提供正确上游。
