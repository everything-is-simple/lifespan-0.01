# 历史账本共享契约规格

日期：`2026-04-09`
状态：`生效中`

## 适用范围

本规格适用于新仓全部正式账本、正式表族、正式快照和正式审计表。

## 一、五根目录来源优先级

### 正式运行

正式运行时，五根目录来源优先级如下：

1. 显式传入的正式运行参数
2. 环境变量
3. `scripts/setup/enter_repo.ps1` 预设值

### 测试与开发兜底

当未提供正式环境来源时，允许使用 `repo_root.parent / Lifespan-*` 作为开发与测试兜底。
该行为不得被文档误写为正式环境默认值。

## 二、共享键契约

### 实体锚点

所有以证券、指数、行业、组合、账户为中心的正式表，必须先具备实体锚点。
默认实体锚点至少包括：

1. `entity_code`
2. `entity_name`

如果历史兼容需要，也可以在模块内命名为 `stock_code / stock_name`、`index_code / index_name`，但必须能映射回共享语义。

### 业务自然键

在实体锚点之上，正式表必须再叠加至少一种业务键：

1. 时间键：如 `trade_date / asof_date / effective_date`
2. 窗口键：如 `window_start / window_end / holding_days`
3. 策略键：如 `alpha_family / trigger_family / sizing_method`
4. 场景键：如 `scene_id / regime_id / filter_profile`
5. 状态键：如 `position_state / order_state / execution_state`

禁止只用 `run_id` 或自增整数充当正式主语义。

## 三、审计字段契约

所有正式表应预留或等价表达以下审计字段：

1. `build_batch_id` 或等价字段
2. `run_id` 或等价字段
3. `written_at`
4. `source_provider`
5. `source_version` 或 `source_digest`

这些字段属于审计层，不得取代业务自然键。

## 四、写入语义契约

### 追加写入表

以下表族默认只允许追加：

1. `ledger`
2. `fact`
3. `event`
4. `journal`
5. `manifest`

追加写入表需要支持断点续跑、分批处理和增量补写。

### 可覆盖快照表

以下表族允许按自然键重算覆盖：

1. `snapshot`
2. `latest_state`
3. `surface`
4. `materialized_view`

但它们必须满足：

1. 可从正式账本或事实层重新生成
2. 携带最近一次构建批次信息
3. 不得成为唯一事实来源

## 五、增量更新契约

正式模块在设计增量更新时，至少要回答：

1. 以什么自然键判断“已存在”
2. 以什么时间键判断“需要补写”
3. 分批粒度是什么
4. checkpoint 写在哪里
5. 中断后如何从上次位置续跑

如果回答不清楚，就不能进入正式 runner。

## 六、物理账本命名契约

当前正式账本物理名冻结为：

1. `raw_market.duckdb`
2. `market_base.duckdb`
3. `malf.duckdb`
4. `structure.duckdb`
5. `filter.duckdb`
6. `alpha.duckdb`
7. `position.duckdb`
8. `portfolio_plan.duckdb`
9. `trade_runtime.duckdb`
10. `system.duckdb`

其中文档边界名与物理名的特殊映射是：

1. 模块名 `trade`
2. 物理账本名 `trade_runtime.duckdb`

## 七、后续模块门槛

`position / alpha / portfolio_plan / trade` 在新增正式表前，必须先声明：

1. 表属于 `ledger` 还是 `snapshot`
2. 自然键是什么
3. 审计字段是什么
4. 写入语义是追加还是覆盖
5. 增量更新和断点续跑如何实现
