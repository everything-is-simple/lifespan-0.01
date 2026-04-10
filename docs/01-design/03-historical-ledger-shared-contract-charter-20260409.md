# 历史账本共享合同设计

日期：`2026-04-09`
状态：`生效`

## 背景

本仓所有正式数据库都不是“一次运行的结果文件”，而是长期历史账本。只要后续模块在自然键、增量策略、断点续跑与审计语义上各写各的，整套系统就会重新退化成难以续跑、难以复算、难以审计的一次性流水线。

## 设计目标

1. 冻结全仓正式账本的共享语义。
2. 统一“稳定自然键优先，`run_id` 只做审计”的口径。
3. 统一“先批量建仓，再增量更新”的生命周期。
4. 统一 checkpoint / dirty / replay / audit 的最低要求。

## 设计原则

### 1. 稳定实体锚点优先

- 正式账本先声明稳定实体锚点。
- 标的类实体默认采用 `asset_type + code`。
- `name` 是快照属性、兼容映射或审计辅助字段，不是唯一主锚。

### 2. 业务自然键叠加

- 在实体锚点之上，必须继续叠加业务自然键。
- 常见业务自然键包括：
  - `trade_date / asof_date / effective_date`
  - `window_start / window_end / holding_days`
  - `family / detector / scene / profile`
  - `state / phase / status`
  - `account / portfolio / leg`

### 3. 两阶段更新

每个正式账本都必须显式回答：

1. 一次性批量建仓怎么做
2. 每日或每批次增量更新怎么做

如果某层不是市场数据，也必须给出等价表达，例如：

- full bootstrap + incremental materialization
- historical backfill + dirty queue replay

### 4. 断点续跑与 replay 必须显式

每个正式实现都必须说明：

- checkpoint / cursor / progress 写在哪里
- 中断后如何续跑
- unchanged replay 如何避免重复写入或重复重算
- dirty queue 是否存在；若存在，如何挂账与消费

### 5. 审计账本与业务主键分层

- `run_id`、批次号、时间戳、source 摘要、status/action/summary 都属于审计层。
- 审计层必须保留，但不得替代业务自然键。

## 共享影响范围

本合同适用于以下正式数据库：

1. `raw_market`
2. `market_base`
3. `malf`
4. `structure`
5. `filter`
6. `alpha`
7. `position`
8. `portfolio_plan`
9. `trade_runtime`
10. `system`

## 与卡 21 的关系

卡 `21` 进一步把本合同升级为全系统硬门禁，要求后续正式卡必须显式声明：

- 实体锚点
- 业务自然键
- 批量建仓
- 增量更新
- 断点续跑
- 审计账本
