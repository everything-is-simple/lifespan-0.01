# malf 模块纯语义走势账本设计宪章

日期：`2026-04-11`
状态：`生效中`

> 角色声明：本文是当前唯一应被当作 `malf core` 读取的正式设计口径。
> 它定义纯语义走势账本本身，不负责说明 bridge v1 的现行兼容输出，也不承担 legacy lessons 的经验归档。
> bridge v1 请读 `01-market-base-to-malf-minimal-snapshot-bridge-charter-20260410.md`；
> legacy lessons 请读 `00-malf-module-lessons-20260409.md`。
> 若需要读取机制层 `pivot-confirmed break` 与 `same-timeframe stats sidecar`，请读 `04-malf-mechanism-layer-break-confirmation-and-same-timeframe-stats-sidecar-charter-20260411.md`。

## 问题

当前 `malf` 在最新一轮扩展设计中同时吸收了：

1. 结构账本
2. 同级别统计
3. 执行动作接口
4. 高周期背景约束

这会把“市场发生了什么”与“该怎么解释、怎么行动”混成一层，带来三个正式问题：

1. `malf` 状态会被高周期背景或下游解释层污染，不再只由本级别价格结构决定。
2. `break` 会被误写成“新趋势已经成立”，而不是“旧结构先失效”。
3. `structure / filter / alpha` 的边界会重新纠缠，后续任何策略调整都可能倒灌回 `malf` 核心定义。

## 设计输入

1. `docs/01-design/modules/malf/00-malf-module-lessons-20260409.md`
2. `docs/01-design/modules/malf/01-market-base-to-malf-minimal-snapshot-bridge-charter-20260410.md`
3. `docs/01-design/modules/malf/02-malf-multi-timeframe-extreme-progression-ledger-charter-20260411.md`
4. `docs/01-design/modules/structure/01-structure-formal-snapshot-charter-20260409.md`
5. `docs/01-design/modules/filter/01-filter-formal-snapshot-charter-20260409.md`
6. `docs/03-execution/16-data-malf-minimal-official-mainline-bridge-conclusion-20260410.md`

## 裁决

### 裁决一：`malf` 的正式核心收缩为按时间级别独立运行的走势记账系统

`malf` 只记录本级别价格如何形成极值、结构如何被守护、何时被破坏、趋势如何继续推进。

它不再把下列内容作为自己的正式身份：

1. 动作接口
2. 仓位建议
3. 高周期背景
4. 直接交易解释

一句话冻结：

`malf` 是按时间级别独立运行的走势记账系统，而不是市场标签系统，也不是交易动作系统。

### 裁决零：当前公理层正式收口

当前 `malf` 公理层正式收口为：

1. `malf` 是按时间级别独立运行的纯语义走势账本系统。
2. 它只依赖本级别 `price bar`。
3. 正式原语只有 `HH / HL / LL / LH / break / count`。
4. `HH / LL` 负责推进，`HL / LH` 负责守成，`break` 负责旧结构失效。
5. `hh_count / ll_count` 只在当前 `wave_id` 内有效，不跨波段、不跨级别。
6. 每个 `timeframe` 独立闭环，其他级别不得参与其状态与统计计算。
7. `牛逆 / 熊逆` 不是背景标签，而是旧顺结构失效后、到新顺结构确认前的本级别过渡状态。
8. `break` 是触发，不是确认；新的同向极值推进出现，才确认新的顺状态。
9. 统计若存在，只能作为同级别 sidecar。
10. 动作、仓位、回测、概率、收益都属于下游，不属于 `malf core`。

### 裁决二：`malf` 的唯一输入回到本级别 price bar

纯语义 `malf` 的唯一输入是本级别 `price bar` 序列。

当前正式实现仍要求从官方 `market_base.stock_daily_adjusted(adjust_method='backward')` 起跑，但这是现阶段 runner 边界，不是把高周期状态或下游动作重新带回 `malf` 核心的理由。

### 裁决三：纯语义 `malf` 的唯一原语冻结为六个

正式原语固定为：

1. `HH`
2. `HL`
3. `LL`
4. `LH`
5. `break`
6. `count`

`malf` 不再把：

1. 均线
2. 收益率标签
3. 背景标签
4. 动作家族

视为自己的主语义原语。

### 裁决四：状态定义只允许由本级别结构排列裁决

`malf` 只保留四类状态：

1. `牛顺`
2. `牛逆`
3. `熊顺`
4. `熊逆`

其裁决原则固定为：

1. `顺状态 = 同向推进仍在延续`
2. `逆状态 = 原顺状态已失效，但新顺状态尚未完成确认`

高周期背景不得参与 `state` 计算，也不得写成 `牛逆 / 熊逆` 的定义前提。

更硬的正式表达：

1. `牛逆` 不是背景标签，而是旧上行顺结构失效后、到新顺结构确认前的本级别过渡状态。
2. `熊逆` 不是背景标签，而是旧下行顺结构失效后、到新顺结构确认前的本级别过渡状态。

### 裁决五：`break` 只表示旧结构失效，不等于新结构成立

正式口径必须钉死：

1. `break(HL)` 或 `break(LH)` 先回答“旧结构失效”
2. 新方向是否成立，必须再看后续是否形成新的同向极值推进

因此：

1. `break` 是触发，不是确认
2. `周/月更可靠` 不等于 `周/月绝不会假`

### 裁决六：推进计数只属于当前波段内部

`hh_count / ll_count` 的语义冻结为：

1. 只记录当前波段内部同方向新极值的累计次数
2. 必须绑定当前 `wave`
3. 不跨波段继承
4. 不跨时间级别混算

### 裁决七：月周日各自闭环，高周期 `context` 不属于 `malf` 本体

月、周、日各自是一套独立世界。

正式规则只保留：

1. 每个 `timeframe` 独立计算自己的结构
2. 不允许跨级别参与状态计算
3. 不允许跨级别共享极值、状态或样本

如果以后策略层要读取“月牛顺 + 周牛逆 + 日熊顺”，那属于下游消费视图，不属于 `malf` 的结构真相。

### 裁决八：同级别统计可以保留为 sidecar，但不再属于 `malf` 纯语义核心

若后续需要恢复同级别统计，它必须：

1. 只基于同一 `timeframe` 的 `wave / pivot / state` 派生
2. 不得跨级别混样本
3. 不得反向参与 `state` 计算

统计层在本轮降级为可选 sidecar，而不是 `malf` 纯语义核心的一部分。

正式边界补充：

1. 统计不是 `malf core`
2. 统计不能反向解释结构
3. 统计只能在下游消费层提供位置感与分布信息

### 裁决九：bridge v1 继续保留，但正式身份降级为兼容层

当前仓内既有：

1. `pas_context_snapshot`
2. `structure_candidate_snapshot`

它们继续作为现有 `scripts/malf/run_malf_snapshot_build.py` 的兼容输出保留，但不再代表 `malf` 的终局定义。

纯语义核心与当前实现之间的关系固定为：

`pure semantic core -> bridge v1 compatibility views -> current structure runner`

## 模块边界

### 范围内

1. 本级别 `pivot / wave / state / extreme_progress`
2. `HH / HL / LL / LH / break / count` 的纯语义定义
3. 结构失效与新推进确认的正式边界
4. 与 bridge v1 的兼容映射

### 范围外

1. `execution_interface`
2. `allowed_actions / confidence / invalidation_price`
3. 高周期 `context` 参与状态机
4. `position sizing`
5. 任何直接交易建议

## 一句话收口

`malf` 的正式核心是：按时间级别独立记清本级别价格如何形成 HH/HL/LL/LH、结构如何被 break 破坏、以及趋势如何以极值推进延续；其余解释、统计和动作都应在下游分层处理。`

更短收口：

`malf` 是一个按时间级别独立运行的纯语义走势账本系统，用 `pivot / wave` 组织生命周期，用 `HH/HL/LL/LH` 描述结构，用 `break` 标记旧结构失效，用当前 `wave` 内的极值累计刻画趋势推进；统计、背景和动作都只能在下游分层消费。`
