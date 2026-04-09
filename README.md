# lifespan-0.01

`lifespan-0.01` 是本次重构的新主仓。

它不是一个追求“代码越少越漂亮”的实验仓，而是一个必须在个人 PC 上长期运行的本地历史账本系统。

## 系统定位

这个系统面对的现实约束是：

- 数据量大
- 本地机器长期受 `cpu / memory / io` 限制
- 很多计算不能反复全量重跑
- 中间结果必须长期沉淀，才能支撑后续增量更新、断点续跑和正式复盘

因此，本系统的核心目标不是“临时跑通一次”，而是：

1. 采集数据
2. 存储数据
3. 加工数据
4. 再次存储加工后的事实
5. 让这些事实逐步沉淀为可以复查、可续跑、可审计的历史账本

## 当前正式模块

- `core`
- `data`
- `malf`
- `structure`
- `filter`
- `alpha`
- `position`
- `portfolio_plan`
- `trade`
- `system`

当前主链路冻结为：

`data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade -> system`

其中：

- `PAS` 是 `alpha` 内部的一组正式能力，不再单独作为顶层模块存在
- `position` 负责单标的仓位计划与资金管理
- `portfolio_plan` 负责组合层计划、组合回测与容量协调
- `trade` 负责执行与成交账本，不承担组合研究角色
- `system` 负责编排、治理、审计和冻结，不保存策略事实主数据

## 五根目录契约

系统采用五根目录协作：

1. `H:\lifespan-0.01`
   - 代码、文档、测试、治理脚本
2. `H:\Lifespan-data`
   - 正式数据库与长期数据资产
3. `H:\Lifespan-temp`
   - working DB、缓存、pytest、smoke、benchmark 等临时产物
4. `H:\Lifespan-report`
   - 人读报告、图表、导出产物
5. `H:\Lifespan-Validated`
   - 跨版本、跨模块的正式验证资产快照

禁止把临时工作库、缓存、benchmark 产物长期堆在仓库内部。

## 历史账本原则

本系统中的数据库不是“一次运行产物”，而是“历史账本”。

正式数据库应优先满足：

- 自然键累积
- 增量更新
- 断点续跑
- 中间事实永续存储
- 尽量减少重复 CPU/IO 开销

`run_id` 可以保留，但只作为构建批次、审计与追踪元数据，不能再充当历史账本的主语义。

## 当前正式数据库

基础账本：

- `raw_market`
- `market_base`

模块账本：

- `malf`
- `structure`
- `filter`
- `alpha`
- `position`
- `portfolio_plan`
- `trade_runtime`
- `system`

## 文档治理

新仓继承老系统对以下内容的重视：

- 证据
- 记录
- 结论

并进一步强化为“文档先行”：

`需求 -> 设计 -> 任务分解 -> 卡片 -> 实现 -> 证据 -> 记录 -> 结论`

任何正式代码生成、Schema 变更、Pipeline 新增或行为改写，都必须先具备：

1. 需求
2. 设计
3. 任务分解

再进入正式执行闭环。

## 文档入口

建议按以下顺序进入仓库：

1. `docs/README.md`
2. `docs/01-design/00-system-charter-20260409.md`
3. `docs/01-design/01-doc-first-development-governance-20260409.md`
4. `docs/02-spec/00-repo-layout-and-docflow-spec-20260409.md`
5. `docs/02-spec/01-doc-first-task-gating-spec-20260409.md`
6. `docs/03-execution/README.md`

如果只是追当前正式口径，先读 `conclusion`；
如果要继续正式实现，再回到对应 `card / evidence / record`。

