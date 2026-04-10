# lifespan-0.01

`lifespan-0.01` 是这次重构的新主仓。
它不是追求“代码越少越漂亮”的实验仓，而是一个必须在个人 PC 上长期运行的本地历史账本系统。

## 系统定位

这个系统面对的现实约束是：

- 数据量大
- 本地机器长期受 `cpu / memory / io` 限制
- 很多计算不能反复全量重跑
- 中间结果必须长期沉淀，才能支撑后续增量更新、断点续跑和正式复盘

因此，本系统的核心目标不是临时跑通一次，而是：

1. 采集数据
2. 储存数据
3. 加工数据
4. 再次储存加工后的事实
5. 让这些事实逐步沉淀为可复查、可续跑、可审计的历史账本

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

当前主链冻结为：

`data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade -> system`

其中：

  - `PAS` 是 `alpha` 内部的一组正式能力，不再单独作为顶层模块存在
  - `data` 负责把本地离线市场数据沉淀为官方 `raw_market / market_base` 历史账本
  - `malf` 负责把官方 `market_base` 价格事实沉淀为官方市场语义快照
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
`pytest` 的 cache 与 basetemp 也属于临时产物，必须写到 `H:\Lifespan-temp`。

## 历史账本原则

本系统中的数据库不是一次运行产物，而是历史账本。
正式数据库应优先满足：

- 自然键累积
- 增量更新
- 断点续跑
- 中间事实永续存储
- 尽量减少重复 CPU/IO 开销

`run_id` 可以保留，但只作为构建批次、审计与追踪元数据，不能再充当历史账本的主语义。
正式运行时应优先通过环境变量或 `scripts/setup/enter_repo.ps1` 固定五根目录；`repo_root.parent` 形式的相邻目录回退只作为测试和开发兜底。

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

## 当前正式 runner 入口

- `scripts/data/run_tdx_stock_raw_ingest.py`
  - 从 `H:\tdx_offline_Data` 按 `gbk` ingest 官方 TDX 股票日线到 `raw_market`
  - 记录 `stock_file_registry / stock_daily_bar`
  - 支持文件级跳过、断点续跑与 `inserted / reused / rematerialized`
- `scripts/data/run_tdxquant_daily_raw_sync.py`
  - 从 `TdxQuant(dividend_type='none')` 按 request/checkpoint 账本语义桥接官方日更原始事实到 `raw_market`
  - 记录 `raw_tdxquant_run / raw_tdxquant_request / raw_tdxquant_instrument_checkpoint`
  - 只写 `stock_daily_bar(adjust_method='none')`，并只标记 `base_dirty_instrument(adjust_method='none')`
- `scripts/data/run_market_base_build.py`
  - 从官方 `raw_market` 物化 `market_base.stock_daily_adjusted`
  - 正式沉淀 `adjust_method in {none, backward, forward}` 三套价格
- `scripts/malf/run_malf_snapshot_build.py`
  - 从官方 `market_base.stock_daily_adjusted` 做 bounded 读取
  - 默认消费 `adjust_method='backward'`
  - 物化 `malf_run / pas_context_snapshot / structure_candidate_snapshot`
- `scripts/structure/run_structure_snapshot_build.py`
  - 从官方 `malf` 上游的结构候选事实与执行上下文做 bounded 读取
  - 物化 `structure_run / structure_snapshot / structure_run_snapshot`
  - 产出可被 `filter / alpha` 稳定消费的官方结构事实层
- `scripts/filter/run_filter_snapshot_build.py`
  - 从官方 `structure snapshot` 与最小 `execution_context` 做 bounded 读取
  - 物化 `filter_run / filter_snapshot / filter_run_snapshot`
  - 产出可被 `alpha` 优先消费的官方 pre-trigger 准入层
- `scripts/alpha/run_alpha_formal_signal_build.py`
  - 从官方 `alpha trigger ledger` 与 `filter / structure snapshot` 做 bounded 读取
  - 物化 `alpha_formal_signal_run / event / run_event`
  - 产出可被 `position` 直接消费的官方 `alpha formal signal`
- `scripts/alpha/run_alpha_family_build.py`
  - 从官方 `alpha_trigger_event` 与 bounded family candidate 输入做 bounded 读取
  - 物化 `alpha_family_run / alpha_family_event / alpha_family_run_event`
  - 产出可被 `alpha formal signal` 与后续审计稳定引用的官方 `alpha family ledger`
- `scripts/alpha/run_alpha_trigger_ledger_build.py`
  - 从 bounded detector 输入与官方 `filter / structure snapshot` 做 bounded 读取
  - 物化 `alpha_trigger_run / alpha_trigger_event / alpha_trigger_run_event`
  - 产出可被 `alpha formal signal` 稳定引用的官方 `alpha trigger ledger`
- `scripts/position/run_position_formal_signal_materialization.py`
  - 从官方 `alpha formal signal` 做 bounded 读取
  - 用 `market_base.stock_daily_adjusted(adjust_method='none')` 补 `reference_trade_date / reference_price`
  - 复用 `materialize_position_from_formal_signals(...)` 落 `position` 正式账本
- `scripts/portfolio_plan/run_portfolio_plan_build.py`
  - 从官方 `position_candidate_audit / position_capacity_snapshot / position_sizing_snapshot` 做 bounded 读取
  - 物化 `portfolio_plan_run / portfolio_plan_snapshot / portfolio_plan_run_snapshot`
  - 产出可被后续 `trade / system` 消费的最小组合裁决账本
- `scripts/trade/run_trade_runtime_build.py`
  - 从官方 `portfolio_plan_snapshot`、`market_base.stock_daily_adjusted(adjust_method='none')` 与上一轮 `trade_carry_snapshot` 做 bounded 读取
  - 物化 `trade_run / trade_execution_plan / trade_position_leg / trade_carry_snapshot / trade_run_execution_plan`
  - 冻结 `planned_entry / blocked_upstream / planned_carry` 最小执行事实与持仓延续

当前价格口径冻结为：

- `malf -> structure -> filter -> alpha` 默认使用 `backward`
- `position -> trade` 默认使用 `none`
- `forward` 当前仅作为研究与展示保留

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

在此基础上，当前待施工卡还必须通过 `python scripts/system/check_doc_first_gating_governance.py` 的硬门禁检查，才允许进入 `src/`、`scripts/`、`.codex/` 下的正式实现。

## 入口文件

下面三个文件是新仓入口，不允许长期滞后于当前治理口径：

1. `AGENTS.md`
2. `README.md`
3. `pyproject.toml`

只要治理规则、环境脚手架、路径契约、测试入口、执行入口发生变化，就必须同步刷新这三个入口文件。
`docs/01-design/`、`docs/02-spec/` 和 `src/mlq/core/paths.py` 的正式口径变化，也视为入口变化。

## 文档入口

建议按以下顺序进入仓库：

1. `AGENTS.md`
2. `docs/README.md`
3. `docs/01-design/00-system-charter-20260409.md`
4. `docs/01-design/01-doc-first-development-governance-20260409.md`
5. `docs/01-design/03-historical-ledger-shared-contract-charter-20260409.md`
6. `docs/01-design/04-doc-first-gating-checker-charter-20260409.md`
7. `docs/01-design/α-system-roadmap-and-progress-tracker-charter-20260409.md`
8. `docs/01-design/modules/README.md`
9. `docs/02-spec/00-repo-layout-and-docflow-spec-20260409.md`
10. `docs/02-spec/01-doc-first-task-gating-spec-20260409.md`
11. `docs/02-spec/03-historical-ledger-shared-contract-spec-20260409.md`
12. `docs/02-spec/04-doc-first-gating-checker-spec-20260409.md`
13. `docs/02-spec/β-system-roadmap-and-progress-tracker-spec-20260409.md`
14. `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
15. `docs/03-execution/README.md`

其中：

- `α / β / Ω` 现在不只负责阶段看板，也负责说明各模块主要吸收自哪些老仓来源、当前继承方式是什么、哪些地方仍未冻结。
