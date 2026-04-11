# lifespan-0.01

`lifespan-0.01` 是面向个人 PC 的、本地优先的历史账本系统。目标不是“单次跑通”，而是让市场数据、研究语义、执行账本都能长期沉淀为可续跑、可复算、可审计的正式资产。

## 系统定位

本仓库默认服从这些现实约束：

- 数据量大
- 本地 `cpu / memory / io` 受限
- 很多计算不能反复全量重跑
- 中间事实必须长期沉淀

因此，正式数据库优先满足：

- 自然键累积
- 增量更新
- 断点续跑
- 中间事实永续存储
- 尽量减少重复 CPU/IO 成本

新增全系统硬规则：

- 稳定实体锚点优先，标的类默认使用 `asset_type + code`
- `name` 只作属性、快照或审计辅助字段，不替代正式主键
- 所有正式实现都必须声明：
  - 一次性批量建仓
  - 后续增量更新
  - checkpoint / dirty queue / replay 续跑语义
  - 审计账本
- `run_id` 只做审计，不做正式业务主语义

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

## 五根目录契约

1. `H:\lifespan-0.01`
   - 代码、文档、测试、治理脚本
2. `H:\Lifespan-data`
   - 正式数据库与长期数据资产
3. `H:\Lifespan-temp`
   - working DB、缓存、pytest、smoke、benchmark 等临时产物
4. `H:\Lifespan-report`
   - 人读报告、图表、导出产物
5. `H:\Lifespan-Validated`
   - 正式验证资产快照

`pytest` cache、`basetemp`、smoke 临时目录、benchmark 临时产物都必须落到 `H:\Lifespan-temp`。

## 当前正式 runner 入口

### data

- `scripts/data/run_tdx_stock_raw_ingest.py`
  - 从本地官方离线目录把股票 txt 日线增量写入 `raw_market.stock_file_registry / stock_daily_bar`
- `scripts/data/run_tdx_asset_raw_ingest.py`
  - 从本地官方离线目录把 `stock / index / block` txt 日线增量写入各自 `raw_market.{asset}_file_registry / {asset}_daily_bar`
  - 支持一次性建仓、每日断点续传、文件级 `skipped_unchanged`
- `scripts/data/run_tdxquant_daily_raw_sync.py`
  - 把 `TdxQuant(dividend_type='none')` 作为股票日更原始事实桥接进 `raw_market.stock_daily_bar(adjust_method='none')`
  - 只标记 `base_dirty_instrument(adjust_method='none')`
- `scripts/data/run_market_base_build.py`
  - 从官方 `raw_market` 物化 `market_base.{stock,index,block}_daily_adjusted`
  - 支持 `--asset-type {stock,index,block}`

### malf / structure / filter / alpha / position / portfolio_plan / trade

- `scripts/malf/run_malf_snapshot_build.py`
- `scripts/structure/run_structure_snapshot_build.py`
- `scripts/filter/run_filter_snapshot_build.py`
- `scripts/alpha/run_alpha_trigger_ledger_build.py`
- `scripts/alpha/run_alpha_family_build.py`
- `scripts/alpha/run_alpha_formal_signal_build.py`
- `scripts/position/run_position_formal_signal_materialization.py`
- `scripts/portfolio_plan/run_portfolio_plan_build.py`
- `scripts/trade/run_trade_runtime_build.py`

## 当前 data 正式口径

- `txt -> raw_market -> market_base` 现在正式覆盖：
  - `stock`
  - `index`
  - `block`
- `TdxQuant(dividend_type='none')` 正式桥接股票 `raw_market.stock_daily_bar(adjust_method='none')`
- 价格口径冻结为：
  - `malf -> structure -> filter -> alpha` 默认消费 `adjust_method='backward'`
  - `position -> trade` 默认消费 `adjust_method='none'`
  - `forward` 当前只作研究与展示保留
- `txt -> raw_market -> market_base` 继续保留为正式 fallback

## 当前 malf 正式口径

- `malf` 的正式核心已冻结为按时间级别独立运行的走势账本，只允许使用 `HH / HL / LL / LH / break / count` 描述本级别结构。
- 高周期 `context`、动作接口、仓位建议与直接交易解释不属于 `malf` core；若后续需要同级别统计或多级别共读，应在 `malf` 之外单独冻结 sidecar 或消费视图。
- 当前 `scripts/malf/run_malf_snapshot_build.py` 仍保留 bridge v1 兼容职责：
  - 消费 `market_base.stock_daily_adjusted(adjust_method='backward')`
  - 物化 `pas_context_snapshot / structure_candidate_snapshot`
  - 供现有 `structure` runner 过渡消费

## 文档治理

正式实现遵循：

`需求 -> 设计 -> 任务分解 -> card -> implementation -> evidence -> record -> conclusion`

硬规则：

1. 先有 `design / spec`，再开 `card`，再实现
2. 缺少前置文档，不允许进入正式实现
3. 缺少 `card / evidence / record / conclusion` 任意一件，不算正式完成
4. 进入 `src/`、`scripts/`、`.codex/` 下的正式实现前，当前待施工卡必须通过 `python scripts/system/check_doc_first_gating_governance.py`
5. 当前待施工卡必须显式填写 `历史账本约束` 六条声明：实体锚点、业务自然键、批量建仓、增量更新、断点续跑、审计账本

## 建议阅读顺序

1. `AGENTS.md`
2. `docs/README.md`
3. `docs/01-design/00-system-charter-20260409.md`
4. `docs/01-design/01-doc-first-development-governance-20260409.md`
5. `docs/01-design/α-system-roadmap-and-progress-tracker-charter-20260409.md`
6. `docs/01-design/modules/README.md`
7. `docs/02-spec/00-repo-layout-and-docflow-spec-20260409.md`
8. `docs/02-spec/01-doc-first-task-gating-spec-20260409.md`
9. `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
10. `docs/03-execution/README.md`

如果只追当前正式口径，优先看 `docs/03-execution/*conclusion*`。
