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

```mermaid
flowchart LR
    D[data] --> M[malf]
    M --> S[structure]
    S --> F[filter]
    F --> A[alpha]
    A --> P[position]
    P --> PP[portfolio_plan]
    PP --> T[trade]
    T --> SY[system]
```

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

### malf / structure / filter / alpha / position / portfolio_plan / trade / system

- `scripts/malf/run_malf_snapshot_build.py`
- `scripts/malf/run_malf_canonical_build.py`
- `scripts/malf/run_malf_mechanism_build.py`
- `scripts/malf/run_malf_wave_life_build.py`
  - 正式脚本入口保持不变；实现允许拆分到 `src/mlq/malf/wave_life_runner.py` 与同目录 helper 模块 `wave_life_shared.py / wave_life_source.py / wave_life_materialization.py`，用于满足治理文件长度约束而不改变外部契约。
- `scripts/structure/run_structure_snapshot_build.py`
- `scripts/filter/run_filter_snapshot_build.py`
- `scripts/alpha/run_alpha_trigger_ledger_build.py`
- `scripts/alpha/run_alpha_family_build.py`
- `scripts/alpha/run_alpha_formal_signal_build.py`
- `scripts/position/run_position_formal_signal_materialization.py`
  - 默认 `adjust_method='none'`
- `scripts/portfolio_plan/run_portfolio_plan_build.py`
- `scripts/trade/run_trade_runtime_build.py`
- `scripts/system/run_system_mainline_readout_build.py`
  - 只消费官方 `structure / filter / alpha / position / portfolio_plan / trade` 账本与 `trade_*` 正式落表事实
  - 物化 `system_run / system_child_run_readout / system_mainline_snapshot / system_run_snapshot`
  - 实现允许拆分到 `src/mlq/system/runner.py` 与同目录 helper 模块 `readout_shared.py / readout_children.py / readout_snapshot.py / readout_materialization.py`，但外部脚本入口与 bounded readout 契约保持不变

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
- 当前最新生效结论锚点已推进到 `38-structure-filter-mainline-legacy-malf-semantic-purge-conclusion-20260413.md`：`38` 已完成 `structure / filter` 主线旧版 malf 语义清理，正式主线不再接受 bridge-era `pas_context_snapshot / structure_candidate_snapshot`；当前正式施工卡已切换到 `39-mainline-local-ledger-standardization-bootstrap-card-20260413.md`，新前置卡组顺序为 `39 -> 40`，先完成主线本地账本标准化与增量续跑，再恢复 `100 -> 101 -> 102 -> 103 -> 104 -> 105` 的 trade/system 卡组
- `txt -> raw_market -> market_base` 继续保留为正式 fallback

## 当前 malf 正式口径

- `malf` 的正式核心已冻结为按时间级别独立运行的走势账本，只允许使用 `HH / HL / LL / LH / break / count` 描述本级别结构。
- 高周期 `context`、动作接口、仓位建议与直接交易解释不属于 `malf` core；若后续需要同级别统计或多级别共读，应在 `malf` 之外单独冻结 sidecar 或消费视图。
- `pivot-confirmed break` 已正式冻结为 `malf` 之外的只读机制层 break 确认事实：它只确认 break 站稳，不替代新的 `HH / LL` 推进确认。
- `same-timeframe stats sidecar` 已正式冻结为同级别只读 sidecar：只允许由同级别 `pivot / wave / state / progress` 派生，并供 `structure / filter` 读取，不得回写 `malf core`。
- 当前 `scripts/malf/run_malf_canonical_build.py` 正式物化 canonical v2 `pivot / wave / extreme / state / same_level_stats` 与 `work_queue / checkpoint / run` 账本。
- 当前 `scripts/malf/run_malf_snapshot_build.py` 仍保留 bridge v1 兼容职责：
  - 消费 `market_base.stock_daily_adjusted(adjust_method='backward')`
  - 物化 `pas_context_snapshot / structure_candidate_snapshot`
  - 仅供显式兼容回退或历史桥接链路消费，不再承担默认正式真值职责
  - 实现允许拆分到 `src/mlq/malf/runner.py` 与同目录 helper 模块 `snapshot_shared.py / snapshot_source.py / snapshot_materialization.py`，但外部脚本入口与 bridge v1 契约保持不变
- `malf bootstrap` 的实现允许拆分到 `src/mlq/malf/bootstrap.py` 与 helper 模块 `bootstrap_tables.py / bootstrap_columns.py`，但对外导出的表名常量、bootstrap/连接/path 入口和表族语义保持不变
- `alpha family` 的实现允许拆分到 `src/mlq/alpha/family_runner.py` 与 helper 模块 `family_shared.py / family_source.py / family_materialization.py`，但外部脚本入口与 family ledger 契约保持不变
- `position bootstrap` 的实现允许拆分到 `src/mlq/position/bootstrap.py` 与 helper 模块 `position_shared.py / position_bootstrap_schema.py / position_materialization.py`，但对外导出的表名常量、输入/输出数据结构、bootstrap/连接/path 入口与 position materialization 语义保持不变
- 当前 `scripts/malf/run_malf_mechanism_build.py` 正式负责 bridge-era 机制层 sidecar 账本：
  - 消费 `pas_context_snapshot / structure_candidate_snapshot`
  - 物化 `pivot_confirmed_break_ledger / same_timeframe_stats_profile / same_timeframe_stats_snapshot`
  - 维护 `malf_mechanism_checkpoint`，支持按 `instrument + timeframe` 增量续跑
- 当前 `scripts/malf/run_malf_wave_life_build.py` 正式负责 canonical 波段寿命概率 sidecar：
  - 只读消费 `malf_wave_ledger / malf_state_snapshot / malf_same_level_stats`
  - 物化 `malf_wave_life_run / malf_wave_life_work_queue / malf_wave_life_checkpoint / malf_wave_life_snapshot / malf_wave_life_profile`
  - 默认无窗口调用走 canonical checkpoint 驱动的 queue/replay；显式窗口参数仍保留 bounded 补跑
  - `wave life` 代码实现允许拆分为 runner + helper 模块，但脚本入口、表族命名与只读 sidecar 边界保持不变

## 当前 canonical downstream 默认绑定

- `structure`
  - 默认 `source_context_table / source_structure_input_table='malf_state_snapshot'`
  - 默认 `source_timeframe='D'`
  - bridge v1 只保留为 canonical 表缺失时的兼容回退
- `filter`
  - 默认 `source_context_table='malf_state_snapshot'`
  - 默认 `source_timeframe='D'`
  - bridge v1 `pas_context_snapshot` 只保留兼容回退
- `alpha`
  - `alpha trigger` 继续只读官方 `filter_snapshot + structure_snapshot`
  - `alpha formal signal` 默认关闭 `pas_context_snapshot` fallback，只有显式指定时才启用兼容路径

## 文档治理

正式实现遵循：

`需求 -> 设计 -> 任务分解 -> card -> implementation -> evidence -> record -> conclusion`

硬规则：

1. 先有 `design / spec`，再开 `card`，再实现
2. 缺少前置文档，不允许进入正式实现
3. 缺少 `card / evidence / record / conclusion` 任意一件，不算正式完成
4. 进入 `src/`、`scripts/`、`.codex/` 下的正式实现前，当前待施工卡必须通过 `python scripts/system/check_doc_first_gating_governance.py`
5. 当前待施工卡必须显式填写 `历史账本约束` 六条声明：实体锚点、业务自然键、批量建仓、增量更新、断点续跑、审计账本
6. 正式文档默认多用图：涉及模块边界、数据流、状态机、账本表族或施工顺序时，至少提供一张与正文一致的图，优先使用 Mermaid。
7. 全仓 `python scripts/system/check_development_governance.py` 盘点允许通过 `scripts/system/development_governance_legacy_backlog.py` 登记历史债务；按改动路径触发的严格治理检查仍直接拦截新增违规。
8. `37` 卡收口时，每解决一项历史债务，都必须同步更新 `development_governance_legacy_backlog.py` 与 `37` 对应的 card / evidence / record / conclusion。
9. 当前已完成的清债包括 `src/mlq/system/runner.py`、`src/mlq/trade/runner.py`、`src/mlq/alpha/trigger_runner.py`、`src/mlq/filter/runner.py`、`src/mlq/malf/mechanism_runner.py`、`src/mlq/malf/canonical_runner.py`、`src/mlq/structure/runner.py`、`src/mlq/alpha/runner.py`、`src/mlq/data/runner.py`、`tests/unit/data/test_data_runner.py`、`src/mlq/data/bootstrap.py`、`src/mlq/malf/runner.py`、`src/mlq/malf/bootstrap.py`、`src/mlq/alpha/family_runner.py` 与 `src/mlq/position/bootstrap.py`；当前 target backlog 已清零，`38-40` 已切换为当前正式施工卡组，`100-105` 顺延为后续 trade/system 卡组，本卡的 `pytest` 证据统一按串行命令登记，避免多个进程争用 `H:\Lifespan-temp\pytest-tmp`。

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
