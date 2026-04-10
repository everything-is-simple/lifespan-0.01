# 全系统历史账本增量治理硬约束设计

日期：`2026-04-10`
状态：`生效`

## 背景

卡 `17`、`19`、`20` 已经分别把股票 txt、`TdxQuant(none)`、以及 `index/block txt` 的 `raw -> base` 历史账本链路做成了正式机制，证明下面这套模式在本仓可行：

1. 先一次性批量建仓
2. 再每日增量更新
3. 支持断点续跑与 replay
4. 使用稳定自然键而不是 `run_id` 充当主语义
5. 保留完整审计账本

但这个模式目前仍主要体现在 `data/raw/base` 实现经验里，还没有被提升成全系统硬约束。后续如果 `malf / structure / filter / alpha / position / portfolio_plan / trade / system` 各自按模块习惯随意定义主键、增量策略、checkpoint 与 replay 语义，重构的“长期可续跑历史账本系统”目标会再次失真。

## 设计目标

1. 把“批量建仓 + 日更增量 + 断点续跑 + 稳定自然键 + 审计账本”冻结为全系统共享合同。
2. 明确 `code + name` 不是全系统主键口径；正式主锚必须先落在稳定实体锚点上。
3. 要求所有后续正式卡在进入 `src/`、`scripts/`、`.codex/` 前，显式声明自己的自然键、批量建仓、增量策略、断点续跑与审计账本语义。
4. 把上述约束接入治理检查器，避免“文档有原则，代码没硬门禁”的回退。

## 核心设计

### 1. 实体锚点与业务自然键分层

- 全系统正式账本先声明稳定实体锚点：
  - 行情/标的相关默认是 `asset_type + code`
  - `name` 保留为实体快照属性、兼容映射或审计字段
- 在实体锚点之上，再叠加业务自然键：
  - 行情类：`trade_date + adjust_method`
  - snapshot/event/ledger 类：再叠加 `window / family / scene / state / effective_date / account / portfolio` 等
- `run_id`、批次号、自增整数都只能做审计字段，不得充当正式主语义。

### 2. 全系统统一两阶段机制

- 所有正式历史账本必须回答两个阶段：
  - 一次性批量建仓如何做
  - 每日断点续传增量更新如何做
- 如果某模块并非“日更市场数据”，也必须给出等价描述：
  - 首次全量回填
  - 后续按自然键与 checkpoint 的增量物化

### 3. checkpoint / dirty / replay 必须显式

- 正式实现必须声明：
  - checkpoint 写在哪里
  - 中断后如何续跑
  - unchanged replay 如何避免重复重算
  - dirty queue 是否存在；若存在，如何挂账与消费
- 允许不同模块采用 `checkpoint`、`request ledger`、`dirty queue`、`scope ledger` 等不同实现，但不允许“不声明，只靠代码习惯”。

### 4. 审计账本必须独立于业务主键

- 正式账本必须保留审计字段或等价表：
  - `run_id`
  - `written_at / recorded_at / updated_at`
  - `source_provider / source_digest / source_file_nk / source_run_id`
  - `status / action / summary_json`
- 审计字段与业务主键必须分层，不允许把“方便追踪的一次运行”误写成“长期真相主语义”。

### 5. 治理门禁接入点

- 执行卡模板新增 `## 历史账本约束` 段，要求显式填写：
  - 实体锚点
  - 业务自然键
  - 批量建仓
  - 增量更新
  - 断点续跑
  - 审计账本
- `check_doc_first_gating_governance.py` 升级为强校验上述段落。
- 后续任何触碰 `src/`、`scripts/`、`.codex/` 的正式改动，如果当前卡缺少该段或仍是占位内容，直接失败。

## 边界

### 范围内

- 共享历史账本合同升级
- 当前卡模板升级
- `doc-first gating` 检查器升级
- 入口文件口径同步

### 范围外

- 逐个模块一次性重写现有全部 schema
- 为所有既有模块补齐新的 runtime/checkpoint 实现
- 引入新的数据库或迁移框架

## 影响

- 卡 `20` 形成的 `raw/base` 机制被提升为全系统正式治理标准，而不再只是 data 模块经验。
- 后续新卡如果不显式说明“如何建仓、如何增量、如何续跑、自然键是什么、审计账本在哪里”，将无法合法进入正式实现。
