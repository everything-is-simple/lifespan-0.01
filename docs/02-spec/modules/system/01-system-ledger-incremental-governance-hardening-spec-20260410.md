# 全系统历史账本增量治理硬约束规格

日期：`2026-04-10`
状态：`生效`

## 适用范围

本规格适用于全仓正式模块：

- `data`
- `malf`
- `structure`
- `filter`
- `alpha`
- `position`
- `portfolio_plan`
- `trade`
- `system`

以及它们对应的正式数据库、正式表族、正式 runner、正式治理脚本。

## 一、稳定自然键规格

### 1. 实体锚点

所有围绕证券、指数、板块、账户、组合、仓位、订单、信号的正式表，必须先声明稳定实体锚点。

默认要求：

1. 标的类实体：`asset_type + code`
2. 组合/账户类实体：模块内稳定业务编号
3. `name` 只能是属性、快照或审计辅助字段

禁止：

- 只用 `name`
- 只用 `run_id`
- 只用自增整数

### 2. 业务自然键

正式表必须在实体锚点之上，再声明业务自然键。至少应包含以下之一：

1. 时间键：`trade_date / asof_date / effective_date`
2. 窗口键：`window_start / window_end / holding_days`
3. 家族键：`family / detector / profile / scene`
4. 状态键：`state / status / phase`
5. 组合键：`account / portfolio / leg`

### 3. `run_id` 限制

- `run_id` 只能用于审计、追踪、回放定位
- `run_id` 不得作为正式主语义
- 任何“按 run_id 直接覆盖全表”的实现都不算正式历史账本机制

## 二、两阶段更新规格

每个正式账本或正式物化层都必须显式声明：

1. 一次性批量建仓策略
2. 每日或每批次增量更新策略

允许表达方式：

- full bootstrap + incremental update
- historical backfill + daily append
- full materialization + dirty queue incremental replay

但不允许缺失其中任一层。

## 三、断点续跑规格

每个正式实现都必须说明：

1. checkpoint / progress / cursor 写在哪里
2. 中断后如何续跑
3. unchanged replay 如何避免重复写入或重复重算
4. 如果存在 dirty queue，如何挂账与消费

若模块不使用单独 checkpoint 表，也必须明确说明“等价 checkpoint 由哪张账本承担”。

## 四、审计账本规格

正式实现必须保留以下审计信息之一或等价语义：

1. `run_id`
2. `written_at / recorded_at / updated_at`
3. `source_provider / source_path / source_digest / source_file_nk`
4. `status / action / summary_json`

允许审计字段分散在多张表中，但不允许完全缺失。

## 五、执行卡硬门禁规格

所有进入正式实现的当前待施工卡，必须新增并填写：

## 历史账本约束

- 实体锚点：
- 业务自然键：
- 批量建仓：
- 增量更新：
- 断点续跑：
- 审计账本：

判定规则：

1. 缺少该段，失败
2. 仍是模板占位内容，失败
3. 任一条为空，失败
4. 链接的设计/规格文档不存在，失败

## 六、治理检查器规格

`scripts/system/check_doc_first_gating_governance.py` 必须在原有检查基础上，新增对 `## 历史账本约束` 的强校验。

触发条件：

- 当本次改动命中 `src/`、`scripts/`、`.codex/`
- 或全仓扫描当前待施工卡时

通过条件：

1. 当前待施工卡具备需求、设计、规格、任务分解
2. 当前待施工卡具备 `历史账本约束` 六条声明

## 七、模板规格

`docs/03-execution/00-card-template-20260409.md` 必须同步携带 `## 历史账本约束` 段，避免未来新卡遗漏此项。

## 流程图

```mermaid
flowchart LR
    NK[稳定自然键] --> BOOT[一次性 full bootstrap]
    BOOT --> INC[每日增量 upsert]
    INC --> CP[checkpoint/dirty queue]
    CP --> AUD[run 审计账本]
    AUD --> GOV[治理检查通过]
```
