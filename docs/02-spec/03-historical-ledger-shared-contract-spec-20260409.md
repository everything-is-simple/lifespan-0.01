# 历史账本共享合同规格

日期：`2026-04-09`
状态：`生效`

## 适用范围

本规格适用于全仓正式账本、正式快照、正式审计表，以及所有正式 runner。

## 一、稳定自然键规格

### 1. 实体锚点

所有正式表必须先声明实体锚点。

默认要求：

1. 标的类实体：`asset_type + code`
2. 组合/账户类实体：模块内稳定业务编号
3. `name` 只能是属性、快照或审计辅助字段

### 2. 业务自然键

正式表必须在实体锚点之上继续声明业务自然键。至少应包含以下之一：

1. 时间键
2. 窗口键
3. 家族/场景键
4. 状态键
5. 组合/账户维度键

### 3. 禁止项

以下内容不得单独充当正式主语义：

1. `run_id`
2. 自增整数
3. `name`

## 二、两阶段更新规格

每个正式账本都必须显式声明：

1. 一次性批量建仓策略
2. 后续增量更新策略

允许不同模块用不同术语表达，但必须能映射到这两层语义。

## 三、断点续跑规格

每个正式实现都必须说明：

1. checkpoint / cursor / progress 存放位置
2. 中断后如何续跑
3. unchanged replay 如何 no-op
4. dirty queue 是否存在；若存在，如何挂账与消费

## 四、审计账本规格

正式实现必须保留以下审计语义之一或等价字段：

1. `run_id`
2. `written_at / recorded_at / updated_at`
3. `source_provider / source_path / source_digest / source_file_nk`
4. `status / action / summary_json`

## 五、执行卡硬门禁规格

当前待施工卡若要进入 `src/`、`scripts/`、`.codex/` 下的正式实现，必须包含：

## 历史账本约束

- 实体锚点：
- 业务自然键：
- 批量建仓：
- 增量更新：
- 断点续跑：
- 审计账本：

判定规则：

1. 缺少该段，失败
2. 任一条为空，失败
3. 任一条仍是模板占位，失败

## 六、runner 共享要求

正式 runner 至少应具备以下之一：

1. full bootstrap / backfill 入口
2. incremental / replay 入口
3. checkpoint / dirty queue / request ledger 等续跑锚点
4. summary / audit 输出

若某模块暂时只实现 full bootstrap，也必须在卡片与合同中明确“后续 incremental 方案是什么，当前为何尚未落地”。

## 流程图

```mermaid
flowchart LR
    NK[稳定自然键] --> BOOT[full bootstrap]
    BOOT --> INC[incremental/replay]
    INC --> CP[checkpoint/dirty queue]
    CP --> AUD[summary/audit 输出]
    AUD --> GATE[合同合法]
```
