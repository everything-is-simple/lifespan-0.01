# trade exit pnl ledger bootstrap

卡片编号：`102`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前 `trade` 只有 `execution_plan / position_leg / carry_snapshot`，没有正式退出账本与 realized pnl 账本，无法沉淀“最终发生了什么”。
- 目标结果：
  建立 `trade_exit_ledger` 与 `trade_realized_pnl_ledger`，作为后续 progression runner 与 system 审计的正式写入目标。
- 为什么现在做：
  `101` 修正 entry 参考价、`100` 冻结 signal anchor 后，这张卡才能稳定定义 `1R`、部分退出和尾仓退出的正式落账口径。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/08-trade-exit-pnl-ledger-bootstrap-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/08-trade-exit-pnl-ledger-bootstrap-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/101-position-entry-t-plus-1-open-reference-price-correction-conclusion-20260411.md`
  - `docs/03-execution/100-trade-signal-anchor-contract-freeze-conclusion-20260411.md`

## 任务分解

1. 冻结 `trade_exit_ledger / trade_realized_pnl_ledger` 的正式表族、自然键与字段口径。
2. 明确 `partial-exit / terminal-exit / fail-fast exit / trailing-stop exit` 如何共同落账。
3. 明确 `entry reference price + signal anchor` 如何计算 `1R`、已实现盈亏和剩余仓位。
4. 回填 `102` 文档、索引与后续 `103` 输入合同。

## 退出账本结构图

```mermaid
flowchart LR
    LEG[trade_position_leg] --> EXIT[trade_exit_ledger]
    EXIT --> PNLR[trade_realized_pnl_ledger]
    PNLR --> SYS[system 审计]
```

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/08-*`
  - `docs/02-spec/modules/system/08-*`
  - `docs/03-execution/102-*`
  - `trade exit / realized pnl` 正式表族与桥接合同
- 范围外：
  - 逐日 progression 队列与 checkpoint
  - system orchestration

## 历史账本约束

- 实体锚点：
  `position_leg_nk / execution_plan_nk`
- 业务自然键：
  `exit_nk / realized_pnl_nk` 必须由正式业务字段稳定复算
- 批量建仓：
  支持对历史 open/closed legs 回灌退出账本
- 增量更新：
  仅对新增 exit、rematerialized leg 或价格变化影响的账目重算
- 断点续跑：
  本卡定义可幂等 upsert 的退出账本，为 `103` 的 queue/checkpoint 提供稳定写入目标
- 审计账本：
  `trade_run`、退出账本、PnL 账本与 `102` execution 文档共同审计

## 正式设计清单

| 设计项 | 正式口径 | 不接受情形 |
| --- | --- | --- |
| 退出主账本 | `trade_exit_ledger` 记录每次退出动作、原因、退出腿角色、退出后剩余仓位 | 把退出只写进 `position_leg` 状态或 summary |
| realized pnl 主账本 | `trade_realized_pnl_ledger` 记录每次退出对应的 realized pnl、成本基准、R multiple | 只在 report 或 system 汇总中临时计算 |
| 退出类型 | 至少区分 `partial-exit / terminal-exit / fail-fast / trailing-stop / time-stop` | 所有退出都混成单一 `close` |
| 锚点输入 | 计算 `1R`、快失败、尾仓时只读 `100/101` 冻结后的正式 anchor 与 entry reference price | 重新从行情或结构层回推 |
| 系统消费 | `system` 只消费正式退出与 realized pnl 账本，不消费 trade 私有中间过程 | system 直接读取 progression 私有状态 |

## 实施清单

| 切片 | 实施内容 | 交付物 |
| --- | --- | --- |
| 切片 1 | 设计 `trade_exit_ledger / trade_realized_pnl_ledger` 的 DDL、主键、来源字段 | 表族设计、字段说明 |
| 切片 2 | 冻结 `1R`、部分退出、尾仓退出、快失败的落账口径 | 计算规则与示例 |
| 切片 3 | 明确 `position_leg` 与 exit/PnL 账本的关系和状态推进规则 | 状态机说明 |
| 切片 4 | 补单元测试与 bounded smoke，验证幂等落账与 rematerialize | tests、evidence 命令 |
| 切片 5 | 回填 `record / conclusion / indexes` | execution 闭环文档 |

## A 级判定表

| 判定项 | A 级通过标准 | 阻断条件 | 对下游影响 |
| --- | --- | --- | --- |
| 退出账本 | `trade_exit_ledger` 正式落表且可幂等 upsert | 退出仍停留在状态位或 summary | `103` 无稳定写入目标 |
| realized pnl | `trade_realized_pnl_ledger` 正式落表且可审计 | realized pnl 仍为临时汇总 | `system` 无法正式审计 PnL |
| 锚点输入稳定 | `1R` 与退出判断只读正式 anchor 与 entry price | 使用临时回推或非正式价格 | 退出事实不可信 |
| 历史回灌与增量 | 支持历史回灌与局部重算 | 只能对新批次生效 | replay 不成立 |
| 审计闭环 | 测试、smoke、record、conclusion 完整 | 只有 DDL 没有证据 | 卡不可收口 |
