# system runtime / orchestration bootstrap 卡

卡片编号：`105`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  `27` 已建立最小 `readout / audit`，但 `system` 仍停留在 bounded acceptance；它还没有以统一 checkpoint/queue 语义编排全链并沉淀正式 orchestration 审计。
- 目标结果：
  建立 `system` 的最小正式 orchestration bootstrap，使其成为全链“正式审计与冻结入口”，而不是临时汇总器；并把它明确绑定到 `84` 之后的新官方 upstream contract。
- 为什么现在做：
  这必须排在 `84`、`100-104` 全部之后，尤其要等 `malf -> alpha` 官方 cutover、trade progression 与真实官方库 smoke 全部通过。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/04-system-runtime-orchestration-bootstrap-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/04-system-runtime-orchestration-bootstrap-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/104-mainline-real-data-smoke-regression-conclusion-20260411.md`

## 层级归属

- 主层：`system`
- 上游输入：`alpha` 五 PAS 日线正式库、正式 `position` 桥接账本、正式 `trade` 执行/退出/PnL/progression 账本，以及必要的 `portfolio_plan`
- 本卡职责：建立基于新 upstream contract 的正式 orchestration / acceptance / freeze 入口

## 任务分解

1. 冻结 `system` orchestration 的 step ledger、checkpoint、resume/retry 与 acceptance/freeze 边界。
2. 明确 `system` 只消费 `84` 之后的官方 `alpha / position / portfolio_plan / trade` 正式账本，不回读任何模块私有过程。
3. 明确 orchestration run 与 `system_child_run_readout / system_mainline_snapshot` 的正式关联方式。
4. 明确 child-run fingerprint 如何绑定五 PAS `alpha`、正式 `position` 与正式 `trade` 的 run/readout。
5. 回填 `105` 文档与索引，完成 `trade/system` 卡组收口。

## 流程图

```mermaid
flowchart LR
    ORCH[orchestration run] --> STEP[step ledger]
    STEP --> CP[checkpoint]
    CP --> RESUME[resume/retry]
    RESUME --> SNAP[system_mainline_snapshot]
```

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/04-*`
  - `docs/02-spec/modules/system/04-*`
  - `docs/03-execution/105-*`
  - `system` orchestration 的 step/checkpoint/freeze 合同
- 范围外：
  - broker runtime
  - live order routing
  - 回写上游业务事实

## 历史账本约束

- 实体锚点：
  `portfolio_id + snapshot_date + system_scene`
- 业务自然键：
  `orchestration_run_nk / orchestration_step_nk / system_checkpoint_nk`
- 批量建仓：
  支持按日期窗口或组合窗口补建 system orchestration 审计
- 增量更新：
  只对脏 child-run 组合或脏 snapshot 窗口重跑
- 断点续跑：
  正式交付 `system_work_queue or orchestration_step ledger + system_checkpoint + resume/retry`
- 审计账本：
  `system_run / system_child_run_readout / system_mainline_snapshot / system_acceptance_ledger / system_run_snapshot`

## 正式设计清单

| 设计项 | 正式口径 | 不接受情形 |
| --- | --- | --- |
| 消费边界 | `system` 只读取 `84` 后官方 `alpha / position / portfolio_plan / trade` 账本及必要上游 readout，不回读私有过程 | 回读模块私有过程、helper 表或临时文件 |
| orchestration 主账本 | 需有 step 级 ledger，记录计划、执行、reuse、失败、resume/retry | 只有最终汇总，没有 step 审计 |
| checkpoint/resume | 必须能从 step 级 checkpoint 恢复，不重跑全部子步骤 | 失败后只能从头再跑 |
| freeze 入口 | orchestration run 最终写 `system_mainline_snapshot` 与 acceptance/freeze 读数 | orchestration 只生成日志，不落正式冻结事实 |
| 自然键 | 系统级 NK 绑定 `portfolio_id + snapshot_date + system_scene + child-run fingerprint` | 仅用 run_id 表示一切 |
| upstream 合同 | child-run fingerprint 必须能追溯到五 PAS `alpha`、正式 `position` 与正式 `trade` 的官方 run/readout | 只记录抽象“某次主线运行”，不记录真实上游来源 |
| 边界约束 | `system` 不定义上游业务事实，只编排、审计、冻结 | orchestration 越界改写 trade/position/alpha 真值 |

## 实施清单

| 切片 | 实施内容 | 交付物 |
| --- | --- | --- |
| 切片 1 | 设计 `orchestration_step ledger / system_checkpoint / acceptance_ledger` | DDL、字段说明 |
| 切片 2 | 冻结 step 状态机：planned/running/reused/failed/resumed/completed | 状态机图与规则表 |
| 切片 3 | 定义 child-run fingerprint 与 `system_mainline_snapshot` 的绑定关系，明确如何指向五 PAS `alpha`、正式 `position` 与正式 `trade` | snapshot/freeze 合同 |
| 切片 4 | 补测试与 bounded smoke，验证 resume/retry 与只读消费边界 | tests、evidence 命令 |
| 切片 5 | 回填 `record / conclusion / indexes` | execution 闭环文档 |

## A 级判定表

| 判定项 | A 级通过标准 | 阻断条件 | 对系统角色的影响 |
| --- | --- | --- | --- |
| 正式审计入口 | `system` 成为全链正式审计与冻结入口 | 仍只是 bounded readout | 全链无法系统级复算 |
| step/checkpoint | orchestration 具备 step ledger、checkpoint、resume/retry | 失败后只能重头执行 | 不可称为可续跑系统 |
| 只读消费边界 | 只消费官方上游账本，且显式承接 `84` 后新 upstream contract | 回读私有过程 | 系统越界污染主链 |
| freeze/readout 关联 | orchestration 结果可稳定关联 `system_mainline_snapshot` 与 child readout | 只有日志、无冻结事实 | 系统 acceptance 不成立 |
| upstream 可追溯 | child-run fingerprint 能明确追溯到五 PAS `alpha`、正式 `position` 与正式 `trade` | 无法知道冻结基于哪套官方上游 | `system` 读数不可信 |
| 审计闭环 | tests、smoke、record、conclusion 完整 | 只有设计没有证据 | 卡不可收口 |
