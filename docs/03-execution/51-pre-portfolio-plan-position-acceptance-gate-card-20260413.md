# 进入 portfolio_plan 前的 position acceptance gate

`卡号`：`51`
`日期`：`2026-04-13`
`状态`：`待施工`

## 需求

- 问题：
  只有在 `position` 达到与上半部一致的 data-grade 质量后，主线才有资格继续进入 `portfolio_plan / trade / system`。
- 目标结果：
  汇总 `47-50` 的正式结论与证据，裁决 `position` 是否已达到进入 `portfolio_plan` 前的 A / A-acceptance 门槛。
- 为什么现在做：
  `50` 已把 `position` 升级为带 `work_queue / checkpoint / replay / rematerialize` 的正式 runner，如果不先做 acceptance gate，就无法合法推进 `52-55`。

## 设计输入

- 设计文档：
  `docs/01-design/modules/system/15-pre-portfolio-plan-position-acceptance-gate-charter-20260413.md`
- 设计文档链接：
  [15-pre-portfolio-plan-position-acceptance-gate-charter-20260413.md](/H:/lifespan-0.01/docs/01-design/modules/system/15-pre-portfolio-plan-position-acceptance-gate-charter-20260413.md)
- 规格文档：
  `docs/02-spec/modules/system/15-pre-portfolio-plan-position-acceptance-gate-spec-20260413.md`
- 规格文档链接：
  [15-pre-portfolio-plan-position-acceptance-gate-spec-20260413.md](/H:/lifespan-0.01/docs/02-spec/modules/system/15-pre-portfolio-plan-position-acceptance-gate-spec-20260413.md)
- 上游结论：
  [50-position-data-grade-checkpoint-and-replay-runner-conclusion-20260414.md](/H:/lifespan-0.01/docs/03-execution/50-position-data-grade-checkpoint-and-replay-runner-conclusion-20260414.md)

## 任务分解

1. 汇总 `47-50` 的结论。
2. 判断 `position` 是否已经达到与 `data -> alpha` 同级的正式质量。
3. 只有 `51` 接受后，才允许进入 `portfolio_plan` 与 `52 -> 55`；`100-105` 仍冻结到 `55`。

## 历史账本约束

- 实体锚点：`position` 的正式候选、计划腿和 checkpoint 实体
- 业务自然键：`candidate_nk / checkpoint_nk / entry_leg_nk / exit_plan_nk / exit_leg_nk`
- 批量建仓：必须验证历史回灌可复现
- 增量更新：必须验证增量挂脏与局部重算可复现
- 断点续跑：必须验证 `work_queue / checkpoint / replay / resume` 有效
- 审计账本：必须形成 evidence / record / conclusion，并以 `position_run / position_run_snapshot` 与 acceptance readout 为准

## A 级判定表

| 判定对象 | A 级通过标准 | 阻断条件 | 对应卡 |
| --- | --- | --- | --- |
| `47` 上下文驱动仓位合同 | `position` 已冻结 MALF 上下文到仓位/分批语义的正式映射，且下游只读消费正式账本 | 仍依赖硬编码权重或 helper 临时推导上下文 | `47` |
| `48` risk/capacity 厚账本 | `risk budget / context cap / single-name cap / portfolio cap / final allowed weight` 已分层落表 | risk/capacity 仍无法解释“为什么只能给这个权重” | `48` |
| `49` 分批进入/退出计划腿 | entry/exit/trim/partial-exit 已以计划腿形式冻结，且未越界写 trade | 计划腿缺失，或 `position` 直接生成执行事实 | `49` |
| `50` data-grade runner | `position` 已具备官方库、queue、checkpoint、incremental、replay、rematerialize、smoke | 仍是 bounded runner，或 queue/checkpoint/freshness 缺失 | `50` |
| `position` 整体 acceptance | `position` 已达到与 `data -> alpha` 同级的 A 或 A-acceptance，可作为 `portfolio_plan` 的唯一正式上游 | 任一子项仍低于 A，或 evidence 无法追溯 | `51` conclusion |

## Gate 判定规则

| 等级 | 判定标准 | 对 `51` 的影响 |
| --- | --- | --- |
| `A` | 正式 ledger、自然键、批量建仓、增量更新、checkpoint/replay、审计/证据全部闭环 | 可通过 |
| `A-acceptance` | 主合同已闭环，残余仅限非主链读数或展示性欠账，不影响 `portfolio_plan` 消费 | 可通过，但必须在 conclusion 显式挂账 |
| `B` | ledger 已有，但 queue/checkpoint/replay/freshness 任一缺失，或计划腿/风险事实仍不完整 | 不可通过 |
| `C` | 仍依赖 bounded helper、私有过程或临时汇总 | 不可通过 |
