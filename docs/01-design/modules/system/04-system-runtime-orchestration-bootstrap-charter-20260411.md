# system runtime / orchestration bootstrap 设计宪章

日期：`2026-04-11`
状态：`待执行`

适用执行卡：`34-system-runtime-orchestration-bootstrap-card-20260411.md`

## 背景

`27` 已经正式建立了 `system_run / system_child_run_readout / system_mainline_snapshot / system_run_snapshot` 四表最小读数与审计能力，但 `system` 仍然停留在“读数者”位置，没有成为“编排者”。

## 设计目标

1. 为 official runner 提供最小 orchestration 合同。
2. 提供 step 级计划、执行、reuse、失败与 resume 审计。
3. 把一次 bounded orchestration run 与最终 `system_mainline_snapshot` 正式关联起来。

## 核心裁决

1. `system` orchestration 只编排 official runner，不回写上游业务事实。
2. `system` orchestration 仍然是本地历史账本运行时，不是 live broker runtime。
3. orchestration 只在 `28-33` 先把 queue/checkpoint、价格锚点、exit/pnl 和真实数据 smoke 夯实后进入正式实现。

## 最小对象

1. `system_orchestration_run`
2. `system_orchestration_step`
3. `system_orchestration_checkpoint`
4. `system_orchestration_run_snapshot`

## 非目标

1. broker/account lifecycle
2. live order routing
3. 全量 filled / pnl / reconciliation
