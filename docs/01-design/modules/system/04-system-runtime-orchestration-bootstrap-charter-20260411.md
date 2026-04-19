# system runtime / orchestration bootstrap 设计宪章

日期：`2026-04-11`
状态：`待执行`

适用执行卡：`105-system-runtime-orchestration-bootstrap-card-20260411.md`

## 背景

`27` 已正式建立 `system` 最小读数与审计能力，但正式 orchestration 仍未落地。由于上游当前先进入 canonical malf 与 trade 回测闭环修订，本卡必须后移到整条链稳定之后。

## 设计目标

1. 为 official runner 提供最小 orchestration 合同。
2. 提供 step 级计划、执行、reuse、失败与 resume 审计。
3. 把一次 bounded orchestration run 与最终 `system_mainline_snapshot` 正式关联起来。

## 当前施工位裁决

1. 本卡必须排在 `55` 与 `100-104` 全部收口之后，不允许跳过 trade 恢复卡组直接做 orchestration。
2. 本卡的系统角色固定为“编排、审计、冻结”，不允许越位定义上游业务事实。
3. 本卡收口后，`system` 才从 bounded acceptance readout 升级为正式 orchestration 入口。

## 核心裁决

1. orchestration 只消费官方 `data -> trade` 正式账本，不回读私有过程。
2. orchestration 必须有 step ledger、checkpoint、resume/retry 与最终 freeze/readout 桥接。
3. `system_mainline_snapshot` 的系统级自然键必须绑定 child-run fingerprint，而不是只依赖 `run_id`。

## 流程图

```mermaid
flowchart LR
    ORCH[orchestration run] --> STEP[step级计划/执行]
    STEP --> REUSE[reuse/失败/resume]
    REUSE --> SNAP[system_mainline_snapshot]
```
