# system runtime / orchestration bootstrap 规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `105-system-runtime-orchestration-bootstrap-card-20260411.md` 及其后续 evidence / record / conclusion。

## 目标

为 `system` 冻结最小 orchestration 合同，使一次 bounded mainline 执行拥有正式 step ledger、checkpoint 和最终 snapshot bridge。

## 最小合同

1. 正式账本至少包括：
   - orchestration step ledger
   - `system_checkpoint`
   - `system_acceptance_ledger` 或等价正式冻结读数
2. step ledger 至少记录：
   - `step_nk`
   - `step_name`
   - `step_status`
   - `source_child_run_id`
   - `materialization_action`
   - `started_at / completed_at`
3. checkpoint 至少记录：
   - `system_checkpoint_nk`
   - `portfolio_id`
   - `snapshot_date`
   - `last_completed_step`
   - `last_success_child_fingerprint`
4. orchestration 只能消费官方 child ledger，不得回读私有中间过程。

## 流程图

```mermaid
flowchart LR
    ORCH[orchestration run] --> STEP[step ledger]
    STEP --> CP[checkpoint]
    CP --> RESUME[resume/retry]
    RESUME --> SNAP[system_mainline_snapshot]
```
