# position data-grade ledger 与 runner 规格

`生效日期`：`2026-04-13`
`状态`：`Active`

## 1. 新增正式账本

当前 `position` 除 `position_run` 外，至少还应新增：

1. `position_work_queue`
2. `position_checkpoint`
3. `position_risk_budget_snapshot`
4. `position_entry_leg_plan`

已有表族需要升级，而不是重命名废弃：

1. `position_candidate_audit`
2. `position_capacity_snapshot`
3. `position_sizing_snapshot`
4. `position_exit_plan`
5. `position_exit_leg`

## 2. work_queue 合同

`position_work_queue` 的最小字段应包括：

1. `queue_nk`
2. `signal_nk`
3. `instrument`
4. `reference_trade_date`
5. `source_signal_fingerprint`
6. `queue_reason`
7. `queue_status`
8. `queued_at`

## 3. checkpoint 合同

`position_checkpoint` 的最小字段应包括：

1. `checkpoint_nk`
2. `instrument`
3. `checkpoint_scope`
4. `last_signal_nk`
5. `last_reference_trade_date`
6. `last_source_signal_fingerprint`
7. `last_completed_at`

## 4. 增量触发

`position` 应在以下场景挂脏：

1. `alpha_formal_signal_event` 新增
2. `alpha_formal_signal_event` rematerialized
3. `market_base(none)` 参考价变化
4. `position_policy_registry` 版本变化
5. 后续接入的 `structure/filter` 上下文 fingerprint 变化

## 5. replay / resume

`position` replay 不得重算全部历史，只允许：

1. 读取 `position_work_queue`
2. 对脏候选或脏计划腿局部重算
3. 用 `position_checkpoint` 跳过未变化历史

## 6. acceptance 最小要求

进入 `portfolio_plan` 前，`position` 至少要通过：

1. 本地官方库 smoke
2. checkpoint/resume smoke
3. rematerialize smoke
4. candidate/capacity/sizing/entry/exit 五层事实对齐检查

## 7. 与当前 runner 的兼容要求

当前 `scripts/position/run_position_formal_signal_materialization.py` 不允许被直接删除。

兼容策略：

1. 在 `50` 之前，它仍是最小 bounded 入口。
2. 在 `50` 中，它应升级或包裹正式 data-grade runner。
3. 对外脚本名和 `WorkspaceRoots` 路径契约不变。

