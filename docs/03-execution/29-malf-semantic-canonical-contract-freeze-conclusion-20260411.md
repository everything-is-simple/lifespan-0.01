# malf semantic canonical contract freeze 结论

日期：`2026-04-11`
状态：`已裁决`

`29` 已正式通过。当前 `malf` 真值口径冻结为：

- 输入只允许是本级别 `price bars`
- 核心正式账本只允许是：
  - `malf_pivot_ledger`
  - `malf_wave_ledger`
  - `malf_extreme_progress_ledger`
  - `malf_state_snapshot`
  - `malf_same_level_stats`
- 核心原语只允许是 `HH / HL / LL / LH / break / count`
- `D / W / M` 必须独立计算结构，不允许跨级别共享状态、极值、计数
- 高周期 `context`、执行动作、仓位建议、跨级别解释反馈不属于 `malf core`
- bridge v1 `pas_context_snapshot / structure_candidate_snapshot` 仍可保留供 `31` 之前的下游过渡消费，但不再代表 `malf` 正式真值

本次裁决同步落到了：

- `06-malf-semantic-canonical-contract-freeze-charter-20260411.md`
- `06-malf-semantic-canonical-contract-freeze-spec-20260411.md`
- `AGENTS.md`
- `README.md`

从这一结论开始，后续 `malf` 正式实现、下游 rebind 与 truthfulness revalidation，必须全部以 canonical v2 纯语义账本为准，而不能再以 `MA / ret20 / new_high_count` 近似层为真值。
