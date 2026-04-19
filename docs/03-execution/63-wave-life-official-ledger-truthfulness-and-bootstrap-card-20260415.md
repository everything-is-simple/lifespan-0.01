# wave life official ledger truthfulness and bootstrap card

`卡号：63`
`日期：2026-04-15`
`状态：已完成`

## 需求

- 问题：当前正式库 `malf_wave_life_snapshot` 与 `malf_wave_life_profile` 行数为 `0`，说明 `wave_life` 虽已冻结 sidecar 合同，但尚未在正式库形成可消费事实。
- 目标结果：裁决 `wave_life` 的正式库真值状态、bootstrap/replay 路径，以及它成为下游正式输入前必须满足的最小可用性标准。
- 为什么现在做：`alpha` 与 `family` 当前拿到的 `exhaustion_risk_bucket / reversal_probability_bucket` 为空时，只能退化为结构腿数粗分桶；在 `62` 收口后，需要继续确认 `wave_life` 是否仍属于允许空缺的 sidecar，还是必须进入正式库真值。

## 设计输入

- `docs/01-design/modules/malf/13-malf-wave-life-probability-sidecar-charter-20260411.md`
- `docs/03-execution/36-malf-wave-life-probability-sidecar-bootstrap-conclusion-20260412.md`
- `docs/03-execution/59-mainline-middle-ledger-2010-truthfulness-gate-conclusion-20260414.md`
- `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`

## 任务分解

1. 固化当前正式库 `wave_life` 空表事实、runner 状态与下游空值传播路径。
2. 裁决 `wave_life` 在正式库中应先补齐 bootstrap，还是继续保持可选 sidecar。
3. 回填 `63` evidence / record / conclusion，并明确 `wave_life` 对 `alpha/position` 的准入前提。

## 实现边界

- 本卡只处理 `wave_life` 正式库 truthfulness、bootstrap 与可消费性。
- 本卡不直接设计 `alpha` 的 action matrix。
- 如需重跑，只允许围绕 canonical `malf_wave_ledger / malf_state_snapshot / malf_same_level_stats` 只读消费。

## 历史账本约束

- 实体锚点：`asset_type + code + timeframe + wave_id`
- 业务自然键：`instrument + timeframe + wave_nk`
- 批量建仓：允许一次性 bootstrap `malf_wave_life_snapshot / profile`
- 增量更新：继续沿 canonical checkpoint/work queue 驱动
- 断点续跑：不得把寿命概率反写回 `malf core`
- 审计账本：`malf_wave_life_run / work_queue / checkpoint / snapshot / profile` 与 `63-* evidence / record / conclusion`

## 收口标准

1. `wave_life` 正式库现状已有正式 truthfulness 定性。
2. `wave_life` 是否必须补齐到正式库已有裁决。
3. 下游消费前提与缺失时的降级口径已写清。

## 卡片结构图

```mermaid
flowchart LR
    CANON["malf_wave_ledger / state / stats"] --> WL["wave_life sidecar"]
    WL --> DB["official DB snapshot/profile"]
    DB --> DOWN["alpha / position downstream"]
```
