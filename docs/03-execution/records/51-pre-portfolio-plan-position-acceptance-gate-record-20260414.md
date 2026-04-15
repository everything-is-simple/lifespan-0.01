# 进入 portfolio_plan 前的 position acceptance gate 记录

记录编号：`51`
日期：`2026-04-14`

## 实施记录

1. 先按 `51` 设计与规格回收 `47-50` 的正式结论，不额外发明新的验收口径
   - `47` 负责冻结 MALF context 与 schedule 合同
   - `48` 负责冻结 risk budget / capacity 厚账本
   - `49` 负责冻结 batched entry / trim / partial-exit 计划腿
   - `50` 负责补齐 data-grade queue / checkpoint / replay / rematerialize
2. 重新跑一轮 `position` 单测，确认 acceptance gate 收口时当前代码状态仍与 `50` 证据一致
   - `tests/unit/position` 在新的 `basetemp` 下再次通过
3. 把 `51` 定义为“准入裁决卡”，而不是新的 schema / runner 施工卡
   - 本卡不新增 `position` 表族
   - 本卡不修改 `portfolio_plan` 代码或 DDL
   - 本卡只把 `position` 是否达到进入 `52-55` 的门槛裁决清楚
4. 裁决结果固定为：`position` 已成为 `portfolio_plan` 的唯一正式上游
   - 下游应只读消费正式 `position_candidate_audit / position_capacity_snapshot / position_sizing_snapshot`
   - 不允许回读 `alpha` 内部过程，也不允许回读 `position` 私有 helper 或临时 DataFrame
5. 收口后把当前待施工卡前移到 `52`
   - `52-55` 继续承担 `portfolio_plan` 与 pre-trade upstream baseline 的正式施工
   - `100-105` 继续冻结到 `55` 接受之后

## 边界

- `51` 不等于 `portfolio_plan` 已完成；它只说明 `position` 作为上游已经达到准入门槛。
- `51` 不提前恢复 `100 -> 105`；trade/system 仍受 `55` gate 约束。
- `51` 不回退到重新讨论 `43-46` 的 upstream 质量门槛；这些结论继续作为已接受前提。
