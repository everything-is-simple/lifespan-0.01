# portfolio_plan 容量与裁决账本硬化记录

记录编号：`53`
日期：`2026-04-14`

## 实施记录

1. 把 `portfolio_plan` 的正式主语义从“最小 admitted/blocked 结果”推进到“厚裁决账本”
   - `portfolio_plan_candidate_decision` 新增 `decision_rank / decision_order_code / trade_readiness_status`
   - 同步挂接 `capacity_before_weight / capacity_after_weight`
   - 正式透传 `source_binding_cap_code / source_capacity_source_code / source_required_reduction_weight`
2. 把组合层容量读数升级为正式厚账本
   - `portfolio_plan_capacity_snapshot` 新增 `requested_candidate_count`
   - 同步落下 `requested/admitted/blocked/trimmed/deferred` 五类权重与计数
   - 正式冻结 `binding_constraint_code / capacity_decision_reason_code / capacity_reason_summary_json`
3. 冻结组合层排序与延后合同
   - 组合层当前正式排序规则冻结为 `requested_weight desc -> instrument -> candidate_nk`
   - `schedule_stage / schedule_lag_days` 未到交易窗口时，正式落为 `deferred`
   - `trade_readiness_status` 明确区分 `trade_ready / blocked / await_schedule`
4. 保持 `portfolio_plan` 边界不越界
   - 仍只读消费 `position_candidate_audit / position_capacity_snapshot / position_sizing_snapshot`
   - 不回读 `position` 私有过程，也不把组合裁决回写到 `position`
5. 为避免重新引入长度治理债务，把组合层容量解释 helper 收敛回 `materialization.py`
   - `runner.py` 只保留 bridge 查询、分日 materialization 与 run 审计主流程
   - 容量原因汇总、绑定约束解析与排序 helper 均回收到内部 helper 模块
6. 单测同步升级到 `53` 厚账本合同
   - 覆盖 `admitted / blocked / trimmed / deferred`
   - 覆盖 `decision_reason_counts / trade_readiness_counts / capacity_reason_summary_json`
   - 覆盖 `inserted / reused / rematerialized`

## 边界

- `53` 只完成组合层容量与裁决账本硬化，不宣称 `work_queue / checkpoint / replay / freshness` 已收口。
- `53` 不提前恢复 `100 -> 105`；当前待施工卡只前移到 `54`。
- `trade` 仍未恢复为 data-grade 主线模块；本卡只为其后续直接消费 `portfolio_plan` 正式输出铺路。
