# portfolio_plan 官方账本族与自然键冻结记录

记录编号：`52`
日期：`2026-04-14`

## 实施记录

1. 把 `portfolio_plan` bootstrap 从最小三表升级到 `v2` 官方账本族
   - 新增 `portfolio_plan_work_queue / checkpoint / candidate_decision / capacity_snapshot / allocation_snapshot / freshness_audit`
   - 保留 `portfolio_plan_run / snapshot / run_snapshot`
   - 对既有 `run / snapshot / run_snapshot` 补齐兼容列，避免旧库只能靠重建迁移
2. 把 `portfolio_plan` runner 从“只写 snapshot”改成“先写官方主语义，再写兼容聚合层”
   - 先按 `reference_trade_date` 逐日计算组合容量
   - 正式物化 `candidate_decision / capacity_snapshot / allocation_snapshot`
   - 最后把 `snapshot` 退回兼容 readout 层，并挂接三类自然键
3. 冻结 `portfolio_id` 组合锚点与三类业务自然键
   - `candidate_decision_nk`
   - `capacity_snapshot_nk`
   - `allocation_snapshot_nk`
   - 明确拒绝继续使用 `run_id` 充当组合业务主键
4. 为避免重新引入文件长度治理债务，把 runner 内部构件拆到 `src/mlq/portfolio_plan/materialization.py`
   - 只拆内部 dataclass / 自然键 / upsert helper
   - 不改外部正式脚本入口与 Python API
5. 单测同步升级到 `52` 新合同
   - 校验 `v2` 九张官方表
   - 校验三类自然键与 `snapshot` 兼容层挂接
   - 校验 `inserted / reused / rematerialized`

## 边界

- `52` 只冻结官方账本族与自然键，不等于 `53` 的厚裁决解释层已经全部完成。
- `52` 只把 `work_queue / checkpoint / freshness_audit` 纳入官方表族，不在本卡内宣称 data-grade 续跑已经收口。
- `52` 仍保持 `portfolio_plan` 只读消费正式 `position` 账本，不回读 `position` 私有 helper、临时 DataFrame 或 `alpha` 内部过程。
