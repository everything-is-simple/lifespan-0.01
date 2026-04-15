# portfolio_plan data-grade checkpoint / replay / freshness 记录
`记录编号`：`54`
`日期`：`2026-04-14`

## 实施记录

1. 把 `portfolio_plan` 从 bounded materialization 升级为正式 data-grade runner
   - `run_portfolio_plan_build(...)` 新增 `bootstrap_mode / incremental_mode / replay_mode`
   - 默认规则冻结为：有显式窗口时走 `bootstrap`，无显式窗口时走 `incremental`，显式指定时可走 `replay`
2. 补齐组合层独立 `work_queue`
   - 每条 queue 行以 `portfolio_id + candidate_nk + reference_trade_date` 为稳定挂账单元
   - queue 行新增 `checkpoint_nk / source_fingerprint / source_run_id / claimed_at / completed_at`
   - `replay` 模式允许显式把候选重新挂账，而不是退化成整段历史全量重跑
3. 冻结组合层 `checkpoint` 续跑语义
   - 候选级 `checkpoint` 保存 source fingerprint 与最近完成边界
   - 组合级 `portfolio_gross` checkpoint 记录最近完成交易日与最后一个候选
   - `incremental / replay` 命中任一候选时，会扩展到同交易日全量上下文重算，避免容量裁决只算局部
4. 补齐 `freshness_audit`
   - 以 source `position` 桥接表的 `expected_reference_trade_date` 对比当前 `portfolio_plan_snapshot` 的 `latest_reference_trade_date`
   - partial claim 时如果仍有更新窗口未追平，正式读数落为 `stale`
5. 保持 `53` 已冻结厚账本语义不回退
   - `candidate_decision / capacity_snapshot / allocation_snapshot / snapshot` 的自然键与解释字段保持不变
   - `run_snapshot` 追加 `queue_nk / queue_reason` 审计字段，不改已有主键与落账契约
6. CLI 入口同步升级
   - `scripts/portfolio_plan/run_portfolio_plan_build.py` 新增 `--bootstrap-mode / --incremental-mode / --replay-mode`
   - 继续固定使用正式脚本入口，不把 runner 语义藏在临时脚本里
7. 单测升级到 `54`
   - 覆盖 `bootstrap`
   - 覆盖 `incremental` 首次挂账与二次 no-op 复跑
   - 覆盖 `replay` 命中候选后扩到同交易日全量上下文
   - 覆盖 `freshness stale` 读数

## 边界

- `54` 只负责把 `portfolio_plan` 提升到 data-grade runner，不提前宣称 `55` 已通过。
- `54` 仍不恢复 `100 -> 105`；只有 `55` 完成 pre-trade baseline gate 后才允许继续。
- `54` 不越界回写 `position` 私有过程，也不把 `portfolio_plan` 扩成 `trade` runtime。
