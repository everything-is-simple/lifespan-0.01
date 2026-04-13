# position risk budget 与 capacity ledger 硬化

`卡号`：`48`
`日期`：`2026-04-13`
`状态`：`待施工`

## 问题

- 当前 `position_capacity_snapshot` 只覆盖极薄的 cap 交集，缺少 risk budget 正式账本。
- `FIXED_NOTIONAL_CONTROL` 与 `SINGLE_LOT_CONTROL` 的旧实验结论还没有升格为主线中的显式 capacity decomposition。

## 设计依据

- [02-position-malf-context-driven-batched-management-charter-20260413.md](/H:/lifespan-0.01/docs/01-design/modules/position/02-position-malf-context-driven-batched-management-charter-20260413.md)
- [04-position-malf-context-driven-batched-management-spec-20260413.md](/H:/lifespan-0.01/docs/02-spec/modules/position/04-position-malf-context-driven-batched-management-spec-20260413.md)

## 任务

1. 新增 `position_risk_budget_snapshot` 正式账本。
2. 把 `risk budget / context cap / single-name cap / portfolio cap / final allowed weight` 拆开落表。
3. 让 `FIXED_NOTIONAL_CONTROL` 成为 operating baseline，让 `SINGLE_LOT_CONTROL` 成为 floor sanity，而不是并列主逻辑。
4. 为后续 `portfolio_plan` 输出清晰的风险门控事实。

## 历史账本约束

1. `实体锚点`
   - `candidate_nk`。
2. `业务自然键`
   - `risk_budget_snapshot_nk / capacity_snapshot_nk`。
3. `批量建仓`
   - 对历史正式 signal 回灌所有 risk/capacity 分解快照。
4. `增量更新`
   - 对 signal 或参考价变化只重算脏候选。
5. `断点续跑`
   - 本卡先定义账本和物化语义，续跑由 `50` 接管。
6. `审计账本`
   - 每个 snapshot 必须保留来源 policy、source fingerprint、contract version。

## A 级判定表

| 判定项 | A 级通过标准 | 不接受情形 | 交付物 |
| --- | --- | --- | --- |
| risk budget 正式账本 | `position_risk_budget_snapshot` 正式落表，并可与 `candidate / capacity / sizing` 一一追踪 | risk budget 仍停留在代码内临时变量、summary 字段或测试夹具 | `position_risk_budget_snapshot` DDL 与物化规则 |
| 容量分解厚账本 | `risk budget / context cap / single-name cap / portfolio cap / final allowed weight` 分层落表，且每层有原因字段 | 仍只有一个最终 cap 值，无法解释为何被裁减 | 分层字段、原因码、读数口径 |
| baseline 冻结 | `FIXED_NOTIONAL_CONTROL` 成为正式 operating baseline，`SINGLE_LOT_CONTROL` 只保留 floor sanity 辅助角色 | 两套控制逻辑继续并列争夺主导权，或旧实验常量直接外溢到正式主链 | policy registry 与 contract version |
| 自然键稳定 | `risk_budget_snapshot_nk / capacity_snapshot_nk` 可由业务字段稳定复算，并绑定 `candidate_nk` | 依赖 `run_id`、窗口参数或写入顺序决定主键 | 自然键定义与 upsert 规则 |
| 批量与增量 | 支持历史 signal 全量回灌，也支持只对脏候选重算 risk/capacity 分解 | risk/capacity 只能跟随整批 materialization 重跑 | bootstrap / incremental 入口说明 |
| 下游可消费性 | `portfolio_plan` 可直接消费 risk/capacity 正式账本判断 admitted/blocked/trimmed/deferred | 组合层仍需读 helper、重新推导 risk budget 或重新计算 cap | 下游输入列清单与消费契约 |
