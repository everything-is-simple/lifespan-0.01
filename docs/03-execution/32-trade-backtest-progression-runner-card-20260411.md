# trade backtest progression runner

卡片编号：`32`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前系统没有把 open leg 逐日推进到 closed 的正式回测引擎。
- 目标结果：
  实现基于日线 OHLC 的最小 progression runner，支持快速失败、`1R` 半仓和 `break_last_higher_low`。
- 为什么现在做：
  这是实际回测模型的核心缺口，也是当前 `trade` 从“记计划”走到“记结果”的关键一步。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/09-trade-backtest-progression-runner-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/09-trade-backtest-progression-runner-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/31-trade-exit-pnl-ledger-bootstrap-conclusion-20260411.md`

## 任务分解

1. 冻结 progression runner 的输入、规则集与 checkpoint 语义。
2. 明确 open leg 的逐日推进边界与 exit 落账关系。
3. 回填 `32` 文档与索引。

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/09-*`
  - `docs/02-spec/modules/system/09-*`
  - `docs/03-execution/32-*`
  - `docs/03-execution/evidence/32-*`
  - `docs/03-execution/records/32-*`
- 范围外：
  - 实盘执行
  - tick 级回放

## 历史账本约束

- 实体锚点：
  以 `leg_nk` 为主锚点，以 `leg_nk + progress_trade_date` 作为 progression checkpoint 锚点。
- 业务自然键：
  `leg_nk + progress_trade_date + progress_stage`；`run_id` 只做审计。
- 批量建仓：
  首次对既有 open legs 全量挂入 progression queue。
- 增量更新：
  后续仅推进仍为 open 的 legs 与新开 legs。
- 断点续跑：
  中断后从最近 `progress_trade_date` checkpoint 恢复。
- 审计账本：
  审计落在 `trade_run / trade_carry_snapshot / trade_leg_exit` 与 `32` 的 evidence / record / conclusion。

## 收口标准

1. 回测推进边界与规则集被写入正式文档。
2. checkpoint / resume 语义成立。
3. 能作为 `33` 真实数据 smoke 的执行基础。
4. 执行索引回填完成。
