# position entry t+1 open reference price correction

卡片编号：`30`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前 `position` 使用 `T+1 close` 作为参考价，与业务模型的 `T+1 open` 入场不一致。
- 目标结果：
  将 `position` 正式参考价切换到 `market_base.stock_daily_adjusted(adjust_method='none')` 的 `T+1 open`。
- 为什么现在做：
  不先修正这个口径，后续 `R`、仓位 sizing 与 `1R` 半仓都会带系统性偏差。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/07-position-entry-t-plus-1-open-reference-price-correction-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/07-position-entry-t-plus-1-open-reference-price-correction-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/29-trade-signal-anchor-contract-freeze-conclusion-20260411.md`

## 任务分解

1. 冻结 `T+1 open` 的正式参考价口径与空值语义。
2. 回填 `30` 文档与索引。
3. 为 `31/32` 提供一致的 entry price 基线。

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/07-*`
  - `docs/02-spec/modules/system/07-*`
  - `docs/03-execution/30-*`
  - `docs/03-execution/evidence/30-*`
  - `docs/03-execution/records/30-*`
- 范围外：
  - exit/pnl 引擎
  - live execution 语义

## 历史账本约束

- 实体锚点：
  以 `asset_type + code + signal_date + sizing_policy` 作为 `position` 候选锚点。
- 业务自然键：
  以 `position_candidate_nk` 锚定参考价物化记录；`run_id` 只做审计。
- 批量建仓：
  对既有 bounded 窗口允许重新物化参考价字段。
- 增量更新：
  新信号只按 `T+1` 交易日增量查询 `open`。
- 断点续跑：
  若参考价查询中断，允许按候选自然键幂等续跑。
- 审计账本：
  审计落在 `position_run` 与 `30` 的 evidence / record / conclusion。

## 收口标准

1. `T+1 close -> T+1 open` 的正式口径被写死。
2. 与 `31/32` 的依赖关系明确。
3. 执行索引回填完成。
4. 入口文件保持一致。
