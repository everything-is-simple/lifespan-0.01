# mainline real-data smoke regression

卡片编号：`33`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前测试以 mock 合同验证为主，缺少真实数据下的主线 smoke。
- 目标结果：
  用 1-2 只真实股票跑通 `alpha -> position -> trade -> system` 的 bounded 主线回归。
- 为什么现在做：
  没有真实数据 smoke，就无法确认前面 `29-32` 的正式合同在真实样本上成立。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/10-mainline-real-data-smoke-regression-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/10-mainline-real-data-smoke-regression-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/32-trade-backtest-progression-runner-conclusion-20260411.md`

## 任务分解

1. 冻结真实数据 smoke 的样本、窗口和证据要求。
2. 只通过正式 runner 跑 bounded 主线。
3. 回填 `33` 文档与索引。

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/10-*`
  - `docs/02-spec/modules/system/10-*`
  - `docs/03-execution/33-*`
  - `docs/03-execution/evidence/33-*`
  - `docs/03-execution/records/33-*`
- 范围外：
  - 大规模 benchmark
  - 策略收益评价报告

## 历史账本约束

- 实体锚点：
  以 `asset_type + code + smoke_window + smoke_scene` 锚定 smoke 样本。
- 业务自然键：
  以 `code + smoke_window + runner_name` 锚定 smoke 结果；`run_id` 只做审计。
- 批量建仓：
  首次建仓时对选定样本跑全链 bounded window。
- 增量更新：
  后续只按相同样本补跑新增窗口或回归窗口。
- 断点续跑：
  中断后按 runner 级 checkpoint 与摘要结果续跑。
- 审计账本：
  审计落在相关模块正式 run 表与 `33` 的 evidence / record / conclusion。

## 收口标准

1. 真实数据 smoke 的样本与证据标准明确。
2. 只通过正式 runner 执行，不绕过历史账本。
3. 执行索引回填完成。
4. 为 `34` 提供更可信的上游主线证据。
