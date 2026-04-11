# trade signal anchor contract freeze

卡片编号：`29`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前 `trade` 已经有 `t_plus_1_open / entry_open_minus_signal_low / half_at_1r / break_last_higher_low` 等策略标签，但真正驱动这些策略的价格锚点没有被正式冻结为跨模块合同。`signal_low`、`last_higher_low` 目前没有稳定地从 `alpha` 透传到 `position` 再透传到 `trade_position_leg`。
- 目标结果：
  冻结 `signal_low / signal_low_trade_date / last_higher_low / last_higher_low_trade_date / trailing_anchor_type` 的正式来源层、字段语义与透传路径，形成 `alpha -> position -> trade` 的共享合同。
- 为什么现在做：
  不先冻结这些字段，后续 `31/32` 就只能凭策略字符串猜价格锚点，无法做可信的 exit / pnl / progression。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/06-trade-signal-anchor-contract-freeze-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/06-trade-signal-anchor-contract-freeze-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/27-system-mainline-bounded-acceptance-readout-and-audit-bootstrap-conclusion-20260411.md`
  - `docs/03-execution/28-system-wide-checkpoint-and-dirty-queue-alignment-conclusion-20260411.md`

## 任务分解

1. 明确 `signal_low` 与 `last_higher_low` 的官方来源层，不允许在 `trade` 临时回推或二次猜测。
2. 冻结 `alpha_formal_signal -> position_* -> trade_position_leg` 的字段桥接与空值语义。
3. 回填 `29` 的 execution 文档与索引，为 `30-32` 提供正式输入。

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/06-*`
  - `docs/02-spec/modules/system/06-*`
  - `docs/03-execution/29-*`
  - `docs/03-execution/evidence/29-*`
  - `docs/03-execution/records/29-*`
  - 相关入口索引
- 范围外：
  - 直接实现回测推进循环
  - 改写 `position` sizing 逻辑
  - 引入 live/runtime 语义

## 历史账本约束

- 实体锚点：
  以 `asset_type + code + signal_date + signal_family + trigger_stage` 作为信号锚点，以 `portfolio_id + leg_nk` 作为 `trade_position_leg` 锚点。
- 业务自然键：
  以 `formal_signal_nk` 锚定 `alpha` 字段，以 `position_candidate_nk` 锚定 `position` 透传，以 `leg_nk` 锚定 `trade` 透传；`run_id` 只做审计。
- 批量建仓：
  首次对既有 `alpha_formal_signal_event` 与 `position/trade` 表族补齐缺失字段与历史透传值。
- 增量更新：
  新信号只沿 `alpha -> position -> trade` 单向透传，不允许 `trade` 反写上游。
- 断点续跑：
  任一步骤中断后，允许按正式自然键幂等补写字段，不允许靠重新跑全表猜测历史值。
- 审计账本：
  审计落在 `alpha_formal_signal_run / position_run / trade_run` 与 `29` 的 evidence / record / conclusion。

## 收口标准

1. `signal_low / last_higher_low` 的来源层与字段语义被正式冻结。
2. `alpha -> position -> trade` 的字段桥接被写入 design/spec/card。
3. `29` 与 `30-32` 的依赖顺序被说明清楚。
4. 执行索引完成回填。
