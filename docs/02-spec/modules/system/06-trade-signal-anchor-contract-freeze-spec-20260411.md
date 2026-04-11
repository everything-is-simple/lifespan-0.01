# trade signal anchor contract freeze 规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `29-trade-signal-anchor-contract-freeze-card-20260411.md` 及其后续 evidence / record / conclusion。

## 目标

为 `trade` 回测运行时冻结最小价格锚点合同，使后续 `1R`、快速失败和 `break_last_higher_low` 有正式上游事实可用。

## 最小字段合同

1. `signal_low_price`
2. `signal_low_trade_date`
3. `trailing_anchor_type`
4. `last_higher_low_price`
5. `last_higher_low_trade_date`

## 正式透传路径

1. `alpha_formal_signal_event`
   - 冻结以上字段的正式来源值。
2. `position` 官方输出
   - 原样透传信号锚点，不改写业务语义。
3. `trade_position_leg`
   - 挂账时固化为当时生效的执行锚点，后续 exit engine 只消费该值。

## 约束

1. 不允许 `trade` 直接回读 `alpha` 内部候选过程。
2. 不允许用 `run_id` 代替正式业务键。
3. 空值必须显式表达“当前策略锚点尚不可用”，而不是静默缺省为零。
