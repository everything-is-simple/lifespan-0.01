# trade backtest progression runner 规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `32-trade-backtest-progression-runner-card-20260411.md` 及其后续 evidence / record / conclusion。

## 输入

1. `trade_position_leg`
2. `trade_leg_exit`
3. `trade_carry_snapshot`
4. `market_base.stock_daily_adjusted(adjust_method='none')`

## 最小规则集

1. `t1_close_below_signal_low_then_t2_open_exit`
2. `half_at_1r`
3. `break_last_higher_low`
