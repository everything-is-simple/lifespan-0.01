# system runtime / orchestration bootstrap 规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `34-system-runtime-orchestration-bootstrap-card-20260411.md` 及其后续 evidence / record / conclusion。

## 目标

为 `system` 冻结最小 orchestration 合同，使一次 bounded mainline 执行拥有正式 step ledger、checkpoint 和最终 snapshot bridge。

## 正式输入

1. official runner 脚本
   - `scripts/structure/run_structure_snapshot_build.py`
   - `scripts/filter/run_filter_snapshot_build.py`
   - `scripts/alpha/run_alpha_trigger_ledger_build.py`
   - `scripts/alpha/run_alpha_formal_signal_build.py`
   - `scripts/position/run_position_formal_signal_materialization.py`
   - `scripts/portfolio_plan/run_portfolio_plan_build.py`
   - `scripts/trade/run_trade_runtime_build.py`
   - `scripts/system/run_system_mainline_readout_build.py`
2. 既有 `system_mainline_snapshot`
3. bounded window 与 portfolio scope

## 最小表族

1. `system_orchestration_run`
2. `system_orchestration_step`
3. `system_orchestration_checkpoint`
4. `system_orchestration_run_snapshot`

## 最小状态

`planned / running / completed / reused / failed / skipped`

## 约束

1. orchestration 不重写上游业务逻辑。
2. orchestration 只消费 official contract。
3. orchestration 必须支持 checkpoint / resume。
