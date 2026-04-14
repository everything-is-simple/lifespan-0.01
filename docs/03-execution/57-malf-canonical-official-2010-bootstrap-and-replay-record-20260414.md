# malf canonical official 2010 bootstrap and replay 记录
`记录编号`：`57`
`日期`：`2026-04-14`

## 实施记录

1. 先核实真实正式数据前置。
   - 发现 `market_base.stock_daily_adjusted(adjust_method='backward')` 在执行前只有 `920000.BJ` 一只票、`1,000` 行数据
   - 发现 `raw_market.stock_daily_bar(adjust_method='backward')` 的 `2010` 窗口实际已有 `1,833` 个代码、`392,478` 行
2. 先用正式 `data` runner 补齐 `2010 backward` 窗口。
   - 使用 `run_market_base_build.py` 对 `2010-01-01 ~ 2010-12-31` 做一次定向 full build
   - 补齐后，`market_base(backward)` 的 `2010` 窗口达到 `1,833` 个代码、`392,478` 行
3. 在真实正式 `malf` 库上执行 canonical bootstrap。
   - `run_malf_canonical_build.py` 首跑 `2010`，完成 `5,499` 个 `asset_type + code + timeframe` scope
   - canonical 五账本、work queue 与 checkpoint 全部落地
4. 补做 replay / resume 验证。
   - 第二次同窗运行没有新增 enqueue/claim/materialization
   - 说明 canonical checkpoint 已经把 `2010` 窗口压成严格 no-op

## 边界

- `57` 只负责真实正式 `malf` canonical `2010` bootstrap 与 replay，不负责 downstream `structure / filter / alpha`。
- `57` 接受后，bridge-v1 表族允许继续并存，但不再代表 `malf` 正式真值。
- `57` 不裁决 `2010` pilot 是否足以放行后续三年窗口，这个裁决留给 `59`。
