# TdxQuant 日更原始事实接入 raw/base 账本桥接 结论

结论编号：`19`
日期：`2026-04-10`
状态：`草稿`

## 裁决

- 接受：卡 `19` 的实现方向聚焦于“把 TdxQuant 日更原始事实正式接进 raw_market 账本”。
- 接受：卡 `19` 当前只应把官方 `none` 路线视作原始事实桥接对象。
- 拒绝：在卡 `19` 里直接把 `TdxQuant(front/back)` 当作正式 `raw_forward / raw_backward` 或正式 `market_base`。

## 原因

- 卡 `18` 已经证明，`TdxQuant` 更接近日更主源头，但其 `front/back` 当前不具备稳定、可复算的正式复权语义。
- 当前最需要推进的不是继续手工 `txt` 导入，而是把官方日更事实纳入现有 `raw/base` 的 run ledger、checkpoint、dirty queue 与 fallback 机制。

## 影响

- 当前待施工卡切到 `19`，后续 `data` 侧的正式推进应围绕 TQ raw 桥接层展开。
- 当前卡 `17` 的 `txt -> raw_market -> market_base` 入口继续保持生效。
- 卡 `19` 的切片 2 已补齐最小代码骨架：
  - `raw_tdxquant_run / request / checkpoint` 三表已进入正式 bootstrap
  - `run_tdxquant_daily_raw_sync(...)` 已可桥接 `dividend_type='none'`
  - `request/checkpoint` skipped_unchanged 与 failed run 审计已有单测覆盖
- 后续若卡 `19` 实现成功，下一步最可能的新问题将转向：
  - TQ raw 日更与 `txt` fallback 的并存治理
  - 仓内复权物化如何接管 `forward / backward`
