# data/malf 最小官方主线桥接结论

结论编号：`16`
日期：`2026-04-10`
状态：`生效中`

## 裁决

- 接受：新仓缺失的官方前半段主线已补齐到 `data -> malf -> structure`，可以作为后续整链复核的正式起点。
- 接受：`market_base.stock_daily_adjusted` 必须长期同时保存 `none / backward / forward` 三套价格。
- 接受：`malf -> structure -> filter -> alpha` 默认使用 `backward`，`position -> trade` 默认使用 `none`。
- 拒绝：在 `data -> malf` 未成立之前继续把 `trade` 或 `system` 当成主线下一锤。
- 拒绝：把当前状态表述成“整条 `data -> ... -> system` 已经系统级跑通”。

## 原因

1. 真实正式库已经补出 `raw_market`、`market_base` 与 `malf` 的官方最小账本层，而不是继续停留在样例数据。
2. `H:\Lifespan-report\data\card16\malf-001.json` 与 `H:\Lifespan-report\data\card16\structure-001.json` 已证明现有 `structure` runner 能真实消费新生成的官方 `malf` 上游。
3. `H:\Lifespan-report\data\card16\malf-002.json` 与 `H:\Lifespan-report\data\card16\structure-002.json` 已证明 rerun 时存在 `reused`，不是一次性跑通假象。
4. 执行层若继续使用复权价，会把参考价格与股数计算带偏，因此 `position / trade` 必须显式回到 `none`。

## 影响

1. 当前主线口径被纠正为：先承认 `data -> malf` 已成立，再谈整链复核；不允许再绕过前半段直接讨论 `system`。
2. 后续任何 `structure / filter / alpha` 正式实现，都应默认建立在官方 `market_base(backward) -> malf` 上游之上。
3. 后续任何 `position / trade` 正式实现，都应默认建立在官方 `market_base(none)` 执行参考价之上。
4. 下一步不是新增治理 sidecar，而是基于这条已补齐的前半段主线，复核 `data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade` 是否整体真实对接，然后才决定是否开 `system` 卡。
