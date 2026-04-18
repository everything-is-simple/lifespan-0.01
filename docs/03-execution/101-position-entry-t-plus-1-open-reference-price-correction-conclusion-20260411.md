# position entry t-plus-1 open reference price correction 结论

结论编号：`101`
日期：`2026-04-11`
状态：`草稿`

## 预设裁决

- 接受：
  当 `market_base.none` 的 `T+1 open` 被正式冻结为 `position -> trade` 的唯一 entry reference price，且 `trade` 只读消费该字段时接受。
- 拒绝：
  如果 `trade` 仍可自行改写参考价，或正式口径仍混用 `T+1 close`、复权价与执行价，则拒绝。

## 预设原因

1. `101` 的核心不是价格修正本身，而是把执行参考价的冻结主权明确落到 `position`。
2. 一旦 entry reference price 不是正式桥接合同的一部分，`102 / 103` 的 `1R`、退出和 progression 就无法保持可复算。
3. `none` 口径服务真实执行，不应与 `backward` 的研究/信号语义混写。

## 预设影响

1. `102` 可以在稳定的 entry reference price 上冻结 exit / realized pnl 账本。
2. `103` 可以只读消费正式执行价格合同，而不是再从行情侧做二次决定。
3. `position` 在新框架下被进一步固化为执行输入冻结层，而不是可有可无的中转层。

## 结论结构图

```mermaid
flowchart LR
    MB["market_base.none T+1 open"] --> P["position entry plan"]
    P --> C["execution-ready contract"]
    C --> T["trade readonly consume"]
    T --> N["放行 102-103"]
```
