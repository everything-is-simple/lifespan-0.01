# position formal signal runner 与 bounded validation 结论

结论编号：`09`
日期：`2026-04-09`
状态：`生效中`

## 裁决

- 接受：`position` 已有正式 bounded runner，可从官方 `alpha formal signal` 合同读取样本，并用 `market_base` 补齐 `reference_trade_date / reference_price` 后落入正式账本。
- 接受：runner 已复用既有 `materialize_position_from_formal_signals(...)`，没有再造第二套 candidate / sizing / family snapshot 落表逻辑。
- 接受：runner 已兼容一层旧官方列名口径，允许旧式 `signal_id / code / pattern / admission_status` 形式接入桥接合同。
- 拒绝：把本轮结果表述成“`alpha` 正式 producer 已完成”或“`alpha-position` 桥接已全链闭环”。

## 原因

1. 08 已把 `position` 最小账本和 in-process materialization helper 建起来，但缺少正式 runner，导致它还不是一个可被执行文档和下游模块调用的正式入口。
2. 09 把 `alpha` bounded 读取、`market_base` 参考价 enrichment、脚本入口和 summary 合同一起补齐，`position` 从 helper 级推进到了 runner 级。
3. 当前 smoke 证据已经证明：
   - admitted 样本可被正式落表
   - 缺价样本会被跳过而不是伪造参考价
   - 旧列名官方表可以映射进当前桥接合同
4. 但新仓内的 `alpha` 正式 producer 与正式表族尚未落库，因此 09 的完成只代表消费侧 runner 成立，不代表上游生产侧已完工。

## 影响

1. `position` 当前状态从“有 bootstrap 和 helper”推进到“有正式 bounded runner”。
2. `trade / system` 后续若要消费 `position`，已经可以站在正式脚本入口之上，而不是只能调用库内 helper。
3. 系统里程碑 `M2 alpha-position 正式桥接成立` 仍不能标记为完成，因为上游 `alpha formal signal` 正式账本出口还未在新仓落下。
4. 当前最自然的下一步不是继续堆 `position` 内部表，而是回到 `alpha`，把新仓里的正式 `formal signal` 出口 design/spec/card 补齐后落库。
