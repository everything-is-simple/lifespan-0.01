# portfolio_plan 最小账本与 position 桥接结论

结论编号：`14`
日期：`2026-04-09`
状态：`生效中`

## 裁决

- 接受：新仓 `portfolio_plan` 已具备最小正式三表、官方 bounded runner 与脚本入口，正式入口为 `scripts/portfolio_plan/run_portfolio_plan_build.py`。
- 接受：`position -> portfolio_plan` 官方桥接已成立，`portfolio_plan` 只消费 `position_candidate_audit / position_capacity_snapshot / position_sizing_snapshot`，不回读 `alpha` 内部临时过程。
- 接受：`portfolio_plan` 已在 `H:\Lifespan-data\portfolio_plan\portfolio_plan.duckdb` 完成真实 bounded pilot，不再停留在 temp-only smoke。
- 接受：组合层 `admitted / blocked / trimmed` 的最小裁决口径已正式落表，并已通过 `inserted / reused / rematerialized` 复跑审计证明可复物化。
- 拒绝：把本轮结果表述成“完整组合回测体系已建成”或“`trade / system` 已可直接消费全部组合层结果”。
- 拒绝：把本轮结果表述成“`portfolio_plan` 已具备多账户、多组合簇与全市场容量治理”。

## 原因

- `13` 之后主线真正缺口已经不在 `alpha` 或 `position`，而在 `position` 之上的组合级裁决账本；本轮正是把这层最小正式主语补齐。
- 单测、官方脚本、正式库 pilot 与 rerun readout 共同证明：`portfolio_plan_snapshot` 已能稳定回答哪些计划被 `admitted / blocked / trimmed`，以及组合容量如何被消耗。
- 受控修改上游 `position` 输入后，`portfolio_plan` 能在自然键不变的前提下显式产生 `rematerialized`，说明该层不是一次性导出，而是可持续维护的历史账本。

## 影响

- 新仓正式主线已从 `data -> malf -> structure -> filter -> alpha -> position` 推进到 `data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan`。
- 后续下一张主线卡应优先回到路线图，规划 `trade` 的最小执行账本与 `portfolio_plan -> trade` 官方桥接，而不是回头把组合语义塞回 `position`。
- 当前执行区的最新已生效结论已推进到 `14`；下一轮正式施工前，应先完成新卡开出与索引切换，而不是直接在 14 号卡之外继续改 `src/`。
