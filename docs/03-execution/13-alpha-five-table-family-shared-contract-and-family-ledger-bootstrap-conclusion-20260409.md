# alpha 五表族共享合同与 family ledger bootstrap 结论

结论编号：`13`
日期：`2026-04-09`
状态：`生效中`

## 裁决

- 接受：新仓 `alpha` 已具备最小正式 `family ledger` 三表与 bounded runner，正式入口为 `scripts/alpha/run_alpha_family_build.py`。
- 接受：`alpha family ledger` 已在 `H:\Lifespan-data\alpha\alpha.duckdb` 完成真实 bounded pilot，不再停留在 `temp-only` smoke。
- 接受：`bof / pb` 两个核心 family 已在共享合同下完成最小 bootstrap，`alpha_family_event.trigger_event_nk` 已稳定引用官方 `alpha_trigger_event.trigger_event_nk`。
- 接受：重复运行与受控上游变化已显式证明 `inserted / reused / rematerialized` 三种动作在 family ledger 层也成立。
- 拒绝：把本轮结果表述成“五家族全部最终专表已齐备”或“full-history family backfill 已完成”。
- 拒绝：把本轮结果表述成“下游已经可以绕过 formal signal 直接消费 family ledger”。 

## 原因

- `12` 已经把共享 trigger 事实层立住，本轮真正剩余的主线空白就是 `alpha` 内部 family-specific 的最小正式解释层。
- 单元测试与正式 pilot 共同证明：family ledger 不只是多一层临时表，而是能在自然键不变的前提下复用既有事实，并在官方 trigger 上游变化时进行复物化。
- 正式库 readout 证明 `alpha_family_event` 已经成为 `alpha_trigger_event` 之上的稳定解释层，而不是下游临时 research sidecar。

## 影响

- `alpha` 当前已经从“trigger ledger + formal signal 两级正式账本”推进到“trigger ledger + family ledger + formal signal 三级正式账本”。
- 后续继续扩 `alpha` 时，应优先在当前 family ledger 之上细化 `bof / tst / pb / cpb / bpb` 的 family-specific payload、trace 或专表，而不是回头扩 `position` 掩盖上游缺口。
- 当前正式主线卡已清零；下一轮应回到 `Ω-system-delivery-roadmap-20260409.md` 重新规划 `portfolio_plan / trade / system` 或 `alpha` 后续深挖的正式开工顺序。
