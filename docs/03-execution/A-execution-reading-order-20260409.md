# 执行阅读顺序

`日期`：`2026-04-09`
`状态`：`持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `91-malf-timeframe-native-base-source-rebind-conclusion-20260418.md`
5. `15-malf-authoritative-timeframe-native-ledger-charter-20260419.md`
6. `15-malf-authoritative-timeframe-native-ledger-spec-20260419.md`
7. `16-malf-origin-chat-semantic-reconciliation-charter-20260419.md`
8. `16-malf-origin-chat-semantic-reconciliation-spec-20260419.md`
9. `17-malf-truth-contract-stale-guard-and-rebuild-governance-charter-20260419.md`
10. `17-malf-truth-contract-stale-guard-and-rebuild-governance-spec-20260419.md`
11. `80-malf-zero-one-wave-filter-boundary-freeze-conclusion-20260418.md`
12. `81-malf-origin-chat-semantic-truth-gap-freeze-card-20260419.md`
13. `82-malf-break-invalidation-confirmation-contract-freeze-card-20260419.md`
14. `83-malf-last-valid-structure-anchor-and-stale-guard-governance-card-20260419.md`
15. `84-malf-canonical-materialization-repair-and-three-ledger-rebuild-card-20260419.md`
16. `85-malf-post-rebuild-truthfulness-and-audit-acceptance-gate-card-20260419.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `91`。
2. `80` 已完成 `0/1` 波段过滤边界冻结；`91` 已完成 `malf day / week / month` 的 canonical timeframe native source rebind 与官方 full coverage，并已把 `modules/malf/15` 补齐为当前 `malf` 的单点权威设计/规格锚点；`modules/malf/17` 进一步把 `81-85` 的修订总设计/总规格补成图优先入口。
3. 当前正式最近卡位先处理 `81-85`：`81` 负责冻结 origin-chat `malf` 语义、当前 canonical truth gap 与后续修订顺序；`82-83` 固化 truth contract 与 stale guard 治理边界；`84` 再正式修改 canonical truth 并决定三库重建；`85` 最后做 truthfulness / audit acceptance gate；`91-95` 继续作为远置的 downstream cutover 卡组保留。
4. 旧 middle-ledger 分窗建库卡组已删除，不再保留为现行执行路线。
5. `100-105` 仍然只在 `95` 放行后恢复。

## 阅读顺序图

```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["91 最新结论锚点"]
    ANC --> DES["malf/15 权威设计/规格"]
    DES --> GAP["malf/16 origin-gap 设计/规格"]
    GAP --> GOV["malf/17 81-85 总设计/总规格"]
    GOV --> G79["81-85 当前近端卡组"]
    G79 --> G84["91-95 远置后续卡组"]
    G84 --> NEXT["100-105 后置恢复"]
```





