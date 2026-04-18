# 执行阅读顺序

`日期`：`2026-04-09`
`状态`：`持续更新`

## 首读顺序

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `80-malf-timeframe-native-base-source-rebind-conclusion-20260418.md`
5. `18-malf-alpha-dual-axis-and-timeframe-native-refactor-charter-20260418.md`
6. `18-malf-alpha-dual-axis-and-timeframe-native-refactor-spec-20260418.md`
7. `81-structure-thin-projection-and-day-binding-card-20260418.md`

## 当前正式口径

1. 最新生效结论锚点已推进到 `80`。
2. `80` 已完成 `malf day / week / month` 的 canonical timeframe native source rebind 与官方 full coverage，当前正式待施工卡已切到 `81-structure-thin-projection-and-day-binding-card-20260418.md`。
3. 当前正式卡组是 `81-84`：它负责把 `structure` 收敛为 `day / week / month` 三薄层、把 `filter` 收薄到 objective gate + note sidecar，并把 `alpha` 切到五个 PAS 日线终审账本，最后再做 truthfulness / cutover gate。
4. 旧 middle-ledger 分窗建库卡组已删除，不再保留为现行执行路线。
5. `100-105` 仍然只在 `84` 放行后恢复。

## 阅读顺序图

```mermaid
flowchart LR
    CONC["00 结论目录"] --> BCAT["B 卡目录"]
    BCAT --> CLED["C 完成账本"]
    CLED --> ANC["80 最新结论锚点"]
    ANC --> DES["18 新版设计/规格"]
    DES --> G79["81 当前待施工卡"]
    G79 --> G84["81-84 后续卡组"]
    G84 --> NEXT["100-105 后置恢复"]
```
