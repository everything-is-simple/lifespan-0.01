# 模块设计入口

日期：`2026-04-09`
状态：`生效中`

本目录用于沉淀新系统各正式模块的长期职责、硬边界和已验证踩坑经验。

这些文档不是临时笔记，而是把老系统真实代价换来的教训，重新冻结成新系统模块设计入口。

补充口径：

- 当前 `malf` 的单点权威总设计不再需要从 `01-14` 自行拼图；统一锚点已经补到 `malf/15-malf-authoritative-timeframe-native-ledger-charter-20260419.md`。
- 当前若要追“聊天里成型的 `malf` 与系统现状差多少、后续怎么修”，则继续读 `malf/16-malf-origin-chat-semantic-reconciliation-charter-20260419.md`。

## 当前来源

当前模块经验主要抽取自：

1. `G:\MarketLifespan-Quant\docs\04-reference\battle-tested-lessons-all-modules-and-mainline-bridging-20260408.md`
2. 老系统 `docs/01-design/modules/` 下各模块已冻结章程
3. 新仓当前已冻结的系统总宪章、共享账本契约与文档先行规则

## 推荐阅读顺序

1. `core/00-core-module-lessons-20260409.md`
2. `data/00-data-module-lessons-20260409.md`
3. `malf/00-malf-module-lessons-20260409.md`
4. `structure/00-structure-module-lessons-20260409.md`
5. `filter/00-filter-module-lessons-20260409.md`
6. `alpha/00-alpha-module-lessons-20260409.md`
7. `position/00-position-module-lessons-20260409.md`
8. `portfolio_plan/00-portfolio-plan-module-lessons-20260409.md`
9. `trade/00-trade-module-lessons-20260409.md`
10. `system/00-system-module-lessons-20260409.md`

## 流程图

```mermaid
flowchart LR
    CORE[core] --> DATA[data]
    DATA --> MALF[malf]
    MALF --> STR[structure]
    STR --> FLT[filter]
    FLT --> ALPHA[alpha]
    ALPHA --> POS[position]
    POS --> PP[portfolio_plan]
    PP --> TRADE[trade]
    TRADE --> SYS[system]
```
