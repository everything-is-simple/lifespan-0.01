# 文档先行硬门禁检查器 结论

结论编号：`03`
日期：`2026-04-09`
状态：`生效中`

## 裁决

- 接受：`doc-first gating` 已成为正式治理链的一部分，当前待施工卡在进入 `src/`、`scripts/`、`.codex/` 下的正式实现前，必须先具备需求、设计、规格和任务分解。
- 拒绝：无。

## 原因

1. 新检查器已经落地，并可在严格模式下拦截缺少前置文档的正式实现改动。
2. `check_development_governance.py` 已串联该检查器，仓库不再只依赖口头纪律维护“文档先行”。
3. 入口新鲜度治理已扩大到 `docs/01-design/`、`docs/02-spec/` 和 `src/mlq/core/paths.py`，入口口径更新更完整。

## 影响

1. 下一步开启 `position` 正式卡时，可以直接在硬门禁约束下推进，不必担心再次滑回“先写代码、后补文档”。
2. 仓库入口文件与治理脚本的联动范围更清晰，后续改正式口径时更容易被发现。
3. 文档先行从原则表达升级为可执行门禁，仓库治理骨架基本成型。

## 门禁链路图

```mermaid
flowchart LR
    CARD[当前待施工卡] --> GATE[check_doc_first_gating]
    GATE --> MAIN[check_development_governance]
    MAIN --> FRESH[入口新鲜度检查]
```
