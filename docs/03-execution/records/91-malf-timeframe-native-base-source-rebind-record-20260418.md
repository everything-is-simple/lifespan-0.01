# malf 权威设计锚点补齐与 timeframe native base source 重绑收口 记录

`记录编号`：`91`
`日期`：`2026-04-19`

## 做了什么

1. 保留 `91` 原有的 native source / full coverage 职责，不额外拆卡。
2. 在 `docs/01-design/modules/malf/` 与 `docs/02-spec/modules/malf/` 下新增 `15-*` 文档，作为当前 `malf` 单点权威设计/规格。
3. 把 `91` 的设计输入、结论与证据改挂到 `modules/malf/15`，不再只依赖系统级 `18` 去代替 `malf` 本体说明。
4. 同步刷新 execution reading order、card catalog、README、AGENTS 与路线图引用，让用户能直接从入口文件和执行索引跳到这对权威文档。

## 偏离项

- 本次没有新增新的物理“81 文件号”。当前执行体系仍保持“逻辑 `81` 对应物理 `91-*` 文件”的既有编号策略，避免把当前待施工位和后续 `92-95` 整体打乱。

## 备注

- 这次补的是“权威锚点缺口”，不是新一轮 `malf` 算法修订。
- `80` 已冻结 `0/1` 问题的统一审计边界；`91` 现在补齐的是“当前 `malf` 到底是什么”的权威总设计/总规格。
- 当前正式待施工位保持为 `92`。

## 记录结构图

```mermaid
flowchart LR
    GAP["malf 权威设计缺口"] --> DOC15["modules/malf/15 authority docs"]
    DOC15 --> CARD91["91 card bundle"]
    CARD91 --> INDEX["reading order / entry files"]
    INDEX --> NEXT["92 current active card"]
```
