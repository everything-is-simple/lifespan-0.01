# mainline middle-ledger 2011-2013 bootstrap 卡
`卡号`：`60`
`日期`：`2026-04-14`
`状态`：`待施工`

## 需求

- 问题：`2010` pilot 通过后，正式库仍只覆盖 pilot 年份，不能代表主线中间库初始建设已完成。
- 目标结果：完成 `2011-01-01 ~ 2013-12-31` 的 middle-ledger 正式建库。
- 为什么现在做：这是三年一段正式建库节奏的第一段历史窗口。

## 设计输入

- 设计文档：`docs/01-design/modules/system/17-official-middle-ledger-phased-bootstrap-and-real-data-pilot-charter-20260414.md`
- 规格文档：`docs/02-spec/modules/system/17-official-middle-ledger-phased-bootstrap-and-real-data-pilot-spec-20260414.md`

## 任务分解

1. 在 `2011-01-01 ~ 2013-12-31` 窗口执行 `malf canonical`。
2. 在同窗口重跑 `structure / filter / alpha`。
3. 留下窗口级 row-count / scope-count / checkpoint 摘要。

## 实现边界

- 范围内：`2011-2013` 中间库建库。
- 范围外：其他年份窗口与 `trade / system`。

## 历史账本约束

- 实体锚点：沿用 `malf -> alpha` 既有正式实体锚点。
- 业务自然键：沿用各模块既有正式自然键。
- 批量建仓：本卡仅覆盖 `2011-01-01 ~ 2013-12-31`。
- 增量更新：增量对齐职责保留给 `65`，本卡只完成历史窗口 bootstrap。
- 断点续跑：继续沿用正式 queue/checkpoint/replay，不允许退化为单次全量脚本。
- 审计账本：正式 run summary 与 execution evidence / record / conclusion 共同审计。

## 收口标准

1. `2011-2013` 窗口建库完成。
2. canonical 与 downstream 正式 run 可追溯。
3. 无默认 bridge-v1 回退。
4. 证据、记录、结论闭环。

## 卡片结构图

```mermaid
flowchart LR
    W60["2011-2013"] --> M60["malf canonical"]
    M60 --> D60["structure/filter/alpha"]
```
