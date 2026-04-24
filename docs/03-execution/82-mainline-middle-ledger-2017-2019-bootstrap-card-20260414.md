# mainline middle-ledger 2017-2019 bootstrap 卡
`卡号`：`82`
`日期`：`2026-04-14`
`状态`：`待施工`

## 需求

- 问题：正式 middle-ledger 初始建设仍需继续向近年历史推进。
- 目标结果：完成 `2017-01-01 ~ 2019-12-31` 的中间库建库。
- 为什么现在做：这是第三段三年窗口，继续补齐真实主线历史覆盖。

## 设计输入

- 设计文档：`docs/01-design/modules/system/17-official-middle-ledger-phased-bootstrap-and-real-data-pilot-charter-20260414.md`
- 规格文档：`docs/02-spec/modules/system/17-official-middle-ledger-phased-bootstrap-and-real-data-pilot-spec-20260414.md`

## 任务分解

1. 完成 `2017-2019` canonical `malf` 建库。
2. 完成 `2017-2019` downstream 重跑。
3. 保留窗口级正式读数与异常挂账。

## 实现边界

- 范围内：`2017-2019` 中间库建库。
- 范围外：其他年份窗口与 `trade / system`。

## 历史账本约束

- 实体锚点：沿用正式实体锚点。
- 业务自然键：沿用正式自然键。
- 批量建仓：本卡仅覆盖 `2017-01-01 ~ 2019-12-31`。
- 增量更新：增量对齐仍保留给 `65`。
- 断点续跑：继续服从正式 queue/checkpoint/replay。
- 审计账本：正式 run summary 与 execution evidence / record / conclusion 共同审计。

## 收口标准

1. `2017-2019` 建库完成。
2. checkpoint / queue 读数可解释。
3. 无默认 bridge-v1 回退。
4. 证据、记录、结论闭环。

## 卡片结构图

```mermaid
flowchart LR
    W62["2017-2019"] --> M62["malf canonical"]
    M62 --> D62["structure/filter/alpha"]
```
