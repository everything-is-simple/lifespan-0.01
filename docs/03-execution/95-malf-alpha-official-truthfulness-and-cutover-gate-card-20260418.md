# malf alpha 官方真值与 cutover gate

`卡号`：`95`
`日期`：`2026-04-18`
`状态`：`草稿`

## 需求

- 问题：即使 `79 -> 80 -> 91 -> 92 -> 93 -> 94` 都完成，也仍需要正式裁决新 `malf -> alpha` 主链是否已经成为官方默认口径。
- 目标结果：给出以下官方 cutover 裁决，并决定是否恢复 `100-105`：
  - `malf_day / week / month` 已完成全覆盖
  - `structure_day / week / month` 已完成 `2010-01-01` 至当前 official `market_base` 覆盖尾部 bounded replay
  - `filter_day` 已完成 `2010-01-01` 至当前 official `market_base` 覆盖尾部 bounded replay
  - `alpha` 五 PAS 日线库已完成 `2010-01-01` 至当前 official `market_base` 覆盖尾部 bounded replay
- 为什么现在做：没有这张 gate，前面所有卡都只是“代码与库已支持”，不是“官方默认口径已切换”。

## 设计输入

- 设计文档：`docs/01-design/modules/system/18-malf-alpha-dual-axis-and-timeframe-native-refactor-charter-20260418.md`
- 规格文档：`docs/02-spec/modules/system/18-malf-alpha-dual-axis-and-timeframe-native-refactor-spec-20260418.md`

## 层级归属

- 主层：`malf -> alpha` 官方 truthfulness / cutover gate
- 次层：`100-105` 恢复放行入口
- 上游输入：`79`、`80`、`91`、`92`、`93`、`94` 的 official run summary、evidence、record 与对应结论页
- 下游放行：接受则恢复 `100-105`；拒绝则继续补开 upstream 卡
- 本卡职责：把 `malf` 全覆盖与 downstream bounded replay 分开审计，并给出是否切换官方默认口径的正式裁决

## 任务分解

1. 审计 `malf_day / week / month` 的全覆盖、checkpoint、row/scope、date-range 与 freshness。
2. 审计 `structure_day / week / month` 是否已默认绑定对应 `malf_*` 并完成 `2010-01-01` 至当前 official `market_base` 覆盖尾部 bounded replay。
3. 审计 `filter_day` 是否已稳定承担 objective gate + note sidecar，且五类 hard block 已标准化为稳定 `reject_reason_code`。
4. 审计 `alpha` 五 PAS 日线库是否已默认绑定新口径，并确认没有扩成 trigger-level `D/W/M` 三套账本，且 bounded replay 范围只围绕 `2010-01-01` 至当前 official `market_base` 覆盖尾部。
5. 裁决是否允许恢复 `100-105`，或继续补开 upstream 卡。

## 实现边界

- 范围内：`malf -> alpha` 官方 truthfulness、cutover gate 与 `100-105` 放行裁决。
- 范围外：本卡不再继续重构 `trade / system`，也不做 bridge-era 物理删表。

## 历史账本约束

- 实体锚点：沿用既有模块正式锚点。
- 业务自然键：沿用既有自然键，本卡只做审计与裁决，不新造业务主键。
- 批量建仓：本卡不新建大规模账本，只审计 `79`、`80`、`91`、`92`、`93`、`94` 的结果。
- 增量更新：审计当前增量链是否已稳定落在新口径上。
- 断点续跑：本卡只审计 queue/checkpoint/freshness 是否闭环。
- 审计账本：`evidence / record / conclusion` 与 official run summary 共同构成本卡审计输入。

## 正式设计清单

| 设计项 | 正式口径 | 不接受情形 |
| --- | --- | --- |
| `malf` 审计 | 单独审计 `malf_day / week / month` 全覆盖、date-range、scope 与 freshness | 把 `malf` 混进 downstream replay 一起糊过去 |
| `structure` 审计 | 审计 `structure_day / week / month` 绑定对应 `malf_*` 且 replay 完成 | 只审 day 或不核对绑定关系 |
| `filter_day` 审计 | 审计 `filter_day` 的 objective gate / note sidecar 边界与五类 `reject_reason_code` | 只看“有库有表”，不看 gate 语义 |
| `alpha` 审计 | 审计五 PAS 日线库默认口径、`owner/reason/audit_note` 与“不做 `5 × 3`”边界 | 只看代码支持，不看默认真值落点 |
| 放行语义 | `95` 接受才允许恢复 `100-105`；否则继续 upstream 补卡 | 口径未定时先恢复 `100-105` |
| 审计输入 | 必须由 official run summary + evidence / record / conclusion 共同支撑 | 只有单一日志或口头说明 |

## 实施清单

| 切片 | 实施内容 | 交付物 |
| --- | --- | --- |
| 切片 1 | 汇总 `79`、`80`、`91`、`92`、`93`、`94` official run summary、证据与记录 | 审计输入包 |
| 切片 2 | 分别审计 `malf` 全覆盖与 `structure/filter/alpha` bounded replay | 审计摘要 |
| 切片 3 | 核对 `filter_day` 五类 gate 与 `alpha` 五 PAS 主权边界 | 差异说明 |
| 切片 4 | 形成 `accept / reject / reopen upstream cards` 裁决 | `95` 结论 |
| 切片 5 | 回填 `95` evidence / record / indexes，并同步 `100-105` 放行状态 | execution 闭环 |

## A 级判定表

| 判定项 | A 级通过标准 | 阻断条件 | 对下游影响 |
| --- | --- | --- | --- |
| `malf` 全覆盖 | `malf_day / week / month` 全覆盖且审计摘要完整 | 任一库覆盖不完整或无 freshness | `100-105` 不可放行 |
| `structure` 绑定 | `structure_day / week / month` 默认绑定对应 `malf_*` 且 replay 完成 | 绑定关系不稳或缺 replay | `alpha` 上下文不可信 |
| `filter_day` gate | 五类 objective gate 与 note sidecar 边界稳定 | gate 语义仍漂移 | `alpha` verdict 输入不稳 |
| `alpha` 五 PAS | 五 PAS 日线库成为默认真值落点，且未膨胀成 `5 × 3` | 仍是单库或 trigger-level `D/W/M` | 下游 authority 混乱 |
| 放行裁决 | `100-105` 恢复或阻断有明确正式裁决 | gate 结论模糊 | 下游施工顺序失控 |

## 收口标准

1. `malf_day / week / month` 成为默认官方 `malf` 库，且已全覆盖。
2. `structure_day / week / month`、`filter_day` 与 `alpha` 五 PAS 日线库默认绑定新双主轴口径。
3. `structure / filter / alpha` 的 bounded replay 与 `malf` 的全覆盖被分别审计，而不是混写成一条完成度。
4. `100-105` 是否恢复有明确裁决。
5. `evidence / record / conclusion` 闭环。

## 卡片结构图

```mermaid
flowchart LR
    C79["79 malf 路径/表族"] --> C80["80 0/1 波段过滤边界"]
    C80 --> C91["91 malf 全覆盖"]
    C91 --> C92["92 structure D/W/M"]
    C92 --> C93["93 filter_day"]
    C93 --> C94["94 alpha five PAS"]
    C94 --> G95["95 cutover gate"]
    G95 --> C100["allow 100"]
    G95 --> BLOCK["open more upstream cards"]
```




