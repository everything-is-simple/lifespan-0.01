# structure 正式 snapshot 设计章程

日期：`2026-04-09`
状态：`生效中`

## 问题

`alpha -> position` 已经收口，但更上游仍缺一层稳定的官方结构事实出口。当前很多 `new_high / new_low / failed_extreme / density` 一类结构语义，仍散落在旧 `phenomenon / scene` 兼容字段里，没有形成可被 `filter / alpha` 长期消费的正式 `structure` 层。

如果这一步继续悬空，会留下三个问题：

1. `filter` 仍然只能寄生在旧兼容字段之上，无法形成独立正式准入层。
2. `alpha` 后续要继续补 trigger ledger 与五表族时，仍会被迫回读旧 `malf` 兼容结构。
3. 下游看上去已经有 `structure` 模块，但实际上没有官方结构账本出口。

## 设计输入

1. `docs/01-design/modules/malf/00-malf-module-lessons-20260409.md`
2. `docs/01-design/modules/structure/00-structure-module-lessons-20260409.md`
3. `G:\MarketLifespan-Quant\docs\01-design\modules\malf\29-malf-two-axis-lifecycle-and-structure-filter-separation-charter-20260407.md`
4. `G:\MarketLifespan-Quant\docs\02-spec\modules\malf\29-malf-two-axis-lifecycle-and-structure-filter-separation-spec-20260407.md`
5. `G:\MarketLifespan-Quant\docs\01-design\modules\malf\31-malf-filter-layer-and-downstream-structure-consumption-charter-20260407.md`
6. `docs/03-execution/10-alpha-formal-signal-contract-and-producer-conclusion-20260409.md`

## 裁决

### 裁决一：`structure` 只回答“发生了什么结构事实”

`structure` 的官方身份固定为：

1. 从 `malf` 语义层中外提稳定、可复查、可增量沉淀的结构事实。
2. 回答“当前这段中级波内部发生了什么结构推进或失败事实”。
3. 不承担 pre-trigger 准入，也不承担 trigger 检测或 formal signal 判定。

### 裁决二：本轮先冻结最小官方出口，不一次性吞完全部结构家族

本轮只先冻结最小正式输出：

1. `structure_run`
2. `structure_snapshot`
3. `structure_run_snapshot`

先把 `snapshot` 事实层站稳，再讨论更细的 `event / trace / study sidecar`。

### 裁决三：结构事实必须优先脱离旧 `phenomenon / scene` 兼容入口

当前允许沿袭的候选事实包括：

1. `new_high_count`
2. `new_low_count`
3. `refresh_density`
4. `advancement_density`
5. `is_failed_extreme`
6. `failure_type`

但这些字段在新仓里不再继续以“旧兼容字段集合”的方式被下游直接消费，而要收敛进官方 `structure_snapshot`。

### 裁决四：`run_id` 继续只承担审计职责

`structure_snapshot` 必须按自然键累积，`run_id` 只记录：

1. 本次 bounded 物化的窗口
2. 输入来源与版本
3. 本次触达了哪些 snapshot

不能再把结构历史语义绑回单次 run。

## 模块边界

### 范围内

1. `structure` 的正式身份
2. 最小 `structure_snapshot` 官方输出
3. `structure -> filter / alpha` 的消费优先级

### 范围外

1. `filter` 的正式准入规则细化
2. `alpha` 五表族与 detector 私有解释
3. `position / trade / system` 的直接消费改造

## 一句话收口

`structure` 下一步不是继续把结构语义埋在旧 `malf` 兼容字段里，而是先把最小官方 `snapshot` 出口冻结出来，让 `filter / alpha` 真正有稳定上游可接。`
