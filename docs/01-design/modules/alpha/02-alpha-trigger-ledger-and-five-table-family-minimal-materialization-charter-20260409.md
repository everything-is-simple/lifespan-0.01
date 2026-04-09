# alpha trigger ledger 与五表族最小物化设计章程
日期：`2026-04-09`
状态：`生效中`

## 问题

`11` 已经把 `structure_snapshot / filter_snapshot` 的最小官方上游站稳了，`alpha` 也已经有了面向下游的 `formal signal` 三表输出。
但当前 `alpha` 内部真正缺的，已经不再是 `position` 侧消费能力，而是“trigger 何时发生过”这一层还没有在新仓成为正式历史账本。

如果这里继续停在 `smoke / temp` 或研究态兼容表，会留下三类长期问题：

1. `alpha` 内部最常复用的中间事实无法在 `H:\Lifespan-data` 长期累积，后续仍会反复重算。
2. `formal signal` 虽然已经成立，但它引用的上游 `trigger` 事实仍然偏薄，无法稳定支撑后续复物化、审计和 selective rebuild。
3. 主线会被错误地诱导去回头补 `position`，或过早跳到 `trade / system`，而不是先把 `alpha` 自己的正式中间账本补厚。

## 设计输入

本章程建立在下面这些已冻结或已验证来源之上：

1. `docs/01-design/modules/alpha/00-alpha-module-lessons-20260409.md`
2. `docs/01-design/modules/alpha/01-alpha-formal-signal-output-charter-20260409.md`
3. `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
4. `docs/03-execution/11-structure-filter-formal-contract-and-minimal-snapshot-conclusion-20260409.md`
5. `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\04-pas-five-trigger-ledger-and-incremental-materialization-reset-20260408.md`
6. `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\05-pas-full-market-five-trigger-ledger-backfill-reset-20260408.md`
7. `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\06-pas-code-ledger-reset-and-2010-pilot-20260408.md`
8. `G:\EmotionQuant-gamma\gene\03-execution\09-phase-g6-bof-pb-cpb-conditioning-card-20260316.md`
9. `G:\EmotionQuant-gamma\gene\03-execution\16-phase-gx5-two-b-window-semantics-refactor-card-20260317.md`
10. `G:\EmotionQuant-gamma\gene\03-execution\17-phase-gx6-123-three-condition-refactor-card-20260317.md`

## 设计目标

本轮只冻结一件事：

把 `alpha` 内部最小正式中间账本往前推进一层，让 `trigger ledger` 与五表族共用的最核心事实层在新仓正式落库，并通过一次真实写入 `H:\Lifespan-data` 的 bounded pilot 证明 `reused / rematerialized` 已经成为正式能力。

## 裁决一：`trigger ledger` 必须先于回头扩 `position`，也先于直接跳到 `trade / system`

当前主线最自然的下一锤不是：

1. 回头再补 `position`
2. 直接开 `trade`
3. 直接开 `system`

而是先把 `alpha` 自己内部最常复用的正式中间事实层站稳。

原因是：

1. `position` 当前已经能消费官方 `alpha_formal_signal_event`
2. `trade / system` 当前尚未具备正式设计与账本入口
3. 真正仍然空着的，是 `alpha` 内部“发生过什么”的正式永续层

## 裁决二：本轮先冻结共享 ledger 层，不在同一张卡里宣称五家族全部完全独立落库

旧仓里的 `bof / tst / pb / cpb / bpb` 是 `alpha` 最核心的已验证遗产，但新仓当前最稳的推进方式不是一次性把五个 family 的全部细节表族都做满，而是先冻结它们共同共享、最常复用、最适合长期沉淀的一层正式 ledger：

1. `alpha_trigger_run`
2. `alpha_trigger_event`
3. `alpha_trigger_run_event`

五家族当前应先通过统一 contract 的：

1. `trigger_family`
2. `trigger_type`
3. `pattern_code`
4. 稳定自然键
5. bounded run 审计

进入正式历史账本。

这张卡不负责宣称：

1. 五个 family 的全部 detector 细节已经完全重构完毕
2. 五个 family 都已经拥有彼此完全独立的正式专表
3. `alpha` 内部所有 trace / observation / sidecar 全部已经正式化

## 裁决三：`trigger ledger` 与 `formal signal` 必须继续彻底分层

新仓继续沿袭已验证边界：

1. `trigger ledger` 回答“它何时发生过”
2. `formal signal` 回答“在当前官方准入口径下，它是否成立为可被下游消费的正式信号”

因此：

1. `alpha_trigger_event` 是中间事实层，不替代 `alpha_formal_signal_event`
2. `alpha_formal_signal_event` 继续作为 `position` 等下游的官方消费表
3. 下游不允许绕过 `formal signal` 去直接消费 `trigger ledger` 充当交易主语义

## 裁决四：正式 pilot 必须真实写入 `H:\Lifespan-data`

本轮最关键的边界不是“跑一次 smoke”，而是：

1. 正式 ledger 必须写入 `H:\Lifespan-data\alpha\alpha.duckdb`
2. `H:\Lifespan-temp` 只允许承载 summary、smoke 产物、临时导出和调试痕迹
3. 正式 pilot 必须以 bounded window 或 bounded instrument slice 形式在正式数据根留下可追溯事实

如果只把结果写到 `temp`，这张卡就没有完成它最关键的历史账本目标。

## 裁决五：复跑能力必须显式证明 `inserted / reused / rematerialized`

这张卡要证明的不是“能写入一次”，而是“正式写入后能被复用”。

因此本轮正式合同必须要求：

1. 初次物化时能记账 `inserted`
2. 重复运行命中相同事实时优先 `reused`
3. 上游官方上下文或 detector 合同变化导致事实需更新时显式记账 `rematerialized`

这也是“空间换时间”在新仓从设计原则进入正式运行能力的最小证据。

## 裁决六：本轮优先补 `alpha` 内部正式中间层，不顺手吞并后续模块职责

本轮范围固定为：

1. `alpha` 内部最小正式 trigger ledger
2. 五家族共享最小 contract
3. bounded pilot
4. rerun / reuse / rematerialize 验证

本轮不负责：

1. 回头扩 `position`
2. 正式开工 `trade / system`
3. 宣称 full-market 全历史 backfill 已完成
4. 把所有 family-specific payload 一次性做成最终形态

## 模块边界

### 范围内

1. `alpha_trigger_run / event / run_event` 三表正式化
2. 五家族共享最小字段组与自然键
3. 正式写入 `H:\Lifespan-data` 的 bounded pilot
4. `inserted / reused / rematerialized` 审计口径
5. `alpha_trigger_event -> alpha_formal_signal_event` 的正式上游关系

### 范围外

1. `position` 新 family 扩写
2. `portfolio_plan / trade / system` 正式 runner
3. 五家族全部细节表一次性补齐
4. 全市场全历史一次性正式回填

## 一句话收口

`12` 号卡要做的不是继续围着 `position` 打补丁，也不是提前跳下游，而是先把 `alpha` 自己最核心、最常复用的中间事实层沉淀成正式历史账本，让新仓第一次真正把“空间换时间”落到 `alpha` 内部。
