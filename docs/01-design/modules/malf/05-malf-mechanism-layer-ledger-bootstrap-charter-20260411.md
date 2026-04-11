# malf 机制层 sidecar 账本 bootstrap 设计宪章

日期：`2026-04-11`
状态：`生效中`

> 角色声明：本文定义 `pivot_confirmed_break_ledger / same_timeframe_stats_profile / same_timeframe_stats_snapshot` 的正式实现入口。
> 它不改写 `03` 号 pure semantic core，也不替代 `04` 号机制层边界文档；它只回答“在当前 bridge v1 仍存在的前提下，如何把机制层能力正式落成可增量、可续跑、可审计的历史账本”。

## 问题

`24` 号卡已经冻结了机制层边界，但当前仓内仍缺三类正式实现：

1. `malf` 账本里没有 `pivot_confirmed_break_ledger / same_timeframe_stats_profile / same_timeframe_stats_snapshot`
2. 没有正式 bounded runner 负责批量建仓、增量续跑与 checkpoint
3. `structure / filter` 虽已获得只读 sidecar 消费资格，但还没有任何可被 runner 真正读取的正式 sidecar 表

如果继续停在文档层，会留下三个问题：

1. 机制层能力只能停留在结论，无法物化成历史账本
2. 下游只能继续靠 `bridge v1` 单独判断，无法验证 sidecar 合同
3. 未来切 pure semantic canonical runner 时，缺少可 replay 的机制层过渡账本

## 设计输入

1. `docs/01-design/modules/malf/03-malf-pure-semantic-structure-ledger-charter-20260411.md`
2. `docs/01-design/modules/malf/04-malf-mechanism-layer-break-confirmation-and-same-timeframe-stats-sidecar-charter-20260411.md`
3. `docs/02-spec/modules/malf/03-malf-pure-semantic-structure-ledger-spec-20260411.md`
4. `docs/02-spec/modules/malf/04-malf-mechanism-layer-break-confirmation-and-same-timeframe-stats-sidecar-spec-20260411.md`
5. `docs/01-design/modules/structure/01-structure-formal-snapshot-charter-20260409.md`
6. `docs/01-design/modules/filter/01-filter-formal-snapshot-charter-20260409.md`
7. `docs/03-execution/24-malf-mechanism-layer-break-confirmation-and-stats-sidecar-conclusion-20260411.md`

## 裁决

### 裁决一：本轮实现采用“bridge v1 驱动的机制层 bootstrap”，不伪称 pure semantic canonical runner 已落地

当前正式实现入口固定为：

`market_base(backward) -> bridge v1 malf snapshots -> mechanism sidecar ledgers`

更具体地说：

1. `pas_context_snapshot`
   - 继续提供过渡语义背景与状态标签
2. `structure_candidate_snapshot`
   - 继续提供最小结构候选事实
3. 新机制层 runner
   - 在此基础上物化 `pivot_confirmed_break_ledger / same_timeframe_stats_profile / same_timeframe_stats_snapshot`

这只是 bridge v1 时代的正式 bootstrap，不等价于：

1. pure semantic canonical pivots 已存在
2. 机制层已经切到 pure semantic core 直接派生

### 裁决二：新增四表实现族，显式把 run 与 checkpoint 纳入正式账本

本轮正式实现最小表族固定为：

1. `malf_mechanism_run`
2. `malf_mechanism_checkpoint`
3. `pivot_confirmed_break_ledger`
4. `same_timeframe_stats_profile`
5. `same_timeframe_stats_snapshot`

其中：

1. `run`
   - 记录一次 bounded 物化运行
2. `checkpoint`
   - 记录 `instrument + timeframe` 的续跑位置
3. `break ledger`
   - 记录 break 机制层确认事实
4. `stats profile`
   - 记录分布样本池
5. `stats snapshot`
   - 记录当前时点所处位置

### 裁决三：checkpoint 只服务续跑，不替代业务自然键

checkpoint 的正式职责固定为：

1. 记住某个 `instrument + timeframe` 已处理到哪里
2. 支持断点续跑与 bounded replay

它不能替代：

1. `pivot_confirmed_break_ledger` 的自然键
2. `same_timeframe_stats_snapshot` 的自然键
3. `same_timeframe_stats_profile` 的自然键

### 裁决四：同级别统计先从 bridge v1 可稳定提供的四类指标起步

在 pure semantic canonical core 尚未实现前，本轮 stats sidecar 只正式统计 bridge v1 已稳定提供的：

1. `new_high_count`
2. `new_low_count`
3. `refresh_density`
4. `advancement_density`

这四类指标足以先建立：

1. 分位
2. bucket
3. snapshot 位置读数

但不宣称：

1. 已覆盖 canonical `wave duration / pullback depth`
2. 已覆盖 pure semantic `HH/LL` 全部分布

### 裁决五：`pivot_confirmed_break_ledger` 本轮以 bridge-compatible 规则落地，但不得反写 core

由于当前仓内还没有 canonical `HL / LH pivot ledger`，本轮 break 确认实现采用 bridge-compatible 判定：

1. 先识别 break 触发窗口
2. 再用后续同级别连续 bar / 候选结构稳定性确认 break 没有立即失效
3. 将确认结果沉淀为机制层只读账本

它的正式身份仍然是：

1. 机制层确认事实
2. 过渡实现

不是：

1. `malf core`
2. 新趋势确认条件

### 裁决六：下游接入先做“可选读取 + 显式落表”，不直接改写硬判断

本轮 `structure / filter` 的最小接入策略固定为：

1. `structure`
   - 可选读取 `pivot_confirmed_break_ledger / same_timeframe_stats_snapshot`
   - 将读到的 sidecar 审计引用和只读 bucket 落表
   - 不改写当前 `structure_progress_state` 硬逻辑
2. `filter`
   - 可选读取 `structure` 已落下来的 sidecar 读数
   - 把关键 bucket 作为 admission notes 或只读辅助字段
   - 不用 sidecar 覆盖现有硬阻断条件

### 裁决七：25 号卡的交付目标是“正式可跑的 bridge-era sidecar bootstrap”，不是终局语义

本卡收口后成立的正式事实是：

1. 机制层 sidecar 已有正式账本
2. 机制层 sidecar 已有正式 bounded runner 与 checkpoint
3. 下游已具备最小只读接入

本卡不声称：

1. pure semantic canonical runner 已完成
2. 机制层算法已经终局化
3. bridge v1 可以移除

## 模块边界

### 范围内

1. `malf` 新增机制层表族、run、checkpoint
2. `scripts/malf` 新增正式机制层 runner 入口
3. `structure / filter` 最小只读 sidecar 接入
4. 单元测试、bounded 证据与执行闭环

### 范围外

1. `alpha / position / trade` 的 sidecar 消费改造
2. pure semantic canonical core 实现
3. 多级别背景系统
4. 动作接口

## 一句话收口

`25` 号实现卡的目标不是提前宣布 pure semantic core 已落地，而是先把 bridge-era 的机制层能力正式物化成可增量、可续跑、可审计的 sidecar 历史账本，并把它们最小接入到 `structure / filter`。`
