# position 资金管理与退出合同设计章程

日期：`2026-04-09`
状态：`生效中`

## 问题

新仓已经明确 `position` 是当前最接近正式业务施工的一层，但真正进入实现前还有三件事必须先写死：

1. 不能继续把所有资金管理方法混在一张宽表里，否则方法差异、参数边界和审计链会一起糊掉。
2. 不能把立花义正式的“测试仓 + 加码”照字面搬成浅层二分；它需要被收编成 `position` 内部的正式资金管理语义，而不是另起一个口头层次。
3. 不能再让 `trim_to_context_cap / final_allowed_position_weight / blocked candidate` 停留在公式或聊天里，必须先冻结为历史账本合同。

如果这些边界不先冻结，后续 `portfolio_plan / trade / system` 会继续缺少稳定上游，路线图里的“还没把握”也无法收口成正式事实。

## 设计输入

本章程建立在下面这些已验证来源之上：

1. `G:\MarketLifespan-Quant\docs\01-design\modules\position\00-position-charter-20260326.md`
2. `G:\MarketLifespan-Quant\docs\02-spec\modules\position\01-position-spec-20260326.md`
3. `G:\EmotionQuant-gamma\positioning\02-implementation-spec\01-positioning-baseline-and-sizing-spec-20260313.md`
4. `G:\EmotionQuant-gamma\positioning\02-implementation-spec\02-partial-exit-contract-spec-20260314.md`
5. `G:\MarketLifespan-Quant\docs\03-execution\81-position-risk-sizing-baseline-migration-conclusion-20260325.md`
6. `G:\MarketLifespan-Quant\docs\03-execution\82-position-partial-exit-contract-migration-conclusion-20260325.md`
7. `G:\MarketLifespan-Quant\docs\03-execution\291-position-long-only-max-position-contract-reset-card-20260407.md`
8. `G:\MarketLifespan-Quant\docs\03-execution\293-position-real-portfolio-capacity-and-total-cap-reset-card-20260407.md`
9. `G:\MarketLifespan-Quant\docs\03-execution\294-position-positive-weight-and-trim-path-bounded-acceptance-conclusion-20260407.md`
10. `F:\《股市浮沉二十载》\2012.(Japan)【立花义正】\你也能成为股票操作高手（立花义正）tw_ocr_results\`
11. `F:\《股市浮沉二十载》\2011.专业投机原理\专业投机原理_ocr_results\`
12. `F:\《股市浮沉二十载》\2020.(Au)LanceBeggs\YTC卷3：交易策略\YTC卷3：交易策略_ocr_results\`

## 设计目标

本轮只冻结三件正式事实：

1. `position` 在新系统里的主语到底是什么。
2. 资金管理、容量约束和退出合同应该如何分表。
3. 旧仓“测试仓 / 主仓 / 加码 / 减仓”这些经验，应该以什么正式语义进入新账本。

## 裁决一：`position` 的主语不是“测试仓 / 主仓”，而是单标的允许仓位账本

`position` 在新仓中的正式主语固定为：

`把 alpha formal signal 与上下文、当前持仓事实、资金管理方法、退出合同组合起来，产出单标的允许仓位与减仓/退出计划。`

因此它回答的是：

1. 这次能不能做
2. 最多可以做到多大
3. 如果当前已超出允许上限，应该减到哪里
4. 如果已经进入退出路径，该怎么表达退出腿

它不回答的是：

1. `alpha` 为什么触发
2. 组合层是否给配额
3. 实盘订单如何成交
4. 系统级组合读数如何展示

## 裁决二：资金管理必须采用“公共账本 + 方法分表”结构

本轮正式拒绝把所有资金管理方法继续塞进一张 `position_sizing_snapshot` 宽表。

新仓 `position` 的正式表族采用两层结构：

### 1. 公共账本层

所有方法共享、必须长期存在的事实固定为：

1. `position_run`
   - 批次、来源、审计元数据
2. `position_policy_registry`
   - 资金管理方法、退出方法、版本、启用状态
3. `position_candidate_audit`
   - admitted / blocked candidate 审计链
4. `position_capacity_snapshot`
   - 单票上限、组合剩余空间、上下文上限、当前持仓占用
5. `position_sizing_snapshot`
   - 最终允许权重、目标股数、需要减掉多少、动作裁决
6. `position_exit_plan`
   - 退出计划头
7. `position_exit_leg`
   - 退出腿明细

### 2. 方法分表层

所有真正带公式差异、参数差异和中间推导差异的资金管理方法，都必须进入：

`position_funding_<family>_snapshot`

命名模式下的独立分表，而不是继续把所有方法的特例列塞进公共账本。

当前 `v1` 先正式开放三类位置：

1. `position_funding_fixed_notional_snapshot`
   - 旧仓已验证的 operating control
2. `position_funding_single_lot_snapshot`
   - floor sanity control
3. 预留 `position_funding_probe_confirm_snapshot`
   - 用于吸收“试探建仓 / 确认加码”语义，但本轮只冻结位置，不立即启用

其他方法家族如 `fixed_risk / fixed_ratio / fixed_percentage / fixed_volatility / williams_fixed_risk` 允许后续继续开分表，但不再回到“一张总表兼容一切”的旧路。

## 裁决三：立花义正“测试仓 + 加码”进入 `entry_leg_role` 语义，而不是账户二分

本轮正式拒绝下面这种浅分法：

`测试仓一层、主仓一层，然后靠口头说明两者关系。`

新仓吸收这类经验的方式固定为：

1. 先把“试探建仓 / 确认加码 / 风险收缩 / 最终平仓”看成 `position` 内部的资金管理动作角色。
2. 这些角色进入共享账本字段 `entry_leg_role / position_action_decision / exit_reason_code`。
3. 它们如需保留公式与参数差异，再进入对应的资金管理分表。

当前冻结的动作角色是：

1. `base_entry`
   - 当前主线允许的正式开仓腿
2. `probe_entry`
   - 小额试探腿，仅作为正式预留语义
3. `confirm_add`
   - 顺势确认后扩大仓位的腿，仅作为正式预留语义
4. `protective_trim`
   - 因上下文上限或容量收缩而减仓
5. `closeout`
   - 因退出合同而清仓

当前 `v1` 只正式开放：

1. `base_entry`
2. `protective_trim`
3. `closeout`

`probe_entry / confirm_add` 已经有正式落点，但在 `trade carry` 与多腿开仓桥接冻结前，不进入默认主线。

同时明确拒绝：

1. 把补仓摊平写成默认语义
2. 把 martingale/越亏越加 当成正式候选
3. 在没有 open leg/carry 真相源之前，假装系统已经支持连续加码

## 裁决四：`run_id` 只做审计，历史主语必须由自然键表达

本轮固定下面这条历史账本纪律：

1. `run_id` 保留为批次与审计字段。
2. `position` 的历史主语不再由 `run_id` 决定，而由上游信号自然键、方法版本、参考交易日和动作角色共同决定。
3. blocked / admitted / trim / closeout 都必须在正式账本保留自然键可追溯记录，不能因为没成交或没放行就直接消失。

## 裁决五：`trim_to_context_cap`、`final_allowed_position_weight` 与 blocked audit 必须显式落账

新仓正式继承旧仓已经证明过的三条硬边界：

1. `final_allowed_position_weight` 必须显式下发，不能只停留在中间公式。
2. `trim_to_context_cap` 必须落成正式动作裁决，不能只在解释里存在。
3. blocked candidate 必须保留在 `position_candidate_audit`，让下游能解释“为什么没做”。

这三条属于公共账本事实，而不是某个资金管理方法的可选附属字段。

## 模块边界

### 范围内

1. 单标的仓位允许权与风险门控
2. 资金管理方法注册与方法分表
3. admitted / blocked 审计链
4. 减仓与退出计划表达

### 范围外

1. `alpha` trigger 发现
2. `portfolio_plan` 组合配额和组合容量协调
3. `trade` 订单、成交、持仓撮合
4. `system` 组合读数、报告与结果复用

## 当前迁移裁决

旧仓 `research_lab` 里的 `position` 研究语义，在新仓不再以“研究线数据库”作为正式落点，而是吸收到模块级历史账本 `position` 中。

也就是说，新仓的 `position` 不是临时实验区，而是正式的单标的仓位账本层。

## 一句话收口

`position` 在新仓里不是“测试仓 / 主仓”的口头分层，而是以单标的允许仓位为主语、以公共账本 + 方法分表为结构、以动作角色吸收试探/加码/减仓经验的正式历史账本模块。

## 流程图

```mermaid
flowchart LR
    SIG[alpha formal signal] --> FUND[funding management 资金管理]
    FUND --> SIZE[position sizing none价]
    SIZE --> EXIT[exit contract 退出合同]
    EXIT --> PP[portfolio_plan]
    SIZE --> AUDIT[blocked/trimmed audit]
