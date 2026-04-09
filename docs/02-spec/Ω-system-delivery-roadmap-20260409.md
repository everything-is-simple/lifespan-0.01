# 系统级总路线图

日期：`2026-04-09`
状态：`生效中`

## 当前进度

当前新仓已经完成的是地基阶段，正准备进入第一个真正的业务层施工阶段。

截至今天，已正式成立的部分是：

1. 五根目录契约
2. 历史账本共享契约
3. 文档先行硬门禁
4. 模块级边界与踩坑经验冻结
5. 执行闭环与治理脚本

当前整体判断：

- 系统阶段位于 `P0 已完成，P1/P2 已有边界，P3-P6 尚未正式展开`
- 当前最接近真正进入业务施工的是 `position`

## 老仓来源分层

当前路线图背后的老仓来源，正式按下面四层理解：

### `L1 核心已验证模块`

1. `position`
2. `alpha`
3. `malf`

这些模块在老仓里同时具备较扎实的设计、规格、执行卡与结论，是新系统最值得吸收的核心资产。

### `L2 支持性较强模块`

1. `data`
2. `system`

这两层有大量主线桥接、验收、修复与 readout 经验，可以直接影响新系统推进顺序，但不能简单照搬旧命名和旧表结构。

### `L3 研究偏少模块`

1. `trade`
2. `core`

这两层已有部分正式经验，但更多仍停留在桥接合同、治理抽象或局部结论，正式账本沉淀还不够厚。

### `L4 新系统正式新建边界`

1. `structure`
2. `filter`
3. `portfolio_plan`

这三层当前更多是“吸收旧边界想法后在新系统正式建立”，而不是已有整套可直接沿袭的老仓成品。

## 系统阶段

### `P0 治理地基`

状态：`已完成`

范围：

1. 五根目录
2. 共享账本契约
3. 文档先行
4. 执行闭环
5. 最小治理脚本

### `P1 数据依据层`

状态：`局部可验`

范围：

1. `raw_market`
2. `market_base`
3. readiness / freshness / targeted repair

### `P2 市场语义层`

状态：`设计中`

范围：

1. `malf`
2. `structure`
3. `filter`

### `P3 alpha 触发层`

状态：`设计中`

范围：

1. `alpha`
2. PAS 五表族
3. trigger ledger 与 formal signal 分层

### `P4 仓位与组合层`

状态：`设计中`

范围：

1. `position`
2. `portfolio_plan`

### `P5 交易执行层`

状态：`未开始`

范围：

1. `trade`
2. `trade_runtime`
3. carry / entry / exit / replay

### `P6 system 总装层`

状态：`未开始`

范围：

1. `system`
2. 组合读数
3. 结果复用
4. 总装验证

## 各模块状态

| 模块 | 当前状态 | 主要来源 | 继承方式 | 置信度 | 下一步重点 |
| --- | --- | --- | --- | --- | --- |
| `core` | `主线待接` | `G:\MarketLifespan-Quant\docs\01-design\modules\core\` 与 `02-spec\modules\core\` | `只吸收经验` | `中` | ownership / checkpoint / version registry |
| `data` | `局部可验` | `G:\MarketLifespan-Quant\docs\01-design\modules\data\`、`02-spec\modules\data\`、`03-execution\` | `沿袭为主` | `高` | readiness / freshness / targeted repair |
| `malf` | `设计中` | `G:\EmotionQuant-gamma\gene\` + `G:\MarketLifespan-Quant\docs\01-design\modules\malf\` + `02-spec\modules\malf\` | `沿袭后改写` | `高` | 语义表族与 `structure/filter` 新边界一起冻结 |
| `structure` | `设计中` | `G:\Lifespan-Quant\docs\01-design\modules\structure\` + 旧 `malf 29/30/31` 分层材料 | `全新设计` | `低` | 冻结 `structure_event / structure_snapshot` |
| `filter` | `设计中` | `G:\Lifespan-Quant\docs\01-design\modules\filter\` + 旧 `malf 29/31/32` 分层材料 | `全新设计` | `低` | 冻结最小硬门与 observation 分层 |
| `alpha` | `设计中` | `G:\EmotionQuant-gamma\normandy\` + `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\` + `02-spec\modules\alpha\` | `沿袭后改写` | `高` | PAS 五表族、trigger ledger 与 formal signal 分层 |
| `position` | `设计中` | `G:\EmotionQuant-gamma\positioning\` + `G:\MarketLifespan-Quant\docs\01-design\modules\position\` + `02-spec\modules\position\` | `沿袭后改写` | `高` | 资金管理分表与单标的仓位账本 |
| `portfolio_plan` | `设计中` | 旧 `position / system` 桥接经验与组合验收材料 | `全新设计` | `低` | 组合容量、配额、blocked/admitted 合同 |
| `trade` | `未开始` | `G:\MarketLifespan-Quant\docs\01-design\modules\trade\` + `02-spec\modules\trade\` + 桥接结论 | `只吸收经验` | `低` | entry / carry / exit / replay 账本 |
| `system` | `未开始` | `G:\MarketLifespan-Quant\docs\01-design\modules\system\` + `02-spec\modules\system\` + bounded acceptance 结论 | `只吸收经验` | `中` | 系统级 readout / reuse / audit |

## 下一锤

当前下一锤建议固定为：

### `position 资金管理账本分表设计`

原因：

1. `position` 是当前距离正式业务实现最近的一层。
2. 你已经明确要把资金管理方式拆表，而不是继续混在一张大表里。
3. 如果 `position` 不先冻结，`portfolio_plan / trade / system` 都会缺少稳定上游。
4. `position` 是当前老仓来源最扎实、最适合首先迁入新系统正式账本的一层。

下一锤产物：

1. `position` 正式设计文档
2. `position` 正式规格文档
3. `position` 第一张执行卡

优先内容：

1. 资金管理方式表族
2. 自然键
3. 增量更新策略
4. audit candidate / blocked candidate 保留规则
5. 立花义正“测试仓 + 加码”正式方案位置

## 阻塞项

### `阻塞 1：position 表族尚未冻结`

影响：

- `portfolio_plan` 无法定义组合层如何消费 `position`
- `trade` 无法稳定消费允许仓位合同

### `阻塞 2：alpha 五表族尚未正式落库`

影响：

- `position` 下游虽然可以先设计，但最终上游 formal signal 还不稳定

### `阻塞 3：structure / filter 仍只有边界，没有正式表合同`

影响：

- `alpha` 的结构语义与准入层还缺正式可消费出口

## 当前不敢写死的点

1. `position` 资金管理表族的最终拆分与自然键仍未冻结，尤其是立花义正“测试仓 + 加码”应落在哪一层还需要正式设计。
2. `alpha` 的 `bof / tst / pb / cpb / bpb` 五表族虽然方向明确，但正式桥接到 `position` 的字段合同还未写死。
3. `malf` 在新系统中已经拆成 `malf -> structure -> filter` 主链，但这三层的正式表合同仍未共同冻结。
4. `trade` 的 `entry / carry / exit / replay` 账本边界只停留在经验层，尚未成为可施工的正式设计。
5. `portfolio_plan` 当前主要来自旧 `position / system` 桥接经验外推，仍不是可直接沿袭的现成模块。

## 里程碑定义

### `M0 地基完成`

判定条件：

1. 五根目录成立
2. 共享账本契约成立
3. 文档先行硬门禁成立
4. 执行闭环成立

当前状态：`已完成`

### `M1 position 正式开工`

判定条件：

1. `position` design/spec/card 完成
2. 资金管理表族冻结
3. 自然键与 audit 规则冻结

当前状态：`未完成`

### `M2 alpha-position 正式桥接成立`

判定条件：

1. `alpha` formal signal 有正式账本出口
2. `position` 能正式消费 `alpha`
3. 有 bounded evidence

当前状态：`未完成`

### `M3 position-portfolio-trade 桥接成立`

判定条件：

1. `portfolio_plan` 有正式容量账本
2. `trade_runtime` 有 entry / carry / exit / replay 账本
3. bounded mainline 可复验

当前状态：`未完成`

### `M4 system 主线可复验`

判定条件：

1. `data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade -> system` 形成系统级读数
2. 有可复验 evidence / record / conclusion
3. 能解释 blocked / admitted / carry / filled

当前状态：`未完成`

## 使用方式

以后你想快速了解系统推进位置，优先看这一份文档。

如果你想继续正式施工，再按下面顺序下钻：

1. 看当前“下一锤”
2. 看对应模块在本页的“主要来源 / 继承方式 / 置信度”
3. 打开对应模块设计文档
4. 打开当前待施工卡
5. 再进入具体实现
