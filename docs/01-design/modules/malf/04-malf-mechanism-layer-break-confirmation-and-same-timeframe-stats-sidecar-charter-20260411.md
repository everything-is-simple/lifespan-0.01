# malf 机制层 break 确认与同级别统计 sidecar 设计宪章

日期：`2026-04-11`
状态：`生效中`

> 角色声明：本文冻结 `malf` 纯语义 core 之外、但仍服务 `structure / filter` 的机制层正式边界。
> `malf core` 仍以 `03-malf-pure-semantic-structure-ledger-charter-20260411.md` 为唯一权威入口；
> 本文只定义 `pivot-confirmed break` 与 `same-timeframe stats sidecar` 如何只读派生、如何被下游消费，以及它们为什么不能回写 `state`。

## 问题

`23` 号卡已经把 `malf core` 收缩到 `HH / HL / LL / LH / break / count`，并明确：

1. `break` 是触发，不是确认。
2. 同级别统计若保留，只能作为 sidecar。
3. 高周期 `context` 与动作接口不得回写 `malf core`。

但还有两件机制层问题没有正式落袋：

1. `break` 触发之后，到新的 `HH / LL` 推进确认之前，是否允许再定义一个“确认 break 已经站稳”的中间事实。
2. 同级别统计若不再属于 `malf core`，它的正式身份、实体、自然键和下游读取顺序是什么。

如果这一步继续悬空，会留下三个风险：

1. 下游会把 `pivot-confirmed break` 偷写成新的 `malf core` 原语或状态前提。
2. 同级别统计会再次被写回 `state`、`major_state` 或高周期背景解释。
3. `structure / filter` 会继续通过各自私有字段消费 `malf`，而不是围绕统一机制层 sidecar 对齐。

## 设计输入

1. `docs/01-design/modules/malf/02-malf-multi-timeframe-extreme-progression-ledger-charter-20260411.md`
2. `docs/01-design/modules/malf/03-malf-pure-semantic-structure-ledger-charter-20260411.md`
3. `docs/02-spec/modules/malf/02-malf-multi-timeframe-extreme-progression-ledger-spec-20260411.md`
4. `docs/02-spec/modules/malf/03-malf-pure-semantic-structure-ledger-spec-20260411.md`
5. `docs/01-design/modules/structure/01-structure-formal-snapshot-charter-20260409.md`
6. `docs/01-design/modules/filter/01-filter-formal-snapshot-charter-20260409.md`
7. `docs/03-execution/23-malf-pure-semantic-ledger-boundary-freeze-conclusion-20260411.md`

## 裁决

### 裁决一：`pivot-confirmed break` 不进入 `malf core` 原语层

`malf core` 的原语仍只允许是：

1. `HH`
2. `HL`
3. `LL`
4. `LH`
5. `break`
6. `count`

`pivot-confirmed break` 的正式身份固定为：

`由 core 事实派生出的机制层 break 确认事件`

它不是新的 core 原语，不重命名四态，也不改写：

1. `break` 的定义
2. `牛逆 / 熊逆` 的定义
3. “新推进才确认新顺状态”的硬规则

### 裁决二：break 的正式读取顺序冻结为三段，而不是两段

对任何一个 `instrument + timeframe`，正式读取顺序固定为：

1. `break trigger`
   - 最后有效 `HL / LH` 被破坏
   - 只回答“旧结构失效”
2. `pivot-confirmed break`
   - break 发生后，同级别修复 pivot 被确认，说明 break 不是单根 bar 的瞬时噪声
   - 只回答“break 已经获得 pivot 级确认”
3. `new progression confirmation`
   - 之后新的同向 `HH / LL` 推进出现
   - 才回答“新的顺状态成立”

其中第二段是机制层增强读法，不替代第三段的趋势确认职责。

### 裁决三：`pivot-confirmed break` 只负责提高 break 的可信度，不负责宣布新趋势成立

`pivot-confirmed break` 的正式语义必须钉死：

1. 它只能说明旧结构失效后的破坏过程已经完成一次同级别 pivot 级确认。
2. 它不能单独把 `BULL_COUNTER_TREND` 改写为 `BEAR_WITH_TREND`。
3. 它不能单独把 `BEAR_COUNTER_TREND` 改写为 `BULL_WITH_TREND`。

更硬的表达：

1. `break trigger` 回答“旧守护点失效没有”。
2. `pivot-confirmed break` 回答“这个 break 是否已经走到同级别 pivot 可确认的阶段”。
3. `new HH / LL` 才回答“新的顺趋势是否已经成立”。

### 裁决四：`pivot-confirmed break` 缺席时，不得为了凑机制层而篡改 core 状态机

快行情里可能出现：

1. 刚 break 就直接走出新的 `HH / LL`
2. 中间还来不及确认一个修复 pivot

这种情况下正式规则是：

1. `malf core` 仍按“新推进确认新顺状态”继续工作。
2. 机制层可以记为“被直接新推进超越”或保持缺席。
3. 不允许为了补造 `pivot-confirmed break` 而回头改写 `pivot / wave / state`。

### 裁决五：同级别统计只能作为只读 sidecar，不再属于 `malf core`

同级别统计的正式身份冻结为：

`same-timeframe stats sidecar`

它必须满足：

1. 只由同一 `timeframe` 的 `pivot / wave / state / progress` 派生。
2. 只回答当前位置、分位、耗尽风险、历史分布等读数。
3. 不得反向参与 `state`、`wave`、`break` 或 `count` 的计算。

因此：

1. 统计不是 `malf core`
2. 统计不是高周期背景
3. 统计不是动作接口

### 裁决六：同级别统计正式拆成“分布账本 + 时点快照”两层

为避免“既当样本池又当当前状态”的混写，正式拆分为两类实体：

1. `same_timeframe_stats_profile`
   - 记录某个 `universe + timeframe + regime_family + metric_name + sample_version` 的样本分布
2. `same_timeframe_stats_snapshot`
   - 记录某个 `instrument + timeframe + asof_bar_dt` 当前落在这些分布里的位置

一句话冻结：

`profile` 负责“样本池长什么样”，`snapshot` 负责“当前这一笔站在哪里”。

### 裁决七：`structure / filter` 对机制层能力的消费顺序必须分层

正式消费顺序固定为：

1. `structure`
   - 先读 `malf core` 或其 bridge v1 兼容输出
   - 可只读附加 `pivot-confirmed break` 与 `same-timeframe stats sidecar`
   - 但不得把 sidecar 反写成 `malf core`
2. `filter`
   - 优先读官方 `structure_snapshot`
   - 再读 `same-timeframe stats sidecar`
   - 不得绕过 `structure` 把 `malf` 内部过程重新当成自己的长期正式输入

### 裁决八：多级别联读仍不属于本卡

本卡只冻结：

1. `same-timeframe`
2. `read-only`
3. `mechanism layer`

明确不做：

1. `month -> week -> day` 背景传播重回 `malf core`
2. 跨级别混样本统计
3. `execution interface` 回潮

## 模块边界

### 范围内

1. `pivot-confirmed break` 的正式身份与读取顺序
2. `same-timeframe stats sidecar` 的正式身份与分层
3. `malf -> structure -> filter` 的只读消费边界

### 范围外

1. 新 canonical runner 代码实现
2. `alpha / position / trade` 的消费细节
3. 多级别背景系统
4. 胜率、收益、动作建议

## 一句话收口

`malf core` 继续只负责“结构真相”，而 `pivot-confirmed break` 与 `same-timeframe stats sidecar` 只负责“机制层确认”和“同级别位置读数”；两者都必须只读派生、只读消费，不能倒灌回 `state`。`
