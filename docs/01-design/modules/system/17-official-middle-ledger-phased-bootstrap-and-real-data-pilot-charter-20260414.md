# official middle-ledger phased bootstrap and real-data pilot 设计宪章

日期：`2026-04-14`
状态：`待执行`

## 背景

`29 / 30 / 31 / 32` 已在代码、单测和执行结论中冻结了 canonical `malf v2` 与 downstream rebind 的正式口径，但 `H:\Lifespan-data` 下的真实正式库仍停留在 bridge-v1 落地状态。

如果不把这条差距单独施工，就会长期出现三套互相错位的“主线”：

1. 代码默认主线是 canonical `malf v2`
2. 单测回归主线是 canonical `malf v2`
3. 真实正式库主线仍是 bridge-v1

这会直接削弱后续 `100-105` 的真实性，因为执行侧恢复建立在真实正式库已经完成 canonical 落地之上，而不是建立在“代码本来就支持”之上。

## 设计目标

新增一组位于 `55` 与 `100` 之间的正式卡组，把 canonical `malf v2` 及其 downstream middle ledger 正式落地到 `H:\Lifespan-data`：

1. 先用 `2010-01-01 ~ 2010-12-31` 做 bounded official pilot
2. pilot 通过后，再按三年一段推进 `2011-2013 / 2014-2016 / 2017-2019 / 2020-2022 / 2023-2025`
3. 最后对齐 `2026` 当年增量，形成到当前年份为止的正式 middle-ledger 初始建设

## middle-ledger 范围

本组卡默认只覆盖真实正式库中的 middle-ledger 主线落地：

- `malf canonical`
- `structure`
- `filter`
- `alpha`

其中：

- `data` 只作为官方上游事实来源，不在本组卡重写其正式合同
- `position / portfolio_plan` 只允许在 gate 卡中做只读 acceptance spot-check，不在本组卡内重做大规模 bootstrap
- `trade / system` 仍保留到 `100-105`

## 设计原则

### 1. 先 bounded pilot，再分段建库

不能在没有 pilot 证据的前提下，直接对全历史正式库做一次性 canonical 切换。

第一段窗口固定为：

- `2010-01-01 ~ 2010-12-31`

pilot 通过后，才允许进入三年一段的建库卡。

### 2. 正式库路径优先

本组卡所有正式写入都必须落到：

- `H:\Lifespan-data\malf\malf.duckdb`
- `H:\Lifespan-data\structure\structure.duckdb`
- `H:\Lifespan-data\filter\filter.duckdb`
- `H:\Lifespan-data\alpha\alpha.duckdb`

`H:\Lifespan-temp` 只允许存放中间缓存、pytest basetemp、临时导出与 smoke 过程文件。

### 3. bridge-v1 先降级职责，不立即删表

本组卡期间：

- bridge-v1 表可以继续并存
- bridge-v1 不得再被声明为默认正式主线上游
- 删除 bridge-v1 表或彻底移除兼容分支，不属于 pilot 卡的强制目标

先把默认主线切成 canonical，再决定是否做兼容清理。

### 4. 每一段都必须可复验

每张窗口卡都必须显式回答：

1. 写了哪些正式库
2. 当前窗口写入了多少 scope / rows
3. checkpoint / queue 是否成立
4. downstream 是否仍回读 `pas_context_snapshot / structure_candidate_snapshot`
5. evidence / record / conclusion 是否闭环

### 5. 2010 是主线试跑，不是历史展示样本

`2010` 不是随便挑一个年份做演示，而是本组卡正式 pilot 年份。

pilot 的目标是验证：

- canonical `malf` 能在真实库完成 bootstrap
- downstream 能默认消费 canonical `malf_state_snapshot`
- bounded truthfulness readout 能在正式路径上复验

只有 pilot 通过，三年一段建库才有正式合法性。

## 正式卡组顺序

本设计冻结的新卡组顺序为：

1. `56`：冻结 `2010` pilot 范围、正式路径、样本读数与收口标准
2. `57`：在真实正式库上完成 `2010` canonical `malf` bootstrap / replay
3. `58`：在真实正式库上完成 `2010` 的 `structure / filter / alpha` canonical smoke
4. `59`：完成 `2010` bounded truthfulness / readout gate，裁决是否进入批量分段建库
5. `60`：`2011-2013`
6. `61`：`2014-2016`
7. `62`：`2017-2019`
8. `63`：`2020-2022`
9. `64`：`2023-2025`
10. `65`：`2026` 当年增量对齐
11. `66`：official middle-ledger cutover gate，之后才恢复 `100`

## 影响

1. `55` 仍然保持“上游 data-grade baseline 已接受”的历史结论，不回滚。
2. 但 `55` 之后、`100` 之前，必须新增 `56-66` 这组真实正式库落地卡。
3. `100-105` 的恢复前提从“`55` 通过”收紧为：
   - `55` 通过
   - `56-66` 完成
   - 真实正式库 canonical mainline cutover 已确认

## 流程图

```mermaid
flowchart LR
    G55["55 upstream baseline gate"] --> C56["56 2010 pilot freeze"]
    C56 --> C57["57 malf canonical 2010"]
    C57 --> C58["58 structure/filter/alpha 2010"]
    C58 --> C59["59 2010 truthfulness gate"]
    C59 --> C60["60 2011-2013"]
    C60 --> C61["61 2014-2016"]
    C61 --> C62["62 2017-2019"]
    C62 --> C63["63 2020-2022"]
    C63 --> C64["64 2023-2025"]
    C64 --> C65["65 2026 YTD"]
    C65 --> C66["66 official cutover gate"]
    C66 --> C100["100-105 trade/system recovery"]
```
