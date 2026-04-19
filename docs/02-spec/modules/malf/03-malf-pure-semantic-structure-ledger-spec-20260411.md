# malf 纯语义走势账本正式规格

日期：`2026-04-11`
状态：`生效中`

> 角色声明：本文是当前唯一应被当作 `malf core` 读取的正式规格。
> 它定义纯语义走势账本本身，不替代 bridge v1 的现行字段合同。
> bridge v1 请读 `01-market-base-to-malf-minimal-snapshot-bridge-spec-20260410.md`。
> 若需要读取机制层 `pivot-confirmed break` 与 `same-timeframe stats sidecar` 合同，请读 `04-malf-mechanism-layer-break-confirmation-and-same-timeframe-stats-sidecar-spec-20260411.md`。

## 1. 适用范围

本规格冻结 `malf` 的纯语义核心。

它只回答三件事：

1. 当前结构是什么
2. 当前推进到了哪里
3. 旧结构是否已经被破坏

本规格覆盖：

1. 本级别 `price bar -> pivot -> wave -> state -> progress`
2. `HH / HL / LL / LH / break / count` 的正式语义
3. bridge v1 与纯语义核心的兼容边界

本规格不覆盖：

1. 高周期背景裁决
2. 动作接口
3. 仓位 sizing
4. 执行定价
5. 直接交易建议

## 2. 正式定义

`malf = 按时间级别独立运行的走势账本`

它的唯一输入是本级别 `price bar` 序列。

它不引入：

1. 均线
2. 收益率标签
3. 高周期 `context`
4. `allowed_actions`
5. `confidence`

公理层冻结为：

1. `malf` 是按时间级别独立运行的纯语义走势账本系统。
2. 它只依赖本级别 `price bar`。
3. 正式原语只有 `HH / HL / LL / LH / break / count`。
4. `HH / LL` 负责推进，`HL / LH` 负责守成，`break` 负责旧结构失效。
5. `hh_count / ll_count` 只在当前 `wave_id` 内有效，不跨波段、不跨级别。
6. 每个 `timeframe` 独立闭环，其他级别不得参与其状态与统计计算。
7. `牛逆 / 熊逆` 是旧顺结构失效后、到新顺结构确认前的本级别过渡状态。
8. `break` 是触发，不是确认；新的同向极值推进出现，才确认新的顺状态。
9. 统计若存在，只能作为同级别 sidecar。
10. 动作、仓位、回测、概率、收益都属于下游，不属于 `malf core`。

## 3. 正式原语

`malf` 的正式原语只允许是：

1. `HH`
2. `HL`
3. `LL`
4. `LH`
5. `break`
6. `count`

语义冻结如下：

1. `HH`
   - 当前上行推进中被确认的新高
2. `HL`
   - 上行结构中的回摆低点，且未破坏原上行结构
3. `LL`
   - 当前下行推进中被确认的新低
4. `LH`
   - 下行结构中的反弹高点，且未破坏原下行结构
5. `break`
   - 对最后有效结构守护点的破坏
6. `count`
   - 当前波段内部同方向新极值的累计次数

## 4. 时间级别原则

每个 `timeframe` 必须独立运行一套完整结构。

正式硬规则：

1. 不允许跨级别参与 `state` 计算
2. 不允许跨级别共享 `pivot / wave / count`
3. 不允许跨级别共享统计样本

因此：

1. 月、周、日之间不存在结构因果关系
2. 多级别同读只允许发生在下游消费层

若后续恢复同级别统计 sidecar，也必须只在同一 `timeframe` 内派生，且不得反向参与状态机。

## 5. canonical 纯语义账本家族

### 5.1 `bar_ledger`

用途：

1. 作为 `malf` 全部结构推导的原始 bar 输入

自然键：

`instrument + timeframe + bar_end_dt`

最小字段：

1. `instrument`
2. `timeframe`
3. `bar_end_dt`
4. `open`
5. `high`
6. `low`
7. `close`
8. `volume`
9. `adjust_method`

### 5.2 `pivot_ledger`

用途：

1. 记录本级别确认后的关键高低点

自然键：

`instrument + timeframe + pivot_id`

最小字段：

1. `pivot_id`
2. `instrument`
3. `timeframe`
4. `pivot_type`
5. `pivot_bar_dt`
6. `pivot_price`
7. `confirmed_at`
8. `prior_pivot_id`

枚举：

1. `pivot_type = H | L`

### 5.3 `wave_ledger`

用途：

1. 记录本级别当前结构生命周期

自然键：

`instrument + timeframe + wave_id`

最小字段：

1. `wave_id`
2. `instrument`
3. `timeframe`
4. `wave_direction`
5. `major_state`
6. `start_bar_dt`
7. `end_bar_dt`
8. `anchor_pivot_id`
9. `active_flag`

枚举：

1. `wave_direction = UP | DOWN`

### 5.4 `extreme_progress_ledger`

用途：

1. 记录当前波段内部的极值推进

自然键：

`instrument + timeframe + wave_id + progress_seq`

最小字段：

1. `instrument`
2. `timeframe`
3. `wave_id`
4. `progress_seq`
5. `progress_type`
6. `record_bar_dt`
7. `record_price`
8. `break_base_pivot_id`
9. `cumulative_count`

枚举：

1. `progress_type = HH | LL`

正式约束：

1. `cumulative_count` 只在当前 `wave_id` 内有效
2. 新波段建立后必须重新从 `1` 开始累计
3. 不允许跨 `wave_id` 或跨 `timeframe` 续算

### 5.5 `state_snapshot`

用途：

1. 在任一 `asof_bar_dt` 固化当前结构状态

自然键：

`instrument + timeframe + asof_bar_dt`

最小字段：

1. `instrument`
2. `timeframe`
3. `asof_bar_dt`
4. `major_state`
5. `trend_direction`
6. `wave_id`
7. `last_confirmed_H`
8. `last_confirmed_L`
9. `last_valid_HL`
10. `last_valid_LH`
11. `current_hh_count`
12. `current_ll_count`

明确排除字段：

1. `push_count`
2. `pullback_count`
3. `break_trigger`
4. `higher_timeframe_state_ref`
5. `allowed_actions`

## 6. 状态语义

`major_state` 固定为四态：

1. `BULL_WITH_TREND`
2. `BULL_COUNTER_TREND`
3. `BEAR_WITH_TREND`
4. `BEAR_COUNTER_TREND`

中文解释：

1. `BULL_WITH_TREND`
   - 牛顺
   - `HH` 持续出现，且回摆不破最后有效 `HL`
2. `BULL_COUNTER_TREND`
   - 牛逆
   - 旧上行顺结构已失效，进入到新顺结构确认前的逆向下摆过渡跟踪
3. `BEAR_WITH_TREND`
   - 熊顺
   - `LL` 持续出现，且反弹不破最后有效 `LH`
4. `BEAR_COUNTER_TREND`
   - 熊逆
   - 旧下行顺结构已失效，进入到新顺结构确认前的逆向反弹过渡跟踪

状态语义只由本级别结构排列裁决，不允许引用更高周期背景。

## 7. 状态转移与转折语义

### 7.1 `break` 的正式语义

`break` 只表示旧结构失效，不表示新结构已经成立。

正式约束：

1. `break_last_valid_HL`
   - 表示最后有效 `HL` 被破坏
2. `break_last_valid_LH`
   - 表示最后有效 `LH` 被破坏
3. `break` 触发后，必须等待新的同向极值推进，才能确认新顺状态

因此：

1. `break` 只负责宣布旧结构失效
2. `HH / LL` 新推进才负责确认新顺结构成立

### 7.2 顺状态到逆状态

1. `BULL_WITH_TREND -> BULL_COUNTER_TREND`
   - 条件：`break_last_valid_HL = true`
2. `BEAR_WITH_TREND -> BEAR_COUNTER_TREND`
   - 条件：`break_last_valid_LH = true`

### 7.3 逆状态的两种结局

逆状态只表示“旧结构已失效，新结构尚未完成确认”。

因此：

1. `BULL_COUNTER_TREND -> BULL_WITH_TREND`
   - 条件：逆向下摆失败，重新确认新的 `HH`
2. `BULL_COUNTER_TREND -> BEAR_WITH_TREND`
   - 条件：逆向下摆继续扩展，确认新的 `LL`
3. `BEAR_COUNTER_TREND -> BEAR_WITH_TREND`
   - 条件：逆向反弹失败，重新确认新的 `LL`
4. `BEAR_COUNTER_TREND -> BULL_WITH_TREND`
   - 条件：逆向反弹继续扩展，确认新的 `HH`

### 7.4 最小转折过程

转折不是一个点，而是一个过程：

1. 触发
   - `break(HL)` 或 `break(LH)` 发生
2. 未确认
   - 旧结构已失效，但新方向尚未形成新的推进
3. 确认
   - 新方向出现新的 `HH` 或 `LL`

## 8. 推进计量

推进只用本波段内部的极值累计来计量：

1. 上行推进记录 `hh_count`
2. 下行推进记录 `ll_count`

正式规则：

1. 每产生一个新的 `HH`，`hh_count += 1`
2. 每产生一个新的 `LL`，`ll_count += 1`
3. `HL / LH` 负责守成，不负责推进累计

## 9. bridge v1 兼容关系

当前仓内既有：

1. `pas_context_snapshot`
2. `structure_candidate_snapshot`
3. `scripts/malf/run_malf_snapshot_build.py`

它们在本规格下重新定义为：

1. `bridge v1` 兼容输出
2. 现有 `structure` runner 的过渡输入
3. 由纯语义核心派生的低维视图，而不是 `malf` 的终局定义

当前 runner 边界仍冻结为：

1. 只消费官方 `market_base.stock_daily_adjusted(adjust_method='backward')`
2. 不允许直接回读离线文本或 `raw_market`
3. 新的 pure semantic canonical ledger 落地前，bridge v1 继续保留

## 10. 对下游模块的边界要求

1. `structure`
   - 应逐步转向消费 `pivot_ledger / wave_ledger / state_snapshot`
2. `filter`
   - 若需要统计或多级别共读，必须在 `malf` 之外单独定义同级别 sidecar 或消费视图
3. `alpha`
   - 不得把 `execution_interface` 重新写回 `malf` 核心规格

## 11. 当前明确不做

1. 不在本规格内冻结 `execution_interface`
2. 不在本规格内冻结 `allowed_actions / confidence / preferred_action_family`
3. 不在本规格内冻结高周期 `context` 参与状态机
4. 不在本规格内冻结 `position sizing`
5. 不在本规格内给出直接交易建议

## 12. 一句话收口

`malf` 的正式核心是：用本级别 price bar 形成的 HH/HL/LL/LH 构造结构，用 break 标记旧结构失效，用当前波段内部的极值累计刻画推进，而不把背景、概率和动作重新混入结构真相。`

更短收口：

`malf` 是一个按时间级别独立运行的纯语义走势账本系统，用 `pivot / wave` 组织生命周期，用 `HH/HL/LL/LH` 描述结构，用 `break` 标记旧结构失效，用当前 `wave` 内的极值累计刻画趋势推进；统计、背景和动作都只能在下游分层消费。`

## 流程图

```mermaid
flowchart LR
    BAR[price bar D/W/M] --> PIV[pivot_ledger]
    PIV --> WAVE[wave_ledger]
    WAVE --> PROG[extreme_progress_ledger]
    PROG --> SNAP[malf_state_snapshot]
    SNAP --> DOWN[structure/filter/alpha 下游]
```
