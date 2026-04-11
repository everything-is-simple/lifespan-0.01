# malf semantic canonical contract freeze 设计

卡号：`29`
日期：`2026-04-11`
状态：`待施工`

## 背景

当前仓库内的 `malf` 仍是 `bridge-v1`：

- 用 `MA20 / MA60 / ret20` 推导 `malf_context_4`
- 用滚动窗口 `new_high_count / new_low_count` 近似推进
- 用 sidecar 统计补充 break / stats

这套实现可以把主链打通，但它不是你现在确认的正式 `malf` 语义。  
新的正式口径必须把 `malf` 收缩成纯结构语义账本，而不是市场标签层，也不是交易动作层。

## 目标

本卡只冻结 `malf canonical v2` 的纯语义合同：

1. `malf` 的唯一输入是本级别 `price bar`
2. `malf` 的唯一核心原语是 `HH / HL / LL / LH / break / count`
3. `malf` 的正式核心账本冻结为：
   - `pivot_ledger`
   - `wave_ledger`
   - `extreme_progress_ledger`
   - `state_snapshot`
   - `same_level_stats`
4. 月 / 周 / 日各自独立计算结构、推进、统计
5. 高级别信息不得参与低级别结构计算
6. 交易动作、仓位建议、执行接口全部排除在 `malf` 之外

## 非目标

本卡不做：

- `structure / filter / alpha` 的下游改绑
- `trade / system` 逻辑
- 交易动作矩阵、执行接口、仓位建议
- 回测 alpha / pnl / execution 语义

## 设计原则

### 1. 结构先于解释

`malf` 只回答：

- 当前结构是什么
- 当前推进到了哪里
- 结构是否被破坏

`malf` 不回答：

- 是否该交易
- 是否处于大周期背景
- 是否该做多 / 做空

### 2. 推进、守成、转移三分

- `HH / LL` 负责推进
- `HL / LH` 负责守成
- `break` 负责旧结构失效与状态转移触发

### 3. 转折是过程，不是点

`break` 只表示旧结构失效，不等于新结构成立。  
新方向必须由新的 `HH` 或 `LL` 推进确认。

### 4. 时间级别完全独立

月 / 周 / 日各自独立跑一套：

- pivot
- wave
- state
- stats

不同级别之间不得共享状态、极值、计数。  
如果后续需要高周期背景，那也是 `filter / alpha` 的只读外挂信息，而不是 `malf` 内部反馈变量。

### 5. 统计对象是 wave，不是 bar

`same_level_stats` 只允许基于本级别已完成的 `wave` 统计，不允许跨级别混样本，也不允许用 `bar` 样本冒充波段统计。

## 正式冻结的核心定义

### 状态

- `牛顺`：`HH` 持续出现，且回摆不破最后有效 `HL`
- `牛逆`：在多头旧结构失效后，处于向下过渡阶段，按 `LL + LH` 跟踪
- `熊顺`：`LL` 持续出现，且反弹不破最后有效 `LH`
- `熊逆`：在空头旧结构失效后，处于向上过渡阶段，按 `HH + HL` 跟踪

### 事件

- `break_last_HL`
- `break_last_LH`
- `new_record_high_in_wave`
- `new_record_low_in_wave`
- `confirmed_pullback_low`
- `confirmed_bounce_high`
- `wave_reset`

### 转移

- `牛顺 -> 牛逆`：`break_last_HL`
- `牛逆 -> 牛顺`：新方向先触发 `break_last_LH`，再由新的 `HH` 推进确认
- `熊顺 -> 熊逆`：`break_last_LH`
- `熊逆 -> 熊顺`：新方向先触发 `break_last_HL`，再由新的 `LL` 推进确认

## 关键裁决

1. `malf_context_4`、`new_high_count`、`new_low_count` 不再代表正式 `malf` 语义
2. `pivot-confirmed break` 与 `same-timeframe stats` 不能继续冒充 `malf core`
3. `execution_interface / confidence / allowed_actions` 不属于 `malf`
4. 高级别 `context` 不得参与 `malf` 状态机
5. `same_level_stats` 只统计本级别已完成 `wave`

## 与后续卡关系

- `30`：把本卡冻结的语义落为正式表结构、runner、queue/checkpoint
- `31`：`structure / filter / alpha` 改绑 canonical `malf`
- `32`：主链 truthfulness revalidation
