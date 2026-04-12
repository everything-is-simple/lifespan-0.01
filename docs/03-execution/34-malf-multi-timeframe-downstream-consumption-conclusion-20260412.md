# malf multi-timeframe downstream consumption 结论

日期：`2026-04-12`  
状态：`已裁决`

## 裁决

- 接受

## 原因

- `structure_snapshot` 已正式显式落表 `daily/weekly/monthly` 多周期字段族；其中 `major_state / trend_direction / reversal_stage / wave_id / current_hh_count / current_ll_count` 继续代表 `D` 主语义，`W/M` 仅作为只读 canonical context。
- `structure` 对 `W/M` 的读取已冻结为 `latest asof_date <= D asof_date` 的只读挂接，不会反向改写 `D` 级别 `malf core` 真值。
- `filter_snapshot` 已把多周期上下文透传到正式账本，并将高周期信息限制为 sidecar/admission note 背景，不引入新的硬拦截逻辑。
- `alpha_trigger_event / alpha_formal_signal_event` 已显式落表 `daily_source_context_nk + weekly_* + monthly_*`，高周期变动会进入 rematerialize 指纹，不再只隐藏在 `upstream_context_fingerprint` 中。
- `tests/unit/structure/test_runner.py`、`tests/unit/filter/test_runner.py`、`tests/unit/alpha/test_runner.py` 合计 `12 passed`，覆盖多周期消费、rematerialize 与只读边界。

## 影响

- 当前最新生效结论锚点推进到 `34-malf-multi-timeframe-downstream-consumption-conclusion-20260412.md`。
- 当前待施工卡推进到 `35-downstream-data-grade-checkpoint-alignment-after-malf-card-20260411.md`；`35` 可以只处理下游 queue/checkpoint/dirty queue 对齐，而不必再回头冻结多周期消费契约。
- `W/M` 的正式定位已经明确为“只读背景”，不是 `malf` 状态机输入，也不是 `filter / alpha` 的新增硬门槛。
- `36` 之后的寿命 sidecar 与 `100-105` 的 trade/system 恢复，应继续基于当前已冻结的多周期下游契约推进。

## 结论结构图

```mermaid
flowchart LR
    MALF["canonical malf D/W/M"] --> STR["structure 显式多周期字段"]
    STR --> FLT["filter 只读透传"]
    FLT --> ALPHA["alpha 事件显式落列"]
    ALPHA --> NEXT["35 checkpoint / dirty queue alignment"]
```
