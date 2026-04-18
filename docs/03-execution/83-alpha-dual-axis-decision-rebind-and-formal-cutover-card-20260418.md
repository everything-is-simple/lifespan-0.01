# alpha 五 PAS 日线终审重绑与 formal cutover

`卡号`：`83`
`日期`：`2026-04-18`
`状态`：`草稿`

## 需求

- 问题：`alpha` 虽然名义上负责 trigger 与 final verdict，但现实上长期被 `structure/filter` 预判和预裁决架空；同时五个 trigger 的账本形态也还没冻结清楚。
- 目标结果：把 `alpha` 明确重绑为五个 PAS 日线终审库：
  - `BOF / TST / PB / CPB / BPB`
  - 读取 `malf_day / week / month` 与 `structure_day / week / month` 上下文
  - 读取 `filter_day` 的 objective gate 结果
  - 但不为五个 trigger 各自再拆独立 `D/W/M` 三套账本
- 为什么现在做：只有把五个 PAS 的物理落库与“仍是日线决策库”同时冻结，`alpha` 才不会再次滑回单库混写或 `5 × 3` 膨胀。

## 设计输入

- 设计文档：`docs/01-design/modules/system/18-malf-alpha-dual-axis-and-timeframe-native-refactor-charter-20260418.md`
- 规格文档：`docs/02-spec/modules/system/18-malf-alpha-dual-axis-and-timeframe-native-refactor-spec-20260418.md`

## 任务分解

1. 冻结 `alpha_bof / alpha_tst / alpha_pb / alpha_cpb / alpha_bpb` 五个 PAS 官方库路径。
2. 冻结五库的默认消费口径：
   - `malf_day / week / month`
   - `structure_day / week / month`
   - `filter_day`
3. 冻结五个 PAS 仍是日线决策账本，不额外再拆 trigger-level `D/W/M` 三套库。
4. 把 objective gate、note sidecar 与 formal signal verdict 冻在 `alpha` 层。
5. 完成 `2010-01-01` 至当前 official `market_base` 覆盖尾部的五 PAS `trigger / family / formal_signal` bounded replay 与 cutover 证据。

## 实现边界

- 范围内：`alpha` 五 PAS 日线终审库、上下文绑定、bounded replay 与 formal cutover。
- 范围外：
  - 本卡不恢复 `position / portfolio_plan / trade / system`
  - 本卡不把五个 trigger 再扩成 `D/W/M` 三套独立账本

## 历史账本约束

- 实体锚点：`asset_type + code`。
- 业务自然键：沿用 `alpha_trigger_event / alpha_family_event / alpha_formal_signal_event` 既有自然键，并在五个 PAS 库内按 `pas_code` 做物理隔离。
- 批量建仓：支持 `2010-01-01` 至当前 official `market_base` 覆盖尾部的 bounded replay。
- 增量更新：沿用 trigger/family/formal signal 的 checkpoint / replay 口径续跑。
- 断点续跑：允许五个 PAS 各自中断后继续，不允许跨库手工短接。
- 审计账本：保留 `run / run_event / summary_json + truthfulness evidence`。

## 收口标准

1. `alpha` 成为唯一正式 `admitted / blocked / downgraded / note_only` 主权层。
2. 五个 PAS 日线官方库成为默认 `alpha` 真值落点。
3. 五个 trigger 不再被写成各自独立 `D/W/M` 三套账本。
4. 默认源码与 summary 不再把 `filter` 当最终 verdict 替身。
5. 完成 `2010-01-01` 至当前 official `market_base` 覆盖尾部的五 PAS bounded replay。
6. 与旧口径差异、owner / reason / audit_note 口径写清。

## 卡片结构图

```mermaid
flowchart LR
    MD["malf_day/week/month"] --> ALP["alpha five PAS day decision"]
    SD["structure_day/week/month"] --> ALP
    FD["filter_day"] --> ALP
    ALP --> SIG["formal signal"]
```
