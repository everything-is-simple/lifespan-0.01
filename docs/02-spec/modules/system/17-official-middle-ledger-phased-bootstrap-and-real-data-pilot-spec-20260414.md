# official middle-ledger phased bootstrap and real-data pilot 规格

适用执行卡：

- `56-mainline-official-middle-ledger-2010-pilot-scope-freeze-card-20260414.md`
- `57-malf-canonical-official-2010-bootstrap-and-replay-card-20260414.md`
- `58-structure-filter-alpha-official-2010-canonical-smoke-card-20260414.md`
- `59-mainline-middle-ledger-2010-truthfulness-gate-card-20260414.md`
- `60-mainline-middle-ledger-2011-2013-bootstrap-card-20260414.md`
- `61-mainline-middle-ledger-2014-2016-bootstrap-card-20260414.md`
- `62-mainline-middle-ledger-2017-2019-bootstrap-card-20260414.md`
- `63-mainline-middle-ledger-2020-2022-bootstrap-card-20260414.md`
- `64-mainline-middle-ledger-2023-2025-bootstrap-card-20260414.md`
- `65-mainline-middle-ledger-2026-ytd-incremental-alignment-card-20260414.md`
- `66-pre-trade-middle-ledger-official-cutover-gate-card-20260414.md`

## 1. 正式范围

本组卡正式写入的库固定为：

1. `H:\Lifespan-data\malf\malf.duckdb`
2. `H:\Lifespan-data\structure\structure.duckdb`
3. `H:\Lifespan-data\filter\filter.duckdb`
4. `H:\Lifespan-data\alpha\alpha.duckdb`

本组卡默认只覆盖：

- `market_base(backward) -> malf canonical`
- `malf_state_snapshot(D) -> structure -> filter -> alpha`

`position / portfolio_plan` 只允许在 `59 / 66` 里做只读 acceptance check。

## 2. pilot 与分段窗口

### 2.1 pilot 年份

`56-59` 固定使用：

- `2010-01-01 ~ 2010-12-31`

### 2.2 三年一段建库窗口

`60-64` 固定按下列窗口推进：

1. `60`：`2011-01-01 ~ 2013-12-31`
2. `61`：`2014-01-01 ~ 2016-12-31`
3. `62`：`2017-01-01 ~ 2019-12-31`
4. `63`：`2020-01-01 ~ 2022-12-31`
5. `64`：`2023-01-01 ~ 2025-12-31`

### 2.3 当年增量窗口

`65` 固定覆盖：

- `2026-01-01 ~ 当前正式 market_base 最大 trade_date`

`65` 默认优先走 queue/checkpoint 增量对齐，而不是整段全量重算。

## 3. `56-59` 的强约束

### 3.1 `56`

必须冻结：

1. pilot 年份
2. 正式写入路径
3. 模块范围
4. 运行命令草案
5. 报告目录与 evidence 输出形式

### 3.2 `57`

必须证明：

1. `bootstrap_malf_ledger` 已在真实正式库建立 canonical 表族
2. `run_malf_canonical_build` 能在 `2010` 窗口写入：
   - `malf_canonical_run`
   - `malf_canonical_work_queue`
   - `malf_canonical_checkpoint`
   - `malf_pivot_ledger`
   - `malf_wave_ledger`
   - `malf_extreme_progress_ledger`
   - `malf_state_snapshot`
   - `malf_same_level_stats`
3. queue/checkpoint 对 `2010` 窗口至少能解释 bootstrap 与 replay
4. bridge-v1 表并存但不被 canonical runner 回写

### 3.3 `58`

必须证明：

1. `structure` 默认绑定 `malf_state_snapshot`
2. `filter` 默认绑定 `malf_state_snapshot`
3. `alpha` 默认不回读 `pas_context_snapshot`
4. 真实正式库的 run summary 中不再出现：
   - `source_context_table='pas_context_snapshot'`
   - `source_structure_input_table='structure_candidate_snapshot'`
   - `fallback_context_table='pas_context_snapshot'`

### 3.4 `59`

必须完成：

1. bounded truthfulness readout
2. official row-count / scope-count 摘要
3. 代表性样本 spot-check
4. 对 `60-65` 是否继续推进的正式裁决

如果 `59` 未通过，不允许进入 `60`。

## 4. `60-65` 的窗口卡约束

每一张窗口卡都必须同时回答：

1. 本窗口使用的日期范围
2. 本窗口写入了哪些正式库
3. canonical `malf` 的 scope / row 增量
4. downstream 的 run / snapshot / event 增量
5. checkpoint / queue 是否稳定推进
6. 是否发现 bridge-v1 兼容回退
7. 是否需要补新卡或中断后续窗口

## 5. `66` cutover gate

`66` 必须裁决以下事项：

1. `H:\Lifespan-data\malf\malf.duckdb` 中 canonical 表族是否已覆盖 `2010 ~ 2026 YTD`
2. `structure / filter / alpha` 正式 run summary 是否已默认绑定 canonical
3. bridge-v1 是否已降级为兼容层，而非默认正式主线
4. `100-105` 是否可以在此基础上恢复

若任一答案为否，`66` 必须阻断 `100`。

## 6. 运行与证据要求

每张卡都至少要保留：

1. 可复验命令
2. 正式库路径
3. 关键表 row count / scope count
4. summary_json 摘要
5. 如有导出，导出到 `H:\Lifespan-report`

## 7. 非目标

本组卡不做：

1. 删除 bridge-v1 历史表
2. 恢复 `trade / system`
3. 重写 `data` 正式合同
4. 把 `position / portfolio_plan` 重新拉回大规模 bootstrap 主角

## 8. 流程图

```mermaid
flowchart LR
    MB["market_base backward"] --> M57["57 canonical malf 2010"]
    M57 --> D58["58 structure/filter/alpha 2010"]
    D58 --> G59["59 pilot gate"]
    G59 --> W60["60-64 three-year waves"]
    W60 --> Y65["65 current-year alignment"]
    Y65 --> G66["66 official cutover gate"]
    G66 --> T100["100-105"]
```
