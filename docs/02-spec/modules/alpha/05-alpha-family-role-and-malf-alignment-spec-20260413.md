# alpha family role 与 malf 协同规格
`日期：2026-04-13`
`状态：生效`

## 目标

在现有 `alpha_trigger_event -> alpha_family_event -> alpha_formal_signal_event` 主链上，为 `alpha family` 冻结正式家族解释合同，使其成为：

- PAS 五触发的官方角色判定层
- canonical `malf` 的只读协同解释层
- `formal signal` 升级前的正式中间账本

## 输入合同

`alpha family` 只允许消费：

1. 官方 `alpha_trigger_event`
2. 官方 `alpha_trigger_candidate`
3. 官方 `structure_snapshot`
4. 官方 canonical `malf_state_snapshot(timeframe='D')` 或由 `structure_snapshot` 映射出的 canonical 下游兼容上下文

不允许：

1. 回读 bridge-era `pas_context_snapshot`
2. 直接回读 trade / position 私有过程
3. 把执行动作或仓位建议写成 `malf core` 真值

## 输出合同

`alpha_family_event` 保持三表家族不变，但 payload 语义升级。

### 顶层字段

保留现有字段：

- `family_event_nk`
- `trigger_event_nk`
- `instrument`
- `signal_date`
- `asof_date`
- `trigger_family`
- `trigger_type`
- `pattern_code`
- `family_code`
- `family_contract_version`
- `payload_json`
- `first_seen_run_id`
- `last_materialized_run_id`

### payload_json 正式键

`payload_json` 至少包含：

1. `family_role`
   - `mainline / supporting / scout / warning`
2. `malf_alignment`
   - `aligned / cautious / conflicted / unknown`
3. `malf_phase_bucket`
   - `early / middle / late / unknown`
4. `family_bias`
   - `trend_continuation / reversal_attempt / countertrend_probe / trap_warning`
5. `trigger_reason`
   - 人读解释，说明触发为何被归入该家族角色
6. `structure_anchor_nk`
   - 关联的官方 `structure_snapshot_nk`
7. `source_context_fingerprint`
   - 触发 rematerialize 的上游指纹

### 默认角色映射

| trigger_type | 默认 family_role | 默认 family_bias |
| --- | --- | --- |
| `bof` | `mainline` | `reversal_attempt` |
| `tst` | `mainline` | `trend_continuation` |
| `pb` | `supporting` | `trend_continuation` |
| `cpb` | `scout` | `countertrend_probe` |
| `bpb` | `warning` | `trap_warning` |

## 业务规则

1. `BOF / TST`
   - 默认进入 `mainline`
   - 当 `malf_alignment='conflicted'` 时只允许降级，不允许升级
2. `PB`
   - 默认 `supporting`
   - 仅当结构与趋势延续条件满足时可被标记为 `pb_first_pullback=true`
3. `CPB`
   - 默认 `scout`
   - 不得在 family 层直接升级为 `mainline`
4. `BPB`
   - 默认 `warning`
   - 用作警戒与降权依据，不作 admitted 主线

## 与 formal signal 的边界

本卡不新增 `alpha_formal_signal_event` 物理列，但要求后续 formal signal 升级只允许消费本卡冻结的 family 解释键。

也就是说：

- `family_role / malf_alignment / malf_phase_bucket / family_bias` 是后续正式真值来源
- `malf_context_4 / lifecycle_rank_*` 仍只是 compat-only 过渡列

## runner 行为约束

1. 保持 bounded runner、keyword-only 参数与 summary dataclass 模式不变。
2. 保持 `run / checkpoint / run_event` 审计表族不变。
3. 允许 rematerialize 已有 `alpha_family_event`，条件是：
   - 上游 trigger 指纹变化
   - structure 指纹变化
   - canonical malf 映射变化
   - family_contract_version 升级

## 验收

1. `41` 的官方 PAS detector 输出可直接进入 family ledger。
2. 五触发均有正式角色输出。
3. family payload 可稳定审计 rematerialize 原因。
4. `python scripts/system/check_doc_first_gating_governance.py` 通过。
