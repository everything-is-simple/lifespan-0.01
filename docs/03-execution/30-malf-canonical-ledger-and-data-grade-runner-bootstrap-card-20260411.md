# malf canonical ledger and data-grade runner bootstrap

卡片编号：`30`
日期：`2026-04-11`
状态：`待施工`

## 目标

真正把 canonical `malf v2` 做成可运行、可续跑、可审计的正式历史账本实现。

## 依赖

- [07-malf-canonical-ledger-and-data-grade-runner-bootstrap-charter-20260411.md](</h:/lifespan-0.01/docs/01-design/modules/malf/07-malf-canonical-ledger-and-data-grade-runner-bootstrap-charter-20260411.md>)
- [07-malf-canonical-ledger-and-data-grade-runner-bootstrap-spec-20260411.md](</h:/lifespan-0.01/docs/02-spec/modules/malf/07-malf-canonical-ledger-and-data-grade-runner-bootstrap-spec-20260411.md>)
- [29-malf-semantic-canonical-contract-freeze-conclusion-20260411.md](</h:/lifespan-0.01/docs/03-execution/29-malf-semantic-canonical-contract-freeze-conclusion-20260411.md>)

## 任务

1. 新增 canonical `malf` 表族：
   - `malf_canonical_run`
   - `malf_canonical_work_queue`
   - `malf_canonical_checkpoint`
   - `malf_pivot_ledger`
   - `malf_wave_ledger`
   - `malf_extreme_progress_ledger`
   - `malf_state_snapshot`
   - `malf_same_level_stats`
2. 新增 canonical runner：
   - 从官方 `market_base` 读取 bars
   - 支持 `D / W / M`
   - 支持批量建仓 / 每日增量 / resume
3. 新增 canonical script，供后续卡与验证脚本调用。
4. 补 canonical 单元测试，覆盖：
   - pivot 生成与 `confirmed_at`
   - wave/state/extreme 物化
   - same-level wave stats
   - dirty queue / checkpoint / replay
5. 回填 `30` 的 evidence / record / conclusion。

## 范围

### 包含

- `src/mlq/malf/*`
- `scripts/malf/*`
- `tests/unit/malf/*`
- `docs/01-design/modules/malf/07-*`
- `docs/02-spec/modules/malf/07-*`
- `docs/03-execution/30-*`
- `docs/03-execution/evidence/30-*`
- `docs/03-execution/records/30-*`

### 不包含

- `31` 的 downstream rebind
- `32` 的 truthfulness revalidation
- 交易动作与 alpha 决策矩阵

## 历史账本约束

- 实体锚点：`asset_type + code + timeframe`
- 业务自然键：
  - `pivot_bar_dt + pivot_type`
  - `wave_id`
  - `wave_id + extreme_seq`
  - `asof_bar_dt`
- 批量建仓：按 `code + timeframe` 全历史回放
- 增量更新：按 dirty scope 回放 `tail_start_bar_dt` 之后的 bars
- 断点续跑：`work_queue + checkpoint + replay`
- 审计账本：`malf_canonical_run` + execution 文档

## 完成标准

1. canonical `malf` 在本地 DuckDB 中正式落表。
2. `D / W / M` 三个时间级别可独立产出结果。
3. `malf` 具备批量建仓、每日增量、断点续跑能力。
4. bridge-v1 兼容产物与 canonical v2 并存，但不混真值。
