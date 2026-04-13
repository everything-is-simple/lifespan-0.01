# position data-grade checkpoint 与 replay runner

`卡号`：`50`
`日期`：`2026-04-13`
`状态`：`待施工`

## 问题

- `position` 仍缺少与 `structure / filter / alpha` 同级的 `work_queue + checkpoint + replay/resume`。
- 这使得 `position` 无法成为真正的主线历史账本 runner。

## 设计依据

- [03-position-data-grade-ledger-and-runner-charter-20260413.md](/H:/lifespan-0.01/docs/01-design/modules/position/03-position-data-grade-ledger-and-runner-charter-20260413.md)
- [05-position-data-grade-ledger-and-runner-spec-20260413.md](/H:/lifespan-0.01/docs/02-spec/modules/position/05-position-data-grade-ledger-and-runner-spec-20260413.md)

## 任务

1. 新增 `position_work_queue / position_checkpoint`。
2. 升级 `scripts/position/run_position_formal_signal_materialization.py` 为正式 data-grade runner。
3. 让 `position` 支持 bootstrap、增量更新、checkpoint 续跑、局部 replay。
4. 保持所有路径严格来自 `WorkspaceRoots`。

## 历史账本约束

1. `实体锚点`
   - `asset_type + code` 与 `candidate_nk`。
2. `业务自然键`
   - `queue_nk / checkpoint_nk / candidate_nk`。
3. `批量建仓`
   - 对历史 formal signal 一次性建仓。
4. `增量更新`
   - 对脏 signal 和脏参考价局部重算。
5. `断点续跑`
   - 正式交付 `work_queue + checkpoint + replay/resume`。
6. `审计账本`
   - `position_run` 记录 inserted / reused / rematerialized。

## A 级判定表

| 判定项 | A 级通过标准 | 不接受情形 | 交付物 |
| --- | --- | --- | --- |
| 官方本地 ledger | `scripts/position/run_position_formal_signal_materialization.py` 升级后仍固定写入 `WorkspaceRoots -> H:\\Lifespan-data` 下的正式 `position.duckdb` | 使用 shadow DB、仓库内临时库或私有 DataFrame 作为主路径 | runner 路径契约与正式落库验证 |
| work queue | `position_work_queue` 具备 `queue_nk / signal_nk / instrument / reference_trade_date / source fingerprint / queue_status` 等正式字段 | 只有内存队列、summary 中隐含队列，或 queue 无法稳定复算 | `position_work_queue` DDL 与挂脏规则 |
| checkpoint | `position_checkpoint` 能按实体锚点与 scope 记录最近完成边界，并用于跳过未变化历史 | 没有 checkpoint，或 checkpoint 只记录 `run_id` | `position_checkpoint` DDL 与更新规则 |
| bootstrap / incremental / replay | 同一 runner 支持历史建仓、每日增量、局部 replay/resume 三种模式，且局部 replay 不重算全历史 | 仍只有 bounded materialization，或 replay 退化为全量重跑 | 运行模式参数与行为说明 |
| rematerialize 审计 | `position_run` 与 run_snapshot 能稳定区分 `inserted / reused / rematerialized`，并解释由 signal/price/policy 哪类脏源触发 | 只能记录 completed/failed，无法解释复用与重物化 | summary_json、run_snapshot 与测试 |
| smoke 与 acceptance | 至少通过官方库 smoke、checkpoint/resume smoke、rematerialize smoke、五层事实对齐检查 | 只有单元测试，没有真实 runner/replay 证据 | evidence 命令、smoke 产物与 acceptance 读数 |
