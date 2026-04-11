# malf canonical ledger and data-grade runner bootstrap 规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `30-malf-canonical-ledger-and-data-grade-runner-bootstrap-card-20260411.md` 及其后续 evidence / record / conclusion。

## 目标

让 canonical malf 成为正式历史账本 runner，并具备与 `data` 同级的批量建仓、增量更新和断点续跑能力。

## 最小能力

1. 一次性批量建仓
2. 每日或每批次增量更新
3. dirty/work queue
4. checkpoint ledger
5. bounded replay / resume

## 约束

1. `run_id` 只做审计。
2. dirty/work queue 必须是正式账本，不允许只靠 rerun 推断。
3. checkpoint 粒度必须比现有 `instrument + timeframe` 更完整，至少能支撑 canonical 阶段推进与 resume。
