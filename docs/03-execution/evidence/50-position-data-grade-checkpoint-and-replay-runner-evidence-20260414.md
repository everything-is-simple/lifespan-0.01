# position data-grade checkpoint 与 replay runner 证据

证据编号：`50`
日期：`2026-04-14`

## 命令证据

1. `python -m pytest tests/unit/position -q --basetemp H:\Lifespan-temp\pytest-tmp\position-50b`
   - 结果：`15 passed in 7.65s`
2. 受控 official smoke 初始化脚本
   - 动作：在 `H:\Lifespan-temp\card50\controlled-data` 建立最小 `alpha / market_base` 官方库副本
   - 事实：写入 `alpha_formal_signal_event(sig-card50-001)` 与 `stock_daily_adjusted(CARD50.SZ, 2026-04-09, none, 10.5)`
3. `python scripts/position/run_position_formal_signal_materialization.py --policy-id fixed_notional_full_exit_v1 --capital-base-value 1000000 --run-id card50-run-1 --summary-path H:\Lifespan-temp\card50\summary-run-1.json`
   - 环境：`PYTHONPATH=src`，`LIFESPAN_DATA_ROOT=H:\Lifespan-temp\card50\controlled-data`
   - 结果：`execution_mode=checkpoint_queue`，`inserted_count=1`，`queue_enqueued_count=1`，`queue_claimed_count=1`，`checkpoint_upserted_count=1`
4. `python scripts/position/run_position_formal_signal_materialization.py --policy-id fixed_notional_full_exit_v1 --capital-base-value 1000000 --run-id card50-run-2 --summary-path H:\Lifespan-temp\card50\summary-run-2.json`
   - 环境：同上
   - 结果：`execution_mode=checkpoint_queue`，`candidate_count=0`，`queue_enqueued_count=0`，证明默认 queue 续跑会跳过未变化历史
5. 受控 rematerialize 更新脚本
   - 动作：把 `H:\Lifespan-temp\card50\controlled-data\base\market_base.duckdb` 中 `CARD50.SZ / 2026-04-09 / none` 的 `close` 从 `10.5` 更新到 `11.0`
6. `python scripts/position/run_position_formal_signal_materialization.py --policy-id fixed_notional_full_exit_v1 --capital-base-value 1000000 --run-id card50-run-3 --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --summary-path H:\Lifespan-temp\card50\summary-run-3.json`
   - 环境：同上
   - 结果：`execution_mode=bounded`，`rematerialized_count=1`，`checkpoint_upserted_count=1`
7. 受控 DuckDB 查询
   - `position_work_queue` 最新行：`queue_status='completed'`、`queue_reason='bootstrap_missing_checkpoint'`
   - `position_checkpoint` 最新行：`checkpoint_scope='fixed_notional_full_exit_v1'`、`last_signal_nk='sig-card50-001'`、`last_run_id='card50-run-3'`
   - `position_run(card50-run-1)`：`execution_mode='checkpoint_queue'`、`inserted_count=1`
   - `position_run(card50-run-3)`：`execution_mode='bounded'`、`rematerialized_count=1`
   - `position_run_snapshot(card50-run-3)`：`materialization_action='rematerialized'`、`candidate_status='admitted'`、`position_action_decision='open_up_to_context_cap'`
   - `position_sizing_snapshot`：`reference_price=11.0`、`target_shares=17000`
8. `python scripts/system/check_doc_first_gating_governance.py`
   - 结果：通过；当前待施工卡 `50-position-data-grade-checkpoint-and-replay-runner-card-20260413.md` 已具备需求、设计、规格、任务分解与历史账本约束
9. `python scripts/system/check_development_governance.py AGENTS.md README.md pyproject.toml scripts/position/run_position_formal_signal_materialization.py src/mlq/position/__init__.py src/mlq/position/position_bootstrap_schema.py src/mlq/position/runner.py src/mlq/position/position_runner_shared.py src/mlq/position/position_runner_support.py src/mlq/position/position_runner_audit.py tests/unit/position/test_position_runner.py docs/03-execution/00-conclusion-catalog-20260409.md docs/03-execution/A-execution-reading-order-20260409.md docs/03-execution/B-card-catalog-20260409.md docs/03-execution/C-system-completion-ledger-20260409.md docs/03-execution/50-position-data-grade-checkpoint-and-replay-runner-evidence-20260414.md docs/03-execution/50-position-data-grade-checkpoint-and-replay-runner-record-20260414.md docs/03-execution/50-position-data-grade-checkpoint-and-replay-runner-conclusion-20260414.md docs/03-execution/51-pre-portfolio-plan-position-acceptance-gate-card-20260413.md`
   - 结果：通过；本次改动范围没有文件超过 1000 行硬上限，入口联动与严格 doc-first 门禁全部通过

## 实现证据

- `src/mlq/position/position_bootstrap_schema.py`
  - 新增 `position_work_queue / position_checkpoint / position_run_snapshot`
  - 为 `position_run` 补齐 `execution_mode / inserted / reused / rematerialized / queue / checkpoint / summary_json`
- `src/mlq/position/runner.py`
  - 保留原脚本入口名与 `WorkspaceRoots` 路径契约
  - 默认无窗口调用切到 `checkpoint_queue`，显式窗口保留 bounded replay/rematerialize
- `src/mlq/position/position_runner_shared.py`
  - 冻结 runner 摘要结构、默认常量与执行模式判定
- `src/mlq/position/position_runner_support.py`
  - 落实 `alpha -> reference price -> candidate fingerprint -> queue/checkpoint -> run/run_snapshot` 的 data-grade helper
- `tests/unit/position/test_position_runner.py`
  - 新增 queue bootstrap、bounded reuse、reference-price rematerialize 回归

## 证据结构图

```mermaid
flowchart LR
    SEED["受控 alpha/market_base seed"] --> RUN1["run-1 checkpoint_queue inserted"]
    RUN1 --> RUN2["run-2 checkpoint_queue skip unchanged"]
    RUN2 --> PX["更新 reference price 10.5 -> 11.0"]
    PX --> RUN3["run-3 bounded rematerialized"]
    RUN3 --> Q["queue/checkpoint/run_snapshot 查询"]
    Q --> PY["position pytest 15 passed"]
```
