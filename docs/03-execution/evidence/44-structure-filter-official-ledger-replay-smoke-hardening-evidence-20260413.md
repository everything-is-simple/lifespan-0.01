# structure/filter 官方 ledger replay 与 smoke 硬化证据

证据编号：`44`
日期：`2026-04-13`

## 命令证据

1. `python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card44_structure tests\unit\structure\test_runner.py -q`
   - 结果：`7 passed, 1 warning in 12.32s`
2. `python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card44_filter tests\unit\filter\test_runner.py -q`
   - 结果：`6 passed, 1 warning in 10.97s`
3. 受控 official smoke 初始化脚本
   - 动作：复制 `H:\Lifespan-data\{malf,structure,filter}` 到 `H:\Lifespan-temp\card44\controlled-data`
   - 动作：为 `CARD44.SZ` 补最小 canonical `malf_state_snapshot + malf_canonical_checkpoint`
4. `python scripts\structure\run_structure_snapshot_build.py --run-id card44-structure-run-1 --summary-path H:\Lifespan-temp\card44\summary\structure-run-1.json`
   - 环境：`PYTHONPATH=src`，`LIFESPAN_DATA_ROOT=H:\Lifespan-temp\card44\controlled-data`
   - 结果：`execution_mode=checkpoint_queue`，`inserted_count=1`，`queue_enqueued_count=1`，`checkpoint_upserted_count=1`
5. `python scripts\filter\run_filter_snapshot_build.py --run-id card44-filter-run-1 --summary-path H:\Lifespan-temp\card44\summary\filter-run-1.json`
   - 环境：`PYTHONPATH=src`，`LIFESPAN_DATA_ROOT=H:\Lifespan-temp\card44\controlled-data`
   - 结果：`execution_mode=checkpoint_queue`，`inserted_count=1`，`queue_enqueued_count=1`，`checkpoint_upserted_count=1`
6. 受控 replay source 更新脚本
   - 动作：把 `CARD44.SZ` 的月级 `malf_state_snapshot` 从 `牛逆/down/trigger` 更新为 `熊逆/down/trigger`
   - 动作：把 `malf_canonical_checkpoint(timeframe='M')` 的 `last_run_id` 推进到 `malf-run-b`
7. `python scripts\structure\run_structure_snapshot_build.py --run-id card44-structure-run-2 --summary-path H:\Lifespan-temp\card44\summary\structure-run-2.json`
   - 结果：`rematerialized_count=1`，`queue_enqueued_count=1`，`checkpoint_upserted_count=1`
8. `python scripts\filter\run_filter_snapshot_build.py --run-id card44-filter-run-2 --summary-path H:\Lifespan-temp\card44\summary\filter-run-2.json`
   - 结果：`rematerialized_count=1`，`queue_enqueued_count=1`，`checkpoint_upserted_count=1`
9. 受控 DuckDB 查询
   - `structure`：`structure_work_queue / structure_checkpoint` 已物理存在；`structure_snapshot` 中 legacy 列 `malf_context_4` 计数为 `0`
   - `structure`：最新 queue 行为 `queue_status='completed'`、`dirty_reason='source_fingerprint_changed'`
   - `structure`：最新 checkpoint 为 `last_run_id='card44-structure-run-2'`、`tail_start_bar_dt='2026-03-31'`、`tail_confirm_until_dt='2026-04-08'`
   - `filter`：`filter_work_queue / filter_checkpoint` 已物理存在
   - `filter`：最新 queue 行为 `queue_status='completed'`、`dirty_reason='source_fingerprint_changed'`
   - `filter`：最新 checkpoint 为 `last_run_id='card44-filter-run-2'`、`tail_start_bar_dt='2026-03-31'`、`tail_confirm_until_dt='2026-04-08'`

## 实现证据

- `src/mlq/structure/bootstrap.py`
  - 为 legacy official `structure_snapshot` 增加 canonical schema 标准化迁移
  - 当遇到 bridge-era compat 列或缺失 canonical 主字段时，重建 `structure_snapshot / structure_run_snapshot`
  - 迁移后只保留当前正式 canonical 列族，允许默认 queue 模式在复制自官方库的 DuckDB 上直接续跑
- `tests/unit/structure/test_runner.py`
  - 新增 legacy official `structure_snapshot` 迁移回归
  - 验证默认 queue 模式会补齐 `structure_work_queue / structure_checkpoint`
  - 验证 legacy `malf_context_4` 列会被物理清理
- `tests/unit/filter/test_runner.py`
  - 新增 legacy official `filter` DB 缺 `filter_work_queue / filter_checkpoint` 时的 bootstrap 回归
  - 验证默认 queue 模式可直接在旧官方库副本上建账并续跑
- `H:\Lifespan-temp\card44\summary\*.json`
  - 保留 `structure/filter` 四次 smoke 运行摘要，可直接回放 `inserted -> rematerialized` 审计读数

## 证据结构图

```mermaid
flowchart LR
    COPY["复制 official DB 到 controlled-data"] --> STR1["structure run 1 inserted"]
    STR1 --> FLT1["filter run 1 inserted"]
    FLT1 --> UPD["更新 malf 月级指纹"]
    UPD --> STR2["structure run 2 rematerialized"]
    STR2 --> FLT2["filter run 2 rematerialized"]
    FLT2 --> Q["queue/checkpoint 查询"]
```
