# market_base 分批建仓治理与 runner 修缮 证据

证据编号：`74`
日期：`2026-04-16`

## 命令

```text
python scripts/system/check_doc_first_gating_governance.py

$env:PYTHONUTF8='1'
$env:PYTEST_ADDOPTS='--basetemp=H:/Lifespan-temp/pytest-tmp'
python -m pytest `
  tests/unit/data/test_market_base_batched_runner.py `
  tests/unit/data/test_market_base_runner.py `
  -q
```

## 关键结果

- doc-first gating：通过，当前待施工卡 `74-market-base-batched-bootstrap-governance-card-20260416.md` 已具备需求、设计、规格、任务分解与历史账本约束。
- 单元测试：`13 passed in 17.94s`。
- 新增测试覆盖：
  - `run_tdx_asset_raw_ingest_batched(..., batch_size=1)` 会生成 3 个 raw ingest child run，每个 child run 独立写入 `raw_ingest_run`。
  - `run_asset_market_base_build_batched(..., batch_size=1)` 会生成 3 个 instrument child run，每个 child run 独立写入 `base_build_run`。
  - instrument scoped `full` 只删除当前标的作用域内已从 raw 消失的行，不删除其他标的历史。
- CLI 口径：
  - `scripts/data/run_tdx_asset_raw_ingest.py --batch-size N --run-mode full`
  - `scripts/data/run_market_base_build.py --batch-size N --build-mode full --limit 0`
  - parent summary 汇总 `batch_count / instrument_count / source_row_count / inserted_count / reused_count / rematerialized_count / child_runs`。

## 产物

- `src/mlq/data/data_raw_runner.py`
  - 新增 `run_tdx_asset_raw_ingest_batched(...)`。
  - 批次模式只读取候选文件名与 code 清单，再按 code batch 调用 child raw ingest。
- `scripts/data/run_tdx_asset_raw_ingest.py`
  - 新增 `--batch-size` CLI 参数。
- `src/mlq/data/data_market_base_runner.py`
  - 新增 `run_asset_market_base_build_batched(...)`。
  - 批次模式只读取 distinct code 清单，再按 code batch 调用 child build。
- `src/mlq/data/data_market_base_materialization.py`
  - scoped full 删除逻辑限制在当前 instruments/date scope。
- `scripts/data/run_market_base_build.py`
  - 新增 `--batch-size` CLI 参数。
- `tests/unit/data/test_market_base_batched_runner.py`
  - 新增批次建仓与 scoped full 清理测试。

## 证据结构图

```mermaid
flowchart LR
    CMD[命令执行] --> OUT[关键结果]
    OUT --> ART[产物落地]
    ART --> REF[结论引用]
```
