# market_base backward 全历史修缮与补全 证据

证据编号：`73`
日期：`2026-04-16`

## 命令

```text
python scripts/system/check_doc_first_gating_governance.py

$env:PYTHONUTF8='1'
$env:PYTEST_ADDOPTS='--basetemp=H:\Lifespan-temp\pytest-tmp'
python -m pytest tests/unit/data/test_market_base_runner.py -q

$env:PYTHONUTF8='1'
python scripts/data/run_market_base_build.py `
  --asset-type stock `
  --adjust-method backward `
  --build-mode full `
  --limit 0 `
  --run-id card73-stock-backward-full-history-20260416 `
  --summary-path H:\Lifespan-temp\data\card73\stock-backward-market-base-full-summary.json

$env:PYTHONUTF8='1'
python - <<'PY'
只读连接 H:\Lifespan-data\raw\raw_market.duckdb 与
H:\Lifespan-data\base\market_base.duckdb，按 stock/index/block 对比
raw backward 与 market_base backward 的 row_count / instrument_count /
min_date / max_date / missing_from_base / extra_in_base / value_mismatch，
并写出 H:\Lifespan-temp\data\card73\post-backward-coverage-audit.json。
PY
```

## 关键结果

- doc-first gating：通过，当前待施工卡 `73-market-base-backward-full-history-backfill-card-20260416.md` 已具备需求、设计、规格、任务分解与历史账本约束。
- 单元测试：`tests/unit/data/test_market_base_runner.py -q` 通过，`10 passed in 13.30s`。
- 正式补库 run：
  - `run_id = card73-stock-backward-full-history-20260416`
  - `asset_type = stock`
  - `adjust_method = backward`
  - `build_mode = full`
  - `source_scope_kind = full`
  - `source_row_count = 16348113`
  - `inserted_count = 15955635`
  - `reused_count = 392478`
  - `rematerialized_count = 0`
- 补后覆盖审计：
  - `stock`：raw/base 均为 `16,348,113` 行、`5,501` 标的、`1990-12-19 -> 2026-04-10`，`missing_from_base = 0`，`extra_in_base = 0`，`value_mismatch = 0`。
  - `index`：raw/base 均为 `377,711` 行、`100` 标的、`1990-12-19 -> 2026-04-10`，`missing_from_base = 0`，`extra_in_base = 0`，`value_mismatch = 0`。
  - `block`：raw/base 均为 `468,542` 行、`127` 标的、`2011-01-04 -> 2026-04-10`，`missing_from_base = 0`，`extra_in_base = 0`，`value_mismatch = 0`。

## 产物

- `H:\Lifespan-temp\data\card73\stock-backward-market-base-full-summary.json`
- `H:\Lifespan-temp\data\card73\post-backward-coverage-audit.json`
- `H:\Lifespan-data\base\market_base.duckdb`
  - `stock_daily_adjusted(adjust_method='backward')` 已从 `2010` pilot 窗口补齐到全历史 `1990-12-19 -> 2026-04-10`。
- `src/mlq/data/data_market_base_runner.py`
  - 新增局部 `full` 防误删 guardrail。
- `src/mlq/data/data_raw_runner.py`
  - 兼容 `stock-day / index-day / block-day` 离线源目录布局。
- `tests/unit/data/test_market_base_runner.py`
  - 覆盖局部 `full` 不删除范围外历史，以及 `*-day` source layout。

## 证据结构图

```mermaid
flowchart LR
    CMD[命令执行] --> OUT[关键结果]
    OUT --> ART[产物落地]
    ART --> REF[结论引用]
```
