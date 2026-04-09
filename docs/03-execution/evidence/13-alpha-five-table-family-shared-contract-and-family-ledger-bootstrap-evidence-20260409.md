# alpha 五表族共享合同与 family ledger bootstrap 证据

证据编号：`13`
日期：`2026-04-09`

## 命令

```text
$env:PYTHONPATH='src'
pytest tests/unit/alpha/test_family_runner.py tests/unit/alpha/test_runner.py tests/unit/position/test_runner.py tests/unit/position/test_bootstrap.py
python -m compileall src/mlq/alpha scripts/alpha

python scripts/alpha/run_alpha_family_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --family bof --family pb --run-id alpha-family-pilot-13-001 --summary-path H:\Lifespan-temp\alpha\alpha-family-pilot-13-001-summary.json
python scripts/alpha/run_alpha_family_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --family bof --family pb --run-id alpha-family-pilot-13-002

$env:PYTHONPATH='src'; @'
... 把 H:\Lifespan-data\malf\malf.duckdb 中 000001.SZ / 2026-04-08 的 structure_candidate_snapshot
... 从 failed_extreme 改回非 failed_extreme，制造受控上游变化 ...
'@ | python -

python scripts/structure/run_structure_snapshot_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --run-id structure-pilot-13-001
python scripts/filter/run_filter_snapshot_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --run-id filter-pilot-13-002
python scripts/alpha/run_alpha_trigger_ledger_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --run-id alpha-trigger-pilot-13-001
python scripts/alpha/run_alpha_family_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --family bof --family pb --run-id alpha-family-pilot-13-003 --summary-path H:\Lifespan-temp\alpha\alpha-family-pilot-13-003-summary.json

$env:PYTHONPATH='src'; @'
... readout alpha_family_run / alpha_family_event / alpha_family_run_event
... 校验 alpha_family_event.trigger_event_nk -> alpha_trigger_event.trigger_event_nk 官方桥接 ...
'@ | python -
```

## 关键结果

- `pytest tests/unit/alpha/test_family_runner.py tests/unit/alpha/test_runner.py tests/unit/position/test_runner.py tests/unit/position/test_bootstrap.py` 通过，结果为 `15 passed`。
- `python -m compileall src/mlq/alpha scripts/alpha` 通过，`scripts/alpha/run_alpha_family_build.py` 已完成脚本入口级语法编译。
- 正式 pilot 首轮真实写入 `H:\Lifespan-data\alpha\alpha.duckdb`：
  - `alpha-family-pilot-13-001` 输出 `inserted=2 / reused=0 / rematerialized=0`
  - `family_scope=["bof","pb"]`
  - `alpha_family_event` 首次建立 `bof_core / pb_core` 两条 family 事实
- unchanged rerun 证明 `reused` 成立：
  - `alpha-family-pilot-13-002` 输出 `inserted=0 / reused=2 / rematerialized=0`
- upstream changed rerun 证明 `rematerialized` 成立：
  - `structure-pilot-13-001` 输出 `reused=1 / rematerialized=1`
  - `filter-pilot-13-002` 输出 `reused=1 / rematerialized=1`
  - `alpha-trigger-pilot-13-001` 输出 `reused=1 / rematerialized=1`
  - `alpha-family-pilot-13-003` 输出 `inserted=0 / reused=1 / rematerialized=1`
- 正式库 readout 证明 family ledger 已成为共享 trigger 事实之上的稳定解释层：
  - `alpha_family_run = [('alpha-family-pilot-13-001','completed',2), ('alpha-family-pilot-13-002','completed',2), ('alpha-family-pilot-13-003','completed',2)]`
  - `alpha_family_run_event = [('alpha-family-pilot-13-001','inserted',2), ('alpha-family-pilot-13-002','reused',2), ('alpha-family-pilot-13-003','rematerialized',1), ('alpha-family-pilot-13-003','reused',1)]`
  - `alpha_family_event` 当前两条事实都保留 `trigger_event_nk`，且 `last_materialized_run_id='alpha-family-pilot-13-003'`
  - `alpha_family_event.trigger_event_nk -> alpha_trigger_event.trigger_event_nk` join readout 为 `[('bof', 1), ('pb', 1)]`
- 真实 pilot 再次暴露 DuckDB 的运行纪律：
  - 共享库上的 runner 需要串行执行
  - 把 `structure / filter / alpha trigger / alpha family` 并行运行会触发文件锁冲突
  - 正式 evidence 已按串行顺序收口

## 产物

- `src/mlq/alpha/bootstrap.py`
- `src/mlq/alpha/family_runner.py`
- `src/mlq/alpha/__init__.py`
- `scripts/alpha/run_alpha_family_build.py`
- `tests/unit/alpha/test_family_runner.py`
- `AGENTS.md`
- `README.md`
- `pyproject.toml`
- `H:\Lifespan-temp\alpha\alpha-family-pilot-13-001-summary.json`
- `H:\Lifespan-temp\alpha\alpha-family-pilot-13-003-summary.json`
