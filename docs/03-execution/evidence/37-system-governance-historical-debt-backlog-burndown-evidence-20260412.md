# system governance historical debt backlog burndown 证据

日期：`2026-04-12`

## 本轮新增证据
- `src/mlq/data/runner.py` 已收缩为 `31` 行 formal orchestrator。
- 新增：
  - `src/mlq/data/data_shared.py`
  - `src/mlq/data/data_common.py`
  - `src/mlq/data/data_raw_support.py`
  - `src/mlq/data/data_raw_runner.py`
  - `src/mlq/data/data_tdxquant.py`
  - `src/mlq/data/data_market_base_scope.py`
  - `src/mlq/data/data_market_base_materialization.py`
  - `src/mlq/data/data_market_base_runner.py`
- 删除：
  - `tests/unit/data/test_data_runner.py`
- 新增拆分测试：
  - `tests/unit/data/test_raw_ingest_runner.py`
  - `tests/unit/data/test_tdxquant_runner.py`
  - `tests/unit/data/test_market_base_runner.py`
- `src/mlq/data/bootstrap.py` 已收缩到 `725` 行。
- 新增 `src/mlq/data/data_bootstrap_maintenance.py`，承接 cleanup / deduplicate / constraint repair helper。

## 验证命令
- `python -m py_compile src/mlq/data/data_shared.py src/mlq/data/data_common.py src/mlq/data/data_raw_support.py src/mlq/data/data_raw_runner.py src/mlq/data/data_tdxquant.py src/mlq/data/data_market_base_scope.py src/mlq/data/data_market_base_materialization.py src/mlq/data/data_market_base_runner.py src/mlq/data/runner.py tests/unit/data/test_raw_ingest_runner.py tests/unit/data/test_tdxquant_runner.py tests/unit/data/test_market_base_runner.py`
- `python scripts/system/check_development_governance.py src/mlq/data/runner.py src/mlq/data/data_shared.py src/mlq/data/data_common.py src/mlq/data/data_raw_support.py src/mlq/data/data_raw_runner.py src/mlq/data/data_tdxquant.py src/mlq/data/data_market_base_scope.py src/mlq/data/data_market_base_materialization.py src/mlq/data/data_market_base_runner.py tests/unit/data/test_raw_ingest_runner.py tests/unit/data/test_tdxquant_runner.py tests/unit/data/test_market_base_runner.py`
- `python scripts/system/check_development_governance.py`
- `python scripts/system/check_doc_first_gating_governance.py`
- `python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked`
- `python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card37_data_20260412 tests/unit/data/test_raw_ingest_runner.py tests/unit/data/test_tdxquant_runner.py tests/unit/data/test_market_base_runner.py -q`
- `python -m py_compile src/mlq/data/bootstrap.py src/mlq/data/data_bootstrap_maintenance.py`
- `python scripts/system/check_development_governance.py src/mlq/data/bootstrap.py src/mlq/data/data_bootstrap_maintenance.py`
- `python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card37_data_bootstrap_20260412 tests/unit/data/test_raw_ingest_runner.py tests/unit/data/test_tdxquant_runner.py tests/unit/data/test_market_base_runner.py -q`

## 验证结果摘要
- `py_compile`：通过。
- 改动路径治理检查：通过。
- 全仓治理检查：通过。
- `data` 串行 pytest：`15 passed`，另有 `Unknown config option: cache_dir` 警告；该警告来自 `-p no:cacheprovider` 口径，不是仓库配置漂移。
- `data bootstrap` 目标线治理检查：通过。

## 治理台账状态
- `LEGACY_HARD_OVERSIZE_BACKLOG`：已清零。
- `LEGACY_TARGET_OVERSIZE_BACKLOG`：剩余 `4` 项。
