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
- `src/mlq/malf/runner.py` 已收缩到 `164` 行。
- 新增 `src/mlq/malf/snapshot_shared.py`、`src/mlq/malf/snapshot_source.py`、`src/mlq/malf/snapshot_materialization.py`，分别承接 bridge v1 snapshot 的共享常量、source 读取/派生与落表审计职责。
- `src/mlq/malf/bootstrap.py` 已收缩到 `545` 行。
- 新增 `src/mlq/malf/bootstrap_tables.py`、`src/mlq/malf/bootstrap_columns.py`，分别承接表名常量与 required-column 补列映射。
- `src/mlq/alpha/family_runner.py` 已收缩到 `134` 行。
- 新增 `src/mlq/alpha/family_shared.py`、`src/mlq/alpha/family_source.py`、`src/mlq/alpha/family_materialization.py`，分别承接共享常量、上游读取与 family ledger 落表职责。

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
- `python -m py_compile src/mlq/malf/snapshot_shared.py src/mlq/malf/snapshot_source.py src/mlq/malf/snapshot_materialization.py src/mlq/malf/runner.py`
- `python scripts/system/check_development_governance.py src/mlq/malf/runner.py src/mlq/malf/snapshot_shared.py src/mlq/malf/snapshot_source.py src/mlq/malf/snapshot_materialization.py scripts/system/development_governance_legacy_backlog.py AGENTS.md README.md pyproject.toml docs/03-execution/37-system-governance-historical-debt-backlog-burndown-card-20260412.md docs/03-execution/37-system-governance-historical-debt-backlog-burndown-conclusion-20260412.md docs/03-execution/evidence/37-system-governance-historical-debt-backlog-burndown-evidence-20260412.md docs/03-execution/records/37-system-governance-historical-debt-backlog-burndown-record-20260412.md`
- `python scripts/system/check_development_governance.py`
- `python scripts/system/check_doc_first_gating_governance.py`
- `python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked`
- `python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card37_malf_runner_20260412 tests/unit/malf/test_malf_runner.py -q`
- `python -m py_compile src/mlq/malf/bootstrap.py src/mlq/malf/bootstrap_tables.py src/mlq/malf/bootstrap_columns.py`
- `python scripts/system/check_development_governance.py src/mlq/malf/bootstrap.py src/mlq/malf/bootstrap_tables.py src/mlq/malf/bootstrap_columns.py`
- `python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card37_malf_bootstrap_20260412 tests/unit/malf/test_malf_runner.py tests/unit/malf/test_mechanism_runner.py tests/unit/malf/test_wave_life_runner.py tests/unit/malf/test_canonical_runner.py -q`
- `python -m py_compile src/mlq/alpha/family_runner.py src/mlq/alpha/family_shared.py src/mlq/alpha/family_source.py src/mlq/alpha/family_materialization.py`
- `python scripts/system/check_development_governance.py src/mlq/alpha/family_runner.py src/mlq/alpha/family_shared.py src/mlq/alpha/family_source.py src/mlq/alpha/family_materialization.py`
- `python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card37_alpha_family_20260412 tests/unit/alpha/test_family_runner.py -q`

## 验证补充
- `py_compile`、按路径治理检查、全仓治理检查、doc-first gating 与执行索引检查均通过。
- `tests/unit/malf/test_malf_runner.py` 串行通过，结果为 `2 passed`。
- `malf bootstrap` 相关串行 pytest 通过，结果为 `8 passed`。
- `alpha family` 相关串行 pytest 通过，结果为 `2 passed`。
- `Unknown config option: cache_dir` 警告来自 `-p no:cacheprovider`，不是仓库配置漂移。

## 验证结果摘要
- `py_compile`：通过。
- 改动路径治理检查：通过。
- 全仓治理检查：通过。
- `data` 串行 pytest：`15 passed`，另有 `Unknown config option: cache_dir` 警告；该警告来自 `-p no:cacheprovider` 口径，不是仓库配置漂移。
- `data bootstrap` 目标线治理检查：通过。

## 治理台账状态
- `LEGACY_HARD_OVERSIZE_BACKLOG`：已清零。
- `LEGACY_TARGET_OVERSIZE_BACKLOG`：剩余 `1` 项。
