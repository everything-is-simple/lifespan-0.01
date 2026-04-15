# structure filter 主线旧版 malf 语义清理证据

证据编号：`38`
日期：`2026-04-13`

## 命令证据

1. `python -m pytest tests/unit/structure/test_runner.py tests/unit/filter/test_runner.py tests/unit/alpha/test_runner.py tests/unit/system/test_mainline_truthfulness_revalidation.py tests/unit/system/test_system_runner.py -q`
   - 结果：`20 passed in 33.30s`
2. `python scripts/system/check_doc_first_gating_governance.py`
   - 结果：通过；当前待施工卡 `38-structure-filter-mainline-legacy-malf-semantic-purge-card-20260413.md` 已具备需求、设计、规格、任务分解与历史账本约束
3. `python scripts/system/check_development_governance.py`
   - 结果：通过；未新增 repo hygiene / 中文化 / 入口新鲜度违规

## 代码证据

- `src/mlq/structure/runner.py`
  - 新增主线 contract 校验：拒绝非 canonical `source_context_table / source_structure_input_table / timeframe`
- `src/mlq/filter/runner.py`
  - 新增主线 contract 校验：拒绝非官方 `structure_snapshot / malf_state_snapshot / D`
- `src/mlq/structure/structure_source.py`
  - 移除主线 bridge-era input/context loader 与 legacy context 映射
  - 只保留 canonical `malf_state_snapshot` 读取路径
- `tests/unit/structure/test_runner.py`
  - 重写为 canonical-only fixture
  - 新增 legacy source rejection 回归
- `tests/unit/filter/test_runner.py`
  - 重写为 canonical-only fixture
  - 新增 legacy source rejection 回归
- `tests/unit/alpha/test_runner.py`
  - 上游 official materialization 改为 canonical `malf_state_snapshot`
- `tests/unit/system/test_mainline_truthfulness_revalidation.py`
  - 主线 truthfulness fixture 去除 `pas_context_snapshot / structure_candidate_snapshot`
- `tests/unit/system/test_system_runner.py`
  - system readout fixture 去除 bridge-era 上游表依赖

## 结果摘要

- `structure / filter` 主线已无法再通过显式参数回退到 `pas_context_snapshot / structure_candidate_snapshot`
- `alpha / system` 的主线上游测试夹具已同步切到 canonical `malf_state_snapshot`
- `queue / checkpoint / sidecar read-only` 回归仍然成立
