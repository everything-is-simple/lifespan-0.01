# system governance historical debt backlog burndown 证据

证据编号：`37`
日期：`2026-04-12`

## 命令

```text
python scripts/system/check_development_governance.py
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python scripts/system/check_doc_first_gating_governance.py
python -m py_compile src/mlq/system/runner.py src/mlq/system/readout_shared.py src/mlq/system/readout_children.py src/mlq/system/readout_snapshot.py src/mlq/system/readout_materialization.py
python scripts/system/check_development_governance.py src/mlq/system/runner.py src/mlq/system/readout_shared.py src/mlq/system/readout_children.py src/mlq/system/readout_snapshot.py src/mlq/system/readout_materialization.py tests/unit/system/test_system_runner.py
pytest tests/unit/system/test_system_runner.py -q
pytest tests/unit/system/test_mainline_truthfulness_revalidation.py -q
python -m py_compile src/mlq/trade/runner.py src/mlq/trade/runtime_shared.py src/mlq/trade/runtime_source.py src/mlq/trade/runtime_execution.py src/mlq/trade/runtime_materialization.py
pytest tests/unit/trade/test_trade_runner.py -q
python -m py_compile src/mlq/alpha/trigger_runner.py src/mlq/alpha/trigger_shared.py src/mlq/alpha/trigger_sources.py src/mlq/alpha/trigger_materialization.py
python scripts/system/check_development_governance.py src/mlq/alpha/trigger_runner.py src/mlq/alpha/trigger_shared.py src/mlq/alpha/trigger_sources.py src/mlq/alpha/trigger_materialization.py tests/unit/alpha/test_runner.py
pytest tests/unit/alpha/test_runner.py -q
python -m py_compile src/mlq/filter/runner.py src/mlq/filter/filter_shared.py src/mlq/filter/filter_source.py src/mlq/filter/filter_materialization.py
python scripts/system/check_development_governance.py src/mlq/filter/runner.py src/mlq/filter/filter_shared.py src/mlq/filter/filter_source.py src/mlq/filter/filter_materialization.py tests/unit/filter/test_runner.py
pytest tests/unit/filter/test_runner.py -q
python -m py_compile src/mlq/malf/mechanism_runner.py src/mlq/malf/mechanism_shared.py src/mlq/malf/mechanism_source.py src/mlq/malf/mechanism_materialization.py
python scripts/system/check_development_governance.py src/mlq/malf/mechanism_runner.py src/mlq/malf/mechanism_shared.py src/mlq/malf/mechanism_source.py src/mlq/malf/mechanism_materialization.py tests/unit/malf/test_mechanism_runner.py
pytest tests/unit/malf/test_mechanism_runner.py -q
python -m py_compile src/mlq/malf/canonical_runner.py src/mlq/malf/canonical_shared.py src/mlq/malf/canonical_source.py src/mlq/malf/canonical_materialization.py
python scripts/system/check_development_governance.py src/mlq/malf/canonical_runner.py src/mlq/malf/canonical_shared.py src/mlq/malf/canonical_source.py src/mlq/malf/canonical_materialization.py tests/unit/malf/test_canonical_runner.py
pytest tests/unit/malf/test_canonical_runner.py -q
```

## 关键结果

- 全仓治理扫描通过，剩余历史债务已经显式收敛为 `LEGACY_HARD_OVERSIZE_BACKLOG` 与 `LEGACY_TARGET_OVERSIZE_BACKLOG`。
- `check_doc_first_gating_governance.py`、`check_execution_indexes.py --include-untracked` 与按路径治理检查通过。
- `system runner` 已拆成 `runner + readout_shared + readout_children + readout_snapshot + readout_materialization` 五段，`src/mlq/system/runner.py` 收缩到 244 行，可从历史硬超长 backlog 移除。
- `trade runner` 已拆成 `runner + runtime_shared + runtime_source + runtime_execution + runtime_materialization` 五段，`src/mlq/trade/runner.py` 收缩到 117 行，现有 `trade` 单测 `2 passed`。
- `alpha trigger runner` 已拆成 `trigger_runner + trigger_shared + trigger_sources + trigger_materialization` 四段，`src/mlq/alpha/trigger_runner.py` 收缩到 708 行，现有 `alpha` 单测 `6 passed`。
- `filter runner` 已拆成 `runner + filter_shared + filter_source + filter_materialization` 四段，`src/mlq/filter/runner.py` 收缩到 707 行，现有 `filter` 单测 `4 passed`。
- `mechanism runner` 已拆成 `runner + mechanism_shared + mechanism_source + mechanism_materialization` 四段，`src/mlq/malf/mechanism_runner.py` 收缩到 171 行，现有 `malf mechanism` 单测 `2 passed`。
- `canonical runner` 已拆成 `runner + canonical_shared + canonical_source + canonical_materialization` 四段，`src/mlq/malf/canonical_runner.py` 收缩到 237 行，现有 `malf canonical` 单测 `2 passed`。
- 当前 `LEGACY_HARD_OVERSIZE_BACKLOG` 已累计减少 6 项；`src/mlq/system/runner.py`、`src/mlq/trade/runner.py`、`src/mlq/alpha/trigger_runner.py`、`src/mlq/filter/runner.py`、`src/mlq/malf/mechanism_runner.py` 与 `src/mlq/malf/canonical_runner.py` 均可从历史硬超长 backlog 移除。
- `37` 卡的 `pytest` 证据统一按串行口径执行，避免多个进程争用 `H:\Lifespan-temp\pytest-tmp`。

## 产物

- `docs/01-design/modules/system/11-governance-historical-debt-backlog-burndown-charter-20260412.md`
- `docs/02-spec/modules/system/11-governance-historical-debt-backlog-burndown-spec-20260412.md`
- `docs/03-execution/37-system-governance-historical-debt-backlog-burndown-card-20260412.md`
- `docs/03-execution/evidence/37-system-governance-historical-debt-backlog-burndown-evidence-20260412.md`
- `docs/03-execution/records/37-system-governance-historical-debt-backlog-burndown-record-20260412.md`
- `docs/03-execution/37-system-governance-historical-debt-backlog-burndown-conclusion-20260412.md`

## 证据结构图

```mermaid
flowchart LR
    CMD["命令执行"] --> OUT["关键结果"]
    OUT --> ART["产物落地"]
    ART --> REF["结论引用"]
```
