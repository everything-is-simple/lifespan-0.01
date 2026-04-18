# malf 日周月分库路径与表族契约冻结 证据

`证据编号`：`79`
`日期`：`2026-04-18`

## 命令

```text
python scripts/system/check_doc_first_gating_governance.py
python -m pytest tests/unit/malf/test_bootstrap_path_contract.py tests/unit/malf/test_malf_runner.py tests/unit/malf/test_mechanism_runner.py tests/unit/malf/test_wave_life_runner.py tests/unit/malf/test_wave_life_explicit_queue_mode.py -q
python -m pytest tests/unit/structure/test_runner.py tests/unit/filter/test_runner.py tests/unit/alpha/test_pas_runner.py -q
python -m compileall src\mlq\core\paths.py src\mlq\malf\bootstrap.py src\mlq\malf\canonical_runner.py src\mlq\malf\mechanism_runner.py src\mlq\malf\runner.py src\mlq\malf\wave_life_runner.py tests\unit\malf\test_bootstrap_path_contract.py
```

## 关键结果

- doc-first gating 通过，当前待施工卡 `79-malf-day-week-month-ledger-split-path-contract-card-20260418.md` 已具备正式前置文档。
- 新增 `tests/unit/malf/test_bootstrap_path_contract.py`，覆盖 `malf_day / malf_week / malf_month` 官方路径、`malf_ledger_contract`、official native 单值约束与 legacy fallback。
- `malf` 侧回归 `11 passed`，`structure/filter/alpha` 代表性下游回归 `20 passed`。
- `compileall` 通过，`core.paths`、`malf bootstrap` 与四个 runner 无语法错误。

## 产物

- `src/mlq/core/paths.py`
- `src/mlq/malf/bootstrap.py`
- `src/mlq/malf/bootstrap_tables.py`
- `src/mlq/malf/bootstrap_columns.py`
- `tests/unit/malf/test_bootstrap_path_contract.py`

## 证据结构图

```mermaid
flowchart LR
    G["gating"] --> P["pytest"]
    P --> C["compileall"]
    C --> A["79 conclusion"]
```
