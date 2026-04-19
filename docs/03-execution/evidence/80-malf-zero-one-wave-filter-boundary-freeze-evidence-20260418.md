# malf 0/1 波段过滤边界冻结 证据

`证据编号`：`80`
`日期`：`2026-04-19`

## 命令

```text
python -m py_compile src/mlq/malf/zero_one_wave_audit.py tests/unit/malf/test_zero_one_wave_audit.py scripts/malf/run_malf_zero_one_wave_audit.py
python -m pytest tests/unit/malf/test_zero_one_wave_audit.py -q
python scripts/malf/run_malf_zero_one_wave_audit.py --timeframes D W M --sample-limit 5 --summary-path H:/Lifespan-report/malf/zero-one-wave-audit/summary.json --report-path H:/Lifespan-report/malf/zero-one-wave-audit/report.md --detail-path H:/Lifespan-report/malf/zero-one-wave-audit/detail.csv
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python scripts/system/check_doc_first_gating_governance.py
python scripts/system/check_development_governance.py
```

## 关键结果

- `py_compile` 通过；`src/mlq/malf/zero_one_wave_audit.py`、CLI 脚本和单测无语法错误。
- `tests/unit/malf/test_zero_one_wave_audit.py` 通过：`1 passed in 2.45s`。
- 官方三库只读审计成功落地，并生成三份产物：
  - `H:/Lifespan-report/malf/zero-one-wave-audit/summary.json`，`10,217` bytes
  - `H:/Lifespan-report/malf/zero-one-wave-audit/report.md`，`3,248` bytes
  - `H:/Lifespan-report/malf/zero-one-wave-audit/detail.csv`，`3,904,625,201` bytes
- `summary.json` 的全量结果为：
  - 总短 wave：`16,992,169`
  - `same_bar_double_switch`：`243,757`
  - `stale_guard_trigger`：`14,085,407`
  - `next_bar_reflip`：`2,663,005`
  - `non_immediate_boundary_count`：`0`
- 分库统计为：
  - `D`：`14,192,751 / 144,901 / 11,696,738 / 2,351,112 / 0`
  - `W`：`2,421,809 / 70,323 / 2,065,552 / 285,934 / 0`
  - `M`：`377,609 / 28,533 / 323,117 / 25,959 / 0`
  - 以上顺序均为：`total_short / same_bar / stale_guard / next_bar / non_immediate`
- 代表样本仍能按统一合同落证：
  - `same_bar_double_switch`：`000001.SZ wave 6 (1991-11-26 -> 1991-11-26, bar_count=0)`
  - `stale_guard_trigger`：`600654.SH wave 7331 (guard_age_days=12790)`
  - `next_bar_reflip`：`000006.SZ wave 3 (1992-06-17 -> 1992-06-18, bar_count=1)`
- `check_execution_indexes.py --include-untracked` 通过；`doc_first_gating` 通过，当前待施工卡仍为 `92-structure-thin-projection-and-day-binding-card-20260418.md`。
- `check_development_governance.py` 未发现本次改动新增治理违规；退出码为 `1` 的原因仍是仓库既有 backlog：
  - `.specstory/history/2026-04-09_07-18-52Z-position-design-spec-card.md` 超过 `1000` 行硬上限
  - 若干既有文件超过 `800` 行目标上限
  - `src/mlq/data/data_tushare_objective.py` 与 `src/mlq/data/data_tushare_objective_helpers.py` 为已登记中文化历史债务

## 产物

- `src/mlq/malf/zero_one_wave_audit.py`
- `scripts/malf/run_malf_zero_one_wave_audit.py`
- `tests/unit/malf/test_zero_one_wave_audit.py`
- `H:/Lifespan-report/malf/zero-one-wave-audit/summary.json`
- `H:/Lifespan-report/malf/zero-one-wave-audit/report.md`
- `H:/Lifespan-report/malf/zero-one-wave-audit/detail.csv`

## 证据结构图

```mermaid
flowchart LR
    UNIT["unit test"] --> AUDIT["official D/W/M audit"]
    AUDIT --> OUT["summary/report/detail"]
    OUT --> GOV["execution index + governance"]
    GOV --> CON["80 conclusion"]
```

