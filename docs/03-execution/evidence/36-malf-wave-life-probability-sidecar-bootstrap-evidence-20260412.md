# malf wave life probability sidecar bootstrap 证据

证据编号：`36`  
日期：`2026-04-12`  
状态：`已补证据`

## 命令

```text
python scripts/system/check_doc_first_gating_governance.py
python -m py_compile src/mlq/malf/bootstrap.py src/mlq/malf/wave_life_runner.py src/mlq/malf/__init__.py scripts/malf/run_malf_wave_life_build.py
python -m py_compile tests/unit/malf/test_wave_life_runner.py
python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card36_malf_suite tests/unit/malf -q
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
```

## 关键结果

- `doc-first gating` 通过；`36-malf-wave-life-probability-sidecar-bootstrap-card-20260411.md` 已具备需求、设计、规格、任务分解与历史账本约束。
- `malf` 正式新增 `wave_life` 五表族，并通过 `py_compile` 验证 `bootstrap / runner / script / export` 入口无语法问题。
- `pytest tests/unit/malf -q` 结果为 `8 passed`，其中新增 `test_wave_life_runner.py` 覆盖：
- 按证据命令原样执行 `python -m pytest -p no:cacheprovider ... tests/unit/malf -q` 结果为 `8 passed, 1 warning`；唯一遗留告警仍是既有 `PytestConfigWarning: Unknown config option: cache_dir`，不影响本卡收口。
- `pytest tests/unit/malf -q` 结果为 `8 passed`，其中新增 `test_wave_life_runner.py` 覆盖：
  - completed wave profile 与 active snapshot 分离建模
  - 默认 queue/checkpoint 路径
  - canonical source fingerprint 变化触发 requeue/rematerialize
- `malf_wave_life_snapshot` 已正式输出 `wave_life_percentile / remaining_life_bars_p50 / remaining_life_bars_p75 / termination_risk_bucket / sample_size / sample_version`。
- `malf_wave_life_profile` 已正式支持 completed sample 与 `malf_same_level_stats` fallback 两条只读来源。

## 产物

- `docs/03-execution/36-malf-wave-life-probability-sidecar-bootstrap-conclusion-20260412.md`
- `docs/03-execution/records/36-malf-wave-life-probability-sidecar-bootstrap-record-20260412.md`
- `src/mlq/malf/bootstrap.py`
- `src/mlq/malf/wave_life_runner.py`
- `src/mlq/malf/__init__.py`
- `scripts/malf/run_malf_wave_life_build.py`
- `tests/unit/malf/test_wave_life_runner.py`

## 证据结构图

```mermaid
flowchart LR
    GATE["doc-first gating"] --> CODE["wave life tables + runner"]
    CODE --> TEST["8 passed"]
    TEST --> INDEX["execution indexes aligned"]
    INDEX --> CON["36 结论生效"]
```
