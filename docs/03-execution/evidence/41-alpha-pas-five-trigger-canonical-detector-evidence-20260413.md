# 41-alpha-pas-five-trigger-canonical-detector 证据
更新时间 `2026-04-13`

## 验证命令

1. `python -m py_compile src/mlq/alpha/bootstrap.py src/mlq/alpha/__init__.py src/mlq/alpha/pas_shared.py src/mlq/alpha/pas_detectors.py src/mlq/alpha/pas_source.py src/mlq/alpha/pas_materialization.py src/mlq/alpha/pas_runner.py scripts/alpha/run_alpha_pas_five_trigger_build.py tests/unit/alpha/test_pas_runner.py`
   - 通过

2. `python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card41_alpha_pas tests/unit/alpha/test_pas_runner.py -q`
   - `2 passed`

3. `python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card41_alpha_pas tests/unit/alpha/test_pas_runner.py tests/unit/alpha/test_runner.py tests/unit/alpha/test_family_runner.py tests/unit/system/test_mainline_truthfulness_revalidation.py tests/unit/system/test_system_runner.py -q`
   - `13 passed`

4. `python scripts/system/check_doc_first_gating_governance.py`
   - 通过，当前待施工卡仍为 `41-alpha-pas-five-trigger-canonical-detector-card-20260413.md`

## 关键落表事实

1. `alpha_trigger_candidate`
   - 官方 producer 已由 `run_alpha_pas_five_trigger_build.py` 负责物化
   - candidate 扩展列包含：
     - `family_code`
     - `trigger_strength`
     - `price_context_json`
     - `structure_context_json`
     - `detector_trace_json`
     - `source_price_fingerprint`

2. `alpha_pas_trigger_*`
   - 已新增：
     - `alpha_pas_trigger_run`
     - `alpha_pas_trigger_work_queue`
     - `alpha_pas_trigger_checkpoint`
     - `alpha_pas_trigger_run_candidate`

3. downstream 对接
   - `run_alpha_trigger_build` 继续消费 `alpha_trigger_candidate`
   - `run_alpha_family_build` 已能从 candidate 扩展列读到 `trigger_strength / structure_context_json`

## 治理结果

1. `check_doc_first_gating_governance.py`
   - 通过

2. `check_development_governance.py`
   - 本轮新增文件未引入新的长度或治理违规
   - 全仓全量盘点仍被历史债务阻断：
     - `src/mlq/data/data_mainline_incremental_sync.py (1013 行)`
