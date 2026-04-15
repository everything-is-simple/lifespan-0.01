# 42-alpha-family-role-and-malf-alignment 证据
更新时间 `2026-04-13`

## 验证命令

1. `python -m py_compile src/mlq/alpha/__init__.py src/mlq/alpha/family_shared.py src/mlq/alpha/family_source.py src/mlq/alpha/family_materialization.py src/mlq/alpha/family_runner.py tests/unit/alpha/test_family_runner.py tests/unit/alpha/test_pas_runner.py`
   - 通过

2. `set PYTHONPATH=H:\lifespan-0.01 && pytest tests/unit/alpha/test_family_runner.py tests/unit/alpha/test_pas_runner.py -q --basetemp H:\Lifespan-temp\pytest-tmp\card42-alpha-family`
   - `4 passed`

3. `set PYTHONPATH=H:\lifespan-0.01 && pytest tests/unit/alpha -q --basetemp H:\Lifespan-temp\pytest-tmp\card42-alpha-all`
   - `10 passed`

4. `python scripts/system/check_doc_first_gating_governance.py`
   - 通过，卡 `42-alpha-family-role-and-malf-alignment-card-20260413.md` 已具备正式施工前置文档

5. `python scripts/system/check_development_governance.py`
   - 本轮新增改动未引入新的治理违规
   - 全仓全量盘点仍被历史长度债务阻断：
     - `src/mlq/data/data_mainline_incremental_sync.py (1013 行)`
     - `src/mlq/data/data_market_base_materialization.py (829 行)`
     - `src/mlq/data/data_tdxquant.py (867 行)`
     - `tests/unit/data/test_market_base_runner.py (852 行)`

## 关键落表事实

1. `alpha_family_event.payload_json`
   - 已新增正式解释键：
     - `family_role`
     - `malf_alignment`
     - `malf_phase_bucket`
     - `family_bias`
     - `trigger_reason`
     - `structure_anchor_nk`
     - `source_context_fingerprint`

2. `alpha family` 官方输入
   - `family_runner` 已直接读取：
     - `alpha_trigger_event`
     - `alpha_trigger_candidate`
     - `structure_snapshot`
     - `malf_state_snapshot`

3. rematerialize 审计
   - `payload_json.source_context_snapshot` 已保留 trigger / structure / canonical malf 的上游快照
   - 上游结构或 canonical malf 变化时，已有 family event 会触发 `rematerialized`

## 回归结果

1. `tests/unit/alpha/test_family_runner.py`
   - 已覆盖五触发默认 family role
   - 已覆盖 `PB` 第一回调升级逻辑
   - 已覆盖 `BOF/TST` 在冲突上下文中的降级/重算

2. `tests/unit/alpha/test_pas_runner.py`
   - 已确认 `41` 的 PAS detector 输出可直接进入新的 family explanation ledger
