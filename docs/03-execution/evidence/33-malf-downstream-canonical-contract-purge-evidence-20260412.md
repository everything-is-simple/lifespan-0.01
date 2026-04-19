# malf downstream canonical contract purge 证据

证据编号：`33`
日期：`2026-04-12`
状态：`已补证据`

## 命令

```text
python scripts/system/check_doc_first_gating_governance.py
$env:PYTHONPATH='src'; python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card33_structure tests/unit/structure/test_runner.py -q
$env:PYTHONPATH='src'; python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card33_filter tests/unit/filter/test_runner.py -q
$env:PYTHONPATH='src'; python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card33_alpha tests/unit/alpha/test_runner.py tests/unit/alpha/test_family_runner.py -q
$env:PYTHONPATH='src'; python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card33_system tests/unit/system/test_canonical_malf_rebind.py tests/unit/system/test_mainline_truthfulness_revalidation.py tests/unit/system/test_system_runner.py -q
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
```

## 关键结果

- `doc-first gating` 通过；在 `33` 收口并把执行指针推进到 `34` 后，当前待施工卡 `34` 也已具备需求、设计、规格、任务分解与历史账本约束。
- `structure` 用例 `3 passed`，`filter` 用例 `2 passed`，`alpha` 用例 `7 passed`，`system` 用例 `4 passed`，合计 `16 passed`，无失败。
- `structure_snapshot / filter_snapshot` 正式输出已切到 canonical `major_state / trend_direction / reversal_stage / wave_id / current_hh_count / current_ll_count`，旧字段壳不再主导正式判断。
- `alpha_trigger_event / alpha_formal_signal_event` 已按 canonical 上游上下文物化；默认 `pas_context_snapshot` fallback 已移除。
- `alpha_formal_signal_event` 保留 `malf_context_4 / lifecycle_rank_*` 仅作为派生兼容字段，供当前 `position` 过渡消费；它们不再是正式真值字段。
- `alpha_family_event.payload_json` 已把 `upstream_context_fingerprint` 结构化落入 payload，不再以转义字符串隐藏 canonical rematerialized 证据。
- 执行索引检查通过，`conclusion / evidence / card / records / reading-order / completion-ledger` 已与当前正式文件一致。
- 唯一告警为 `PytestConfigWarning: Unknown config option: cache_dir`，属于非阻断噪音。

## 产物

- `docs/03-execution/33-malf-downstream-canonical-contract-purge-conclusion-20260412.md`
- `docs/03-execution/records/33-malf-downstream-canonical-contract-purge-record-20260412.md`
- `src/mlq/structure/runner.py`
- `src/mlq/filter/runner.py`
- `src/mlq/alpha/runner.py`
- `src/mlq/alpha/trigger_runner.py`
- `src/mlq/alpha/family_runner.py`

## 证据结构图

```mermaid
flowchart LR
    DOC["doc-first gating"] --> TEST["16 passed"]
    TEST --> CONTRACT["canonical structure/filter/alpha contracts"]
    CONTRACT --> COMPAT["legacy fields downgraded to compat-only"]
    COMPAT --> CON["33 结论引用"]
```
