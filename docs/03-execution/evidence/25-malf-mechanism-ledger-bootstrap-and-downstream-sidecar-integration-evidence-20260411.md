# malf 机制层 sidecar 账本 bootstrap 与下游接入证据

证据编号：`25`
日期：`2026-04-11`

## 命令

```text
python -m pytest tests/unit/malf/test_mechanism_runner.py tests/unit/structure/test_runner.py tests/unit/filter/test_runner.py
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python scripts/system/check_doc_first_gating_governance.py
python scripts/system/check_development_governance.py AGENTS.md README.md pyproject.toml src/mlq/malf src/mlq/structure src/mlq/filter scripts/malf tests/unit/malf tests/unit/structure tests/unit/filter docs/03-execution
```

## 关键结果

- `src/mlq/malf/mechanism_runner.py` 与 `scripts/malf/run_malf_mechanism_build.py` 已正式落地，bridge v1 `pas_context_snapshot / structure_candidate_snapshot` 现在可以物化 `malf_mechanism_run / malf_mechanism_checkpoint / pivot_confirmed_break_ledger / same_timeframe_stats_profile / same_timeframe_stats_snapshot`。
- `src/mlq/malf/bootstrap.py` 已扩展机制层表族，并为新增表补齐 bootstrap 列迁移要求。
- `src/mlq/structure/{bootstrap.py,runner.py}` 已接入只读 sidecar：`structure_snapshot` 现在可以附加 `break_confirmation_status / break_confirmation_ref / stats_snapshot_nk / exhaustion_risk_bucket / reversal_probability_bucket`，但不改写既有 `structure_progress_state` 判定。
- `src/mlq/filter/{bootstrap.py,runner.py}` 已接入最小只读 sidecar 透传：`filter_snapshot` 会保留 break/stats sidecar 字段，并仅以 admission note 形式提示，不把 sidecar 升级成新的阻断逻辑。
- 单测新增 `tests/unit/malf/test_mechanism_runner.py`，并扩展 `tests/unit/structure/test_runner.py`、`tests/unit/filter/test_runner.py`；目标测试共 `7` 项全部通过。
- 执行索引检查、`doc-first gating` 与按改动范围运行的开发治理检查通过。

## 产物

- `src/mlq/malf/mechanism_runner.py`
- `scripts/malf/run_malf_mechanism_build.py`
- `src/mlq/structure/bootstrap.py`
- `src/mlq/structure/runner.py`
- `src/mlq/filter/bootstrap.py`
- `src/mlq/filter/runner.py`
- `tests/unit/malf/test_mechanism_runner.py`
- `tests/unit/structure/test_runner.py`
- `tests/unit/filter/test_runner.py`
