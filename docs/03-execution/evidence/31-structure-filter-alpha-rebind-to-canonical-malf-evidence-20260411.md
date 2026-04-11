# structure filter alpha rebind to canonical malf 证据

日期：`2026-04-11`
状态：`已补证据`

## 代码与合同证据

1. `src/mlq/structure/runner.py`
   - 默认 `source_context_table / source_structure_input_table` 改为 `malf_state_snapshot`
   - 新增 `source_timeframe='D'`
   - canonical `major_state / current_hh_count / current_ll_count` 现在会映射成下游兼容字段 `malf_context_4 / lifecycle_rank_* / new_high_count / new_low_count`
   - bridge v1 `pas_context_snapshot / structure_candidate_snapshot` 只在 canonical 表缺失时回退
2. `src/mlq/filter/runner.py`
   - 默认 `source_context_table` 改为 `malf_state_snapshot`
   - canonical 缺表时才回退到 `pas_context_snapshot`
3. `src/mlq/alpha/runner.py`
   - `DEFAULT_ALPHA_FORMAL_SIGNAL_FALLBACK_CONTEXT_TABLE = None`
   - `alpha formal signal` 默认不再回读 `pas_context_snapshot`

## 测试证据

执行命令：

```bash
pytest tests/unit/malf/test_canonical_runner.py tests/unit/structure/test_runner.py tests/unit/filter/test_runner.py tests/unit/alpha/test_runner.py tests/unit/system/test_canonical_malf_rebind.py tests/unit/system/test_mainline_truthfulness_revalidation.py tests/unit/system/test_system_runner.py -q
```

结果摘要：

1. `16 passed`
2. 新增 `tests/unit/system/test_canonical_malf_rebind.py`
   - 证明在只有 canonical `malf_state_snapshot`、没有 bridge-v1 表的前提下，`structure -> filter -> alpha` 默认主线可直接跑通
3. 原有 `structure / filter / alpha / system` 单测继续通过
   - 证明 canonical 默认入口成立后，legacy bounded 样本仍能通过兼容回退维持测试可复现
