# structure filter alpha rebind to canonical malf 结论

日期：`2026-04-11`
状态：`已裁决`

`31` 已正式通过。`structure / filter / alpha` 现在默认改绑到 canonical `malf v2`，bridge-v1 不再承担默认正式真值职责。

本次正式落地内容：

1. `structure`
   - 默认 `source_context_table / source_structure_input_table` 切到 `malf_state_snapshot`
   - 默认 `source_timeframe='D'`
   - canonical `major_state / current_hh_count / current_ll_count` 会映射出下游仍需保留的兼容字段
2. `filter`
   - 默认 `source_context_table` 切到 canonical `malf_state_snapshot`
   - bridge v1 `pas_context_snapshot` 只在 canonical 表缺失时兼容回退
3. `alpha`
   - `alpha trigger` 继续只读官方 `filter_snapshot + structure_snapshot`
   - `alpha formal signal` 默认关闭 `pas_context_snapshot` fallback
4. 新增 `tests/unit/system/test_canonical_malf_rebind.py`
   - 证明在没有 bridge-v1 表的前提下，默认 `structure -> filter -> alpha` 主线可直接跑通

保留边界：

1. `structure_snapshot` 仍保留 `malf_context_4 / lifecycle_rank_* / source_context_nk`，但这些字段现在只代表 canonical-downstream 兼容映射与审计指针，不得反向定义 `malf core`
2. bridge-v1 表与机制层 sidecar 仍可保留为显式兼容件，但不再是默认正式上游
3. `32` 之前，整条主链仍需做 canonical 后的 truthfulness revalidation

验证已通过：

```bash
pytest tests/unit/malf/test_canonical_runner.py tests/unit/structure/test_runner.py tests/unit/filter/test_runner.py tests/unit/alpha/test_runner.py tests/unit/system/test_canonical_malf_rebind.py tests/unit/system/test_mainline_truthfulness_revalidation.py tests/unit/system/test_system_runner.py -q
```

结果：`16 passed`
