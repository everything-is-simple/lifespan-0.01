# structure filter alpha rebind to canonical malf 记录

日期：`2026-04-11`
状态：`已补记录`

## 实施记录

1. 把 `structure` 默认上游从 bridge v1 切到 canonical `malf_state_snapshot`，并把当前 canonical `major_state / current_hh_count / current_ll_count` 映射成下游仍在消费的兼容字段。
2. 为避免把旧测试样本全部打碎，只保留一个受限兼容层：
   - 只有在 canonical 表缺失时，`structure` 才回退到 `pas_context_snapshot / structure_candidate_snapshot`
   - 只有在 canonical 表缺失时，`filter` 才回退到 `pas_context_snapshot`
3. 关闭 `alpha formal signal` 对 `pas_context_snapshot` 的默认 fallback；现在它默认只承认 `alpha trigger + filter_snapshot + structure_snapshot`。
4. 新增 `tests/unit/system/test_canonical_malf_rebind.py`，专门验证“无 bridge-v1 表、只有 canonical 表”的默认主线。

## 结果判断

1. `structure / filter / alpha` 的默认正式入口已经从 bridge-v1 改绑到 canonical malf。
2. bridge-v1 不再是默认正式真值，只保留成受控兼容回退。
3. `32` 现在可以直接承接 canonical 后的 downstream truthfulness revalidation。
