# structure/filter 官方 ledger replay 与 smoke 硬化记录

记录编号：`44`
日期：`2026-04-13`

## 实施记录

1. 先对真实官方库做受控复制，而不是继续停留在内存夹具
   - 把 `H:\Lifespan-data\malf / structure / filter` 复制到 `H:\Lifespan-temp\card44\controlled-data`
   - 保留“官方 DuckDB 实体形态”，但避免直接污染当前 `H:\Lifespan-data`
2. 在受控复制上暴露并修复 legacy official `structure_snapshot` 迁移阻断
   - 首轮 smoke 发现复制自官方库的 `structure_snapshot` 仍带 `malf_context_4 / lifecycle_rank_*` 等 compat-only `NOT NULL` 列
   - 默认 queue 插入 canonical row 时因此触发 `NOT NULL constraint failed: structure_snapshot.malf_context_4`
   - `44` 在 `structure bootstrap` 内新增一次性 canonical 化迁移：重建 `structure_snapshot / structure_run_snapshot`，丢弃不兼容的 bridge-era 物理列
3. 冻结 `structure` 的官方 ledger replay 语义
   - 首轮默认 queue 运行补齐 `structure_work_queue / structure_checkpoint`
   - 月级 `malf` 指纹推进后，第二轮默认 queue 触发 `source_fingerprint_changed`，并完成 `rematerialized`
4. 冻结 `filter` 的官方 ledger replay 语义
   - 首轮默认 queue 运行补齐 `filter_work_queue / filter_checkpoint`
   - `structure checkpoint` 指纹推进后，第二轮默认 queue 同样触发 `source_fingerprint_changed` 并完成 `rematerialized`
5. 新增回归测试，避免后续只在“新库”上通过
   - `structure` 新增 legacy snapshot schema 迁移回归
   - `filter` 新增 legacy official DB 缺 queue/checkpoint 时的 bootstrap 回归

## 边界

- `44` 不升级官方 `malf` 真值库；当前 `H:\Lifespan-data\malf\malf.duckdb` 仍是 bridge-era 表族，因此 smoke 证据明确登记为“official copy + controlled canonical upstream”。
- `44` 不处理 `alpha formal signal` 的稳定 producer 合同；该项继续留在 `45`。
- `44` 不进入 `position / portfolio_plan / trade / system`。
