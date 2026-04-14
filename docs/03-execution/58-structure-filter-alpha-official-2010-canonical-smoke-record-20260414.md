# structure filter alpha official 2010 canonical smoke 记录
`记录编号`：`58`
`日期`：`2026-04-14`

## 实施记录

1. 先用 bounded smoke 验证 canonical rebind 是否生效。
   - `structure` 与 `filter` 先在 `5,000` 条窗口样本上确认默认来源不再回退 bridge-v1
   - `alpha detector/trigger/family/formal signal` 也先做了一轮小样本探路
2. 识别出 bounded full-window 路径不适合作为正式主线执行方式。
   - `card58-structure-2010-002-full` 连续运行约 1 小时后仍未结束
   - 该进程被手动终止，并把对应 `structure_run` 审计状态显式回填为 `failed`
3. 正式改走 data-grade `queue/checkpoint` 路径。
   - `structure` 以 `malf_canonical_checkpoint` 为 scope 源，完整消费 `1,833` 个 `2010` 标的 scope
   - `filter` 以 `structure_checkpoint` 为 scope 源，完整消费 `1,833` 个 `2010` 标的 scope
4. 在 queue 路径之上完成 alpha 全链。
   - `detector` 从 `6,833` 条 `filter_snapshot` 中筛出 `35` 条 trigger candidate
   - `trigger / family / formal signal` 串行完成，避免 DuckDB 单写锁冲突
5. 修正执行期审计细节。
   - 由于 `alpha.duckdb` 是单写锁库，`family / formal signal` 不再并行运行
   - `structure_run` 中被外部终止的 bounded full 记录已改成 `failed`，并注明被 `card58-structure-2010-003-queue` 取代

## 边界

- `58` 的正式全窗口结论建立在 queue/checkpoint 路径上，不建立在 bounded 全量脚本上。
- `58` 只验证到 `alpha formal signal`；是否把 `2010` pilot 作为后续三年窗口模板，还要由 `59` truthfulness gate 裁决。
- `58` 不处理 bridge-v1 物理删除，只裁决“默认正式来源是否已经绑到 canonical”。
