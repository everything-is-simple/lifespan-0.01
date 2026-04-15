# mainline middle-ledger 2010 truthfulness gate 记录
`记录编号`：`59`
`日期`：`2026-04-14`

## 实施记录
1. 先复核 `56 / 57 / 58` 的 design / spec / card / evidence / record / conclusion，明确 `59` 只负责 `2010` pilot 的 truthfulness / readout / acceptance 裁决，不继续扩大写库范围。
2. 顺序查询真实正式 `malf / structure / filter / alpha / market_base` DuckDB。
   - Windows 上 DuckDB 文件存在并行附库锁冲突，因此本卡统一改为单进程顺序读取，再把事实汇总到同一份 gate report。
3. 按正式链路做 truthfulness 核对。
   - 先核对 row-count / instrument-count / date-range；
   - 再核对 `signal -> trigger -> filter -> structure -> malf` 的自然键引用完整性；
   - 最后核对 `admitted signal -> market_base(none)` 的只读执行价兼容性。
4. 对 `position / portfolio_plan` 保持严格边界。
   - 查询确认两者正式库仍只有 `2026-04-09` 的 bounded pilot 样本；
   - 因而本卡只把它们写成“只读 acceptance 契约已具备消费前提”，而不把 `2010` official materialization 写成既成事实。
5. 明确 `60-65` 模板约束。
   - `malf` 继续复用 `57` 的 `bootstrap + replay`；
   - `structure / filter` 继续复用 `58` 的 `checkpoint_queue`；
   - `alpha` 继续复用 `58` 的 bounded full-window；
   - 不得把 `structure` bounded full-window 再当成默认模板。

## 边界
- `59` 只裁决真实正式库 middle-ledger 是否可作为 `60-65` 模板，不裁决 `position / portfolio_plan / trade / system` 的 `2010` 官方落表完成度。
- `59` 不删除 bridge-v1 表，不重做 `2010` 的 position / portfolio_plan bootstrap，不提前解锁 `100-105`。
- `100-105` 仍由 `66` 的 official cutover gate 决定是否恢复。

## 结论前的关键判断
1. 真实正式库已经具备可复制模板。
   - `malf` 的 `2010` replay 为严格 no-op；
   - `structure / filter` 的 queue/checkpoint 已在 `1,833` scope 上落成；
   - `alpha` 的正式信号链条已在真实正式库跑通。
2. 模板必须写成“路径模板”，不能只写成“代码能跑”。
   - `57/58` 暴露出的真实经验是：`structure / filter` 不能把 bounded full-window 视为默认执行路径；
   - `60-65` 必须显式沿用 queue/checkpoint。
3. `position / portfolio_plan` 当前只构成只读 acceptance 辅助信号。
   - 它们不阻断 `60-65`；
   - 但也不能被提前宣传成已完成 `2010` official truth。
