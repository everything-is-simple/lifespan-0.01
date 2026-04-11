# malf 纯语义账本边界冻结记录

记录编号：`23`
日期：`2026-04-11`

## 做了什么

1. 审阅现有 `malf` 扩展 design/spec、`structure/filter` 边界文档与执行目录，确认当前冲突点集中在 `execution_interface` 和高周期 `context` 被写回 `malf` 正式核心。
2. 新增 `03` 号 `malf` design/spec，把正式核心收缩为按时间级别独立运行的纯语义走势账本，并把 bridge v1 保留为兼容层。
3. 按前 4 段合并后的最终裁判结果，把“`牛逆 / 熊逆` 是本级别过渡状态”“`break` 是触发不是确认”“统计只能是同级别 sidecar”进一步压进 `03` 号 malf design/spec 与 `23` 号结论。
4. 为 `00 / 01 / 03` 三组 `malf` 文档补上统一角色声明，显式区分 `legacy lessons / bridge v1 / malf core` 三层。
5. 将 `02` 号 `malf` 扩展 design/spec 降级为历史保留，回填 `23` 号四件套、执行目录、证据目录与入口文件。
6. 同步修订 `structure / filter` 相关 design/spec 与 `16` 号 bridge 结论，明确它们只能把 bridge v1 当作兼容输入或下游 sidecar，不再把旧上下文字段写回 `malf core`。
7. 预挂 `24-malf-mechanism-layer-break-confirmation-and-stats-sidecar-card-20260411.md`，把下一阶段正式议题固定为 `pivot-confirmed break` 与 `same-timeframe stats sidecar`。
8. 运行执行索引检查、开发治理检查与 doc-first gating 检查，确认本轮文档闭环成立。

## 偏离项

- `.codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --register` 与当前 `00-conclusion-catalog-20260409.md` 的分栏标题不兼容，无法自动回填索引；本轮改为手工回填目录，未影响正式闭环结果。

## 备注

- 本轮只冻结文档口径，不宣称 pure semantic canonical ledger 已有代码实现。
- 当前 `scripts/malf/run_malf_snapshot_build.py` 继续保留 bridge v1 兼容职责，后续若要实现 pure semantic canonical runner，必须另开新卡。
