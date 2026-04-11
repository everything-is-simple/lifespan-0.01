# malf 机制层 break 确认与同级别统计 sidecar 冻结记录

记录编号：`24`
日期：`2026-04-11`

## 做了什么

1. 回读 `02` 号 malf 扩展章与 `03` 号 pure semantic core 文档，拆分出“保留机制层术语”和“禁止回流到 core 的边界”。
2. 新增 `04` 号 malf mechanism layer design/spec，正式冻结 `pivot-confirmed break` 与 `same-timeframe stats sidecar` 的职责、实体与自然键。
3. 把 break 的正式读取顺序写成三段：`break trigger -> pivot-confirmed break -> new progression confirmation`，并明确第二段不是状态确认前提。
4. 把同级别统计正式拆成 `same_timeframe_stats_profile + same_timeframe_stats_snapshot`，并钉死“只读 sidecar、不得回写 state”。
5. 同步修订 `structure / filter` 的角色声明，明确它们若消费相关 sidecar，只能按只读机制层解释。
6. 回填 `24` 号 card/evidence/record/conclusion，切换执行索引、最新结论锚点与仓库入口文件。
7. 复核发现 `structure` 规格正文的正式输入合同尚未完整吸收机制层 sidecar，于是补充“只读机制层可选输入”口径，并明确当前 runner 参数尚未实现这些 sidecar 接入。
8. 同步清理执行索引里残留的“预挂下一卡 / 待施工”歧义表述，改为“治理锚点保留，不代表本卡未完成”。
9. 运行执行索引检查、开发治理检查与 doc-first gating 检查，确认 24 号卡闭环成立。

## 偏离项

- 本轮仍只冻结正式文档合同，不宣称 `pivot_confirmed_break_ledger` 或 `same_timeframe_stats_*` 已有代码级 canonical runner。

## 备注

- `23` 号卡继续负责 pure semantic core；
- `24` 号卡只负责 pure semantic core 之外的只读机制层；
- 后续若实现机制层 runner 或正式表族，必须另开新卡补齐实现、证据与审计账本。
