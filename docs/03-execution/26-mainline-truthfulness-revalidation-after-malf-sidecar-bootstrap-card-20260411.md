# 主链 truthfulness 复核卡
卡号：`26`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  `23`、`24`、`25` 已经先后冻结并实现了 `malf` pure semantic core、机制层 sidecar 边界与 bridge-era sidecar 账本，但当前仍缺少一张专门的整链复核卡，来回答这些新口径接入后，主链
  `data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade`
  是否仍然真实成立，是否仍遵守正式账本与正式 runner 合同。
- 目标结果：
  正式完成一次 bounded mainline truthfulness 复核，明确裁决当前主链是否可以继续推进到下一张主线卡，还是必须先开修复卡。
- 为什么现在做：
  如果跳过这一步直接开 `system`、`alpha-position sidecar readout` 或 `canonical runner bootstrap`，就会把“局部能力新增”和“整链真实闭环”混在一起，导致后续系统级结论建立在未经复核的主链上。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/02-system-mainline-truthfulness-revalidation-after-malf-sidecar-bootstrap-charter-20260411.md`
  - `docs/01-design/α-system-roadmap-and-progress-tracker-charter-20260409.md`
- 规格文档：
  - `docs/02-spec/modules/system/02-system-mainline-truthfulness-revalidation-after-malf-sidecar-bootstrap-spec-20260411.md`
  - `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
- 当前锚点结论：
  - `docs/03-execution/25-malf-mechanism-ledger-bootstrap-and-downstream-sidecar-integration-conclusion-20260411.md`
  - `docs/03-execution/24-malf-mechanism-layer-break-confirmation-and-stats-sidecar-conclusion-20260411.md`
  - `docs/03-execution/23-malf-pure-semantic-ledger-boundary-freeze-conclusion-20260411.md`

## 任务分解

1. 复核 `data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade` 各层正式输入/输出合同是否仍闭合。
2. 复核 `break/stats sidecar` 在 `structure / filter` 的消费身份是否仍是只读附加，并检查下游是否存在误读或绕过正式上游的情况。
3. 复核 `backward` 与 `none` 两套价格口径在主链中的切换边界是否仍与正式治理口径一致。
4. 运行 bounded mainline 验证，补齐 evidence / record / conclusion，并给出正式裁决：下一张是主线卡、修复卡，还是 system 卡。

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/*`
  - `docs/02-spec/modules/system/*`
  - `docs/03-execution/26-*`
  - `docs/03-execution/evidence/26-*`
  - `docs/03-execution/records/26-*`
  - 当前主链相关正式 runner、测试与治理脚本的复核性改动
  - 执行索引与入口文件
- 范围外：
  - 新增 `system` 运行时代码
  - 新增 `alpha / position` sidecar readout
  - pure semantic canonical runner 替换工程
  - 借复核之名新增与主链 truthfulness 无关的新特性

## 历史账本约束

- 实体锚点：
  以当前正式主链模块与其正式 runner / ledger contract 为复核锚点，核心对象是 `data / malf / structure / filter / alpha / position / portfolio_plan / trade` 之间的正式对接关系。
- 业务自然键：
  以 `模块 + 正式上游输入 + 正式输出账本 + 价格口径` 作为复核自然键，重点核对 bridge-era `malf` sidecar 新口径是否破坏原主链。
- 批量建仓：
  首次复核按 bounded 主链命令链路全量抽样构造，覆盖 `data -> trade` 的最小正式闭环。
- 增量更新：
  后续若出现新结论、新 runner 或新边界变更，按受影响主链片段重复执行 bounded 复核，而不是默认整仓全量重跑。
- 断点续跑：
  若复核中途失败，允许按模块片段、bounded 时间窗和结论锚点重新执行，不要求每次从零开始；但最终裁决必须基于完整复核证据。
- 审计账本：
  审计通过 `26` 号卡对应的 evidence / record / conclusion、执行索引与验证命令留痕，不得只在聊天中口头裁决。

## 收口标准

1. 能明确裁决当前主链在 `23/24/25` 之后是否仍真实成立到 `trade`。
2. 能明确裁决下一张应是主线卡、修复卡还是 `system` 卡。
3. evidence / record / conclusion 与执行索引回填完整。
4. 不把 `26` 号卡偷渡成 sidecar 扩展卡或 canonical runner 实现卡。
