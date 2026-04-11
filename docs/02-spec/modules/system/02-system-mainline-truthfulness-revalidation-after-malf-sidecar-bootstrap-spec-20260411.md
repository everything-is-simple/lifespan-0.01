# system 主链 truthfulness 复核规格

日期：`2026-04-11`
状态：`生效`

## 适用范围

本规格适用于 `26-mainline-truthfulness-revalidation-after-malf-sidecar-bootstrap-card-20260411.md` 及其后续 evidence / record / conclusion。

复核范围固定为当前正式主链：

`data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade`

## 一、复核目标规格

`26` 号卡必须回答以下问题：

1. `23/24/25` 引入的新 `malf` 口径是否没有破坏当前主链。
2. 当前各 runner 是否仍然只读取各自正式上游账本。
3. `break/stats sidecar` 是否仍保持只读附加身份。
4. `backward -> none` 价格口径切换边界是否仍与正式治理口径一致。
5. bounded mainline 验证是否还能给出一致、可追溯的证据。

## 二、正式输入规格

`26` 号卡复核时必须显式覆盖以下正式输入：

1. `docs/03-execution/23-malf-pure-semantic-ledger-boundary-freeze-conclusion-20260411.md`
2. `docs/03-execution/24-malf-mechanism-layer-break-confirmation-and-stats-sidecar-conclusion-20260411.md`
3. `docs/03-execution/25-malf-mechanism-ledger-bootstrap-and-downstream-sidecar-integration-conclusion-20260411.md`
4. 当前正式 runner 入口脚本与对应 `src/` runner 实现。
5. 当前执行索引与系统路线图文档。

## 三、复核检查面规格

### 1. 上游 contract 面

必须检查：

1. `data` 是否仍只通过正式 `raw_market / market_base` 向下游供数。
2. `malf` 是否仍保持 `core` 与 bridge-era mechanism sidecar 分层。
3. `structure / filter` 是否只把 sidecar 当作只读附加。

### 2. 下游 consumption 面

必须检查：

1. `alpha` 是否仍只消费正式 `filter / structure snapshot`。
2. `position` 是否仍只消费正式 `alpha formal signal` 与 `market_base.none` 参考价。
3. `portfolio_plan / trade` 是否仍只消费各自正式上游，不回读 bridge 中间过程。

### 3. 价格口径面

必须检查：

1. `malf -> structure -> filter -> alpha` 默认仍使用 `adjust_method='backward'`。
2. `position -> trade` 默认仍使用 `adjust_method='none'`。
3. 不允许把 `forward` 重新引入正式执行主链。

### 4. bounded 验证面

必须至少包含以下之一，最好同时具备：

1. bounded runner 命令链路验证。
2. 关键落表事实抽样。
3. 目标单测或 smoke 验证。
4. sidecar 只读身份与主链 truthfulness 的裁决说明。

## 四、允许输出规格

`26` 号卡 conclusion 只允许输出以下类型的正式结论：

1. “主链 truthfulness 通过，可继续开主线卡。”
2. “发现局部偏移，需先开修复卡，主线暂不前推。”
3. “发现主链级断裂，不得继续推进 system 或 sidecar 下游扩展。”

不允许输出：

1. 把 `26` 写成 `system` 实现卡。
2. 把 `26` 写成 `alpha-position sidecar` 扩展卡。
3. 把 `26` 写成 canonical runner 实现卡。

## 五、历史账本约束

`26` 号卡必须显式填写以下六条，并保持面向“复核账本与验证证据”的语义：

1. 实体锚点：以“正式主链模块 + 正式 runner / ledger contract”作为复核对象锚点。
2. 业务自然键：以“模块 + 上游正式输入 + 输出账本 + 价格口径”作为复核自然键。
3. 批量建仓：说明首次整链 bounded 复核如何构造。
4. 增量更新：说明后续在新结论后如何重复执行复核。
5. 断点续跑：说明复核失败后如何按 bounded 范围重跑。
6. 审计账本：说明 evidence / record / conclusion 与索引如何回填。

## 六、通过标准

`26` 号卡通过至少需要同时满足：

1. 当前 card 已具备 design / spec / task / 历史账本约束。
2. 有明确的 bounded mainline 复核命令。
3. 有正式 evidence / record / conclusion。
4. 能裁决“下一张应该是主线卡、修复卡，还是 system 卡”。
