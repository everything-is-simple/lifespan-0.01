# system 主链 bounded acceptance readout 与 audit bootstrap 规格

日期：`2026-04-11`
状态：`生效`

## 适用范围

本规格适用于 `27-system-mainline-bounded-acceptance-readout-and-audit-bootstrap-card-20260411.md` 及其后续 evidence / record / conclusion。

当前 `system` 只负责建立最小官方主链 readout / audit / freeze contract，不负责新增策略事实。

## 一、目标规格

`27` 号卡必须回答以下问题：

1. `system` 如何只基于官方上游账本给出一次 bounded mainline acceptance readout。
2. `system` 如何沉淀 child-run readout，而不是重新计算上游模块逻辑。
3. `system` 如何把 `portfolio_plan / trade` 的系统级结果冻结为官方 snapshot。
4. `system` 如何记录 `inserted / reused / rematerialized / failed` 等系统级审计状态。
5. `system` 的最小 bootstrap 完成后，是否已经具备作为后续 runtime/orchestration 扩展的正式落点。

## 二、正式输入规格

`27` 号卡实现时必须显式覆盖以下正式输入：

1. `docs/03-execution/26-mainline-truthfulness-revalidation-after-malf-sidecar-bootstrap-conclusion-20260411.md`
2. 当前 `position / portfolio_plan / trade` 的正式 bounded runner 入口与对应 `src/` runner 实现
3. 必要的官方 `*_run` 账本与 `summary_json`
4. 官方 `portfolio_plan_snapshot`
5. 官方 `trade_execution_plan / trade_position_leg / trade_carry_snapshot`

## 三、正式输出规格

`27` 号卡至少应物化以下正式对象或其等价物：

1. `system_run`
2. `system_child_run_readout`
3. `system_mainline_snapshot`
4. `system_run_snapshot`

输出必须满足：

1. 有稳定自然键
2. 支持 bounded 重跑
3. 支持 inserted / reused / rematerialized 审计
4. 不替代上游业务事实主数据

## 四、输入边界规格

### 1. 允许读取

1. 官方 `*_run` 与 `summary_json`
2. 官方 `portfolio_plan_snapshot`
3. 官方 `trade_execution_plan / trade_position_leg / trade_carry_snapshot`
4. 为 readout 必需的官方 `position / alpha / filter / structure` 落表事实

### 2. 禁止读取

1. 任意模块内部临时过程
2. 非官方桥接脚本输出
3. 绕过正式账本的私有缓存或临时文件
4. broker / account live runtime 状态

## 五、bounded acceptance 规格

`system` 当前阶段的 acceptance readout 至少要能回答：

1. 本次 bounded mainline 覆盖哪些 child run
2. 当前 snapshot_date 下有多少 `planned_entry`
3. 当前 snapshot_date 下有多少 `blocked_upstream`
4. 当前 snapshot_date 下有多少 `planned_carry`
5. 当前 snapshot_date 下有多少 open leg / current carry
6. 本次 system run 的系统级状态与 summary_json 是什么

## 六、历史账本约束

`27` 号卡必须显式填写以下六条：

1. 实体锚点：以 `portfolio_id + snapshot_date + system_contract_version` 及 `run_id` 审计元数据为 system 最小对象锚点。
2. 业务自然键：以 `portfolio_id + snapshot_date + system scene` 与 `child_module + child_run_id` 作为系统 readout 自然键。
3. 批量建仓：说明首次如何从当前正式主链账本全量构造 system 最小快照。
4. 增量更新：说明后续如何按新的 `snapshot_date / portfolio_id / child_run` 增量更新。
5. 断点续跑：说明 system run 失败后如何按 bounded scope 重跑。
6. 审计账本：说明 `system_run / system_child_run_readout / system_run_snapshot` 如何留痕。

## 七、允许输出规格

`27` 号卡 conclusion 只允许输出以下类型的正式结论：

1. “system 最小官方 readout / audit bootstrap 已成立，可继续开后续 system runtime 卡。”
2. “system bootstrap 发现上游合同仍有缺口，需先开修复卡。”
3. “当前 system 范围过宽，需先收缩边界后重开实现卡。”

不允许输出：

1. 把 `27` 写成 live broker/account lifecycle 卡。
2. 把 `27` 写成 filled / pnl / reconciliation 全量 runtime 卡。
3. 把 `27` 写成重新计算 `alpha / position / trade` 业务事实的替代实现卡。

## 八、通过标准

`27` 号卡通过至少需要同时满足：

1. 当前 card 已具备 design / spec / task / 历史账本约束。
2. 有最小正式 `system` 表族与 bounded runner。
3. 有面向主链 acceptance 的 bounded 验证。
4. 有正式 evidence / record / conclusion。
5. 能明确裁决后续 `system` 应继续开哪一类 runtime / orchestration 卡。
