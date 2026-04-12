# system 主链 bounded acceptance readout 与 audit bootstrap 设计宪章

日期：`2026-04-11`
状态：`生效`

## 背景

`26` 已经正式裁决：当前主链

`data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade`

在引入 `23/24/25` 新口径之后仍然真实闭环，且 `position -> trade` 的 `none` 价格边界与 sidecar 只读身份都已再次验证成立。

这意味着当前主线不再缺“能否继续前推”的裁决，而是进入下一阶段的正式问题：

如何在不重新发明策略事实、不回读各模块内部私有过程的前提下，把已经成立的主链结果收束为 `system` 层的 bounded acceptance readout、child-run trace、审计留痕与冻结入口。

如果这一阶段继续拖延，仓库将长期停留在“各模块都能跑，但系统层没有正式 readout / audit / freeze contract”的状态；这会导致：

1. 无法以官方 `system` 视角回答“一次 bounded mainline run 到底是否成立、卡在哪一段、哪些 child run 被复用或重物化”。
2. 无法把 `trade` 之后的组合 readout、carry/blocked/planned/filled 等系统级状态沉淀成稳定账本。
3. 后续若继续扩 live orchestration、broker/account lifecycle 或 system reuse，都会缺少最小官方系统账本作为落点。

## 设计目标

1. 正式新增一张 `system` 主线卡，把当前已成立的主链结果上收为 `system` 的最小官方 readout / audit / freeze contract。
2. 明确 `system` 当前第一阶段只负责：
   - child-run readout
   - bounded acceptance summary
   - 审计账本
   - freeze / snapshot 入口
3. 明确 `system` 不负责重新计算 `malf / structure / filter / alpha / position / portfolio_plan / trade` 的业务事实，也不接管 live broker/account lifecycle。
4. 为后续 `system` 扩展提供正式主语义锚点，让未来 readout/reuse/orchestration 建在稳定账本之上。

## 核心设计

### 1. system 的正式对象

`27` 号卡的正式对象固定为：

1. `system_run`：一次 bounded system readout / acceptance 审计批次。
2. `system_child_run_readout`：被 system 汇总的各正式 child run 引用与状态快照。
3. `system_mainline_snapshot`：面向 `portfolio_id + snapshot_date + system_contract_version` 的系统级主链读数快照。
4. `system_run_snapshot`：一次 system run 与其主链 snapshot 的桥接账本。

### 2. system 的正式输入边界

`system` 当前阶段只允许消费官方上游：

1. 各模块正式 `*_run` 账本与其 `summary_json`
2. 官方 `portfolio_plan_snapshot`
3. 官方 `trade_execution_plan / trade_position_leg / trade_carry_snapshot`
4. 必要时只读引用官方 `position / alpha / filter / structure` 已落成账本

不允许：

1. 回读模块内部临时 DataFrame 或私有中间过程
2. 绕过 `trade` 重新解释策略事实
3. 把 `system` 写成 broker/account lifecycle adapter

### 3. bounded acceptance 的正式语义

`system` 当前阶段要回答的不是“下单执行成功没有”，而是“这次 bounded mainline 从官方账本视角是否成立”。因此它的核心语义是：

1. 哪些 child run 被纳入本次 readout
2. 当前主链 bounded window 内有多少 `planned_entry / blocked_upstream / planned_carry / open_leg`
3. 当前主链 snapshot 的系统级状态是什么
4. 本次 system run 是 `inserted / reused / rematerialized` 还是失败

### 4. 与后续 system 扩展的关系

- `27-system-mainline-bounded-acceptance-readout-and-audit-bootstrap` 是 `system` 的最小主线 bootstrap 卡。
- 后续若要扩：
  - live orchestration
  - broker/account lifecycle
  - filled/pnl/runtime reconciliation
  - cross-run reuse automation
  都必须建立在 `27` 形成的正式 system 账本之上。

## 边界

### 范围内

1. `system` 最小正式表族、bounded runner 与 readout contract。
2. child-run readout 与 bounded acceptance summary。
3. 审计账本、freeze / snapshot 入口与最小验证。
4. `27` 对应的 evidence / record / conclusion 与执行索引收口。

### 范围外

1. broker / account lifecycle adapter
2. live trading orchestration
3. filled、pnl、slippage 等更重运行时 reconciliation
4. 回写上游模块或改造既有策略事实主数据
5. sidecar 向 `alpha / position` 的新一轮扩展

## 影响

1. 主线将首次拥有官方 `system` 层最小账本，而不再只有模块级账本。
2. 后续对“主链是否成立”的系统级回答，将从聊天口头裁决升级为可复算的 `system` 官方 readout。
3. `system` 的扩展门槛被明确约束：先有 bounded acceptance readout，再谈 live/runtime 扩展。

## 流程图

```mermaid
flowchart LR
    CHAIN[主链各模块账本] --> SYS_RUN[system bounded runner]
    SYS_RUN --> RDOUT[system_mainline_snapshot readout]
    RDOUT --> AUD[audit bootstrap]
    AUD -->|可复算裁决| ACCEPT[主链成立 bounded acceptance]
```
