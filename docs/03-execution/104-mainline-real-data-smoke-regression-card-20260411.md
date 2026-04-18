# mainline real-data smoke regression

卡片编号：`104`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  在 `103` 完成前跑全主线真实数据 smoke 没有意义，因为 `trade` 还不会真实推进到 exit/pnl；同时如果不显式承接 `84` 之后的新官方 upstream，真实 smoke 也会误把旧 upstream 假设当成通过条件。
- 目标结果：
  用 1-2 只真实股票跑通 `alpha -> position -> trade -> system` 的 bounded 主线回归，并验证这条链已经承接 `84` 之后的新官方 upstream。
- 为什么现在做：
  这是 trade 完整回测链条落地后的真实数据复核卡，也是 `system` 在新 upstream 下第一次端到端验收。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/10-mainline-real-data-smoke-regression-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/10-mainline-real-data-smoke-regression-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/103-trade-backtest-progression-runner-conclusion-20260411.md`

## 层级归属

- 主层：`system`
- 被测链条：`alpha -> position -> trade -> system`
- 前提：承接 `84` 之后的新官方 upstream
- 本卡职责：做新框架下的第一次真实端到端验收，而不是单纯跑通一个旧链条 smoke

## 任务分解

1. 明确真实 smoke 的上游必须是 `84` 之后的官方链条：五 PAS 日线 `alpha`、正式 `position` 桥接层、正式 `trade` 结果与 progression 账本。
2. 选取 1-2 只真实股票，在 bounded 窗口内跑通 `alpha -> position -> trade -> system`。
3. 验证 `alpha` 是否稳定喂给 `position`，`position` 是否正确桥接到 `trade`，`trade` 是否只基于正式输入推进。
4. 验证 `system` 是否只读消费正式账本并生成一致 readout。
5. 回填 `104` 文档、evidence 与为 `105` 准备的 acceptance 输入摘要。

## 流程图

```mermaid
flowchart LR
    REAL[真实股票 1-2只 bounded] --> ALPHA[alpha]
    ALPHA --> POS[position]
    POS --> TRADE[trade]
    TRADE --> SYS[system readout]
    SYS --> EV[evidence 导出摘要]
```

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/10-*`
  - `docs/02-spec/modules/system/10-*`
  - `docs/03-execution/104-*`
  - `alpha -> position -> trade -> system` 的真实 bounded smoke 验收
- 范围外：
  - 扩大到全市场 full replay
  - live/broker runtime
  - 替代 `105` 的 orchestration step 设计

## 历史账本约束

- 实体锚点：
  `formal_signal_nk / position_candidate_nk / position_leg_nk / system_snapshot_nk`
- 业务自然键：
  smoke 只读取正式 NK 绑定的官方账本，不接受 run 级临时主键串接主链。
- 批量建仓：
  本卡不负责 full bootstrap，只负责 bounded 实盘化验收。
- 增量更新：
  允许对 smoke 标的和窗口重复重跑，但结果必须按正式 NK 幂等复算。
- 断点续跑：
  使用既有模块的 queue/checkpoint 续跑，不自行发明临时旁路。
- 审计账本：
  `alpha / position / trade / system` 正式 run 摘要与 `104` evidence / record / conclusion 共同构成本卡审计输入。

## 正式设计清单

| 设计项 | 正式口径 | 不接受情形 |
| --- | --- | --- |
| upstream 口径 | smoke 必须承接 `84` 后的新官方 upstream | 继续拿旧 `55` 时代 upstream 假设做验收 |
| 被测链条 | `alpha -> position -> trade -> system` 四层全部使用正式账本 | 依赖内存态、helper 表或临时导出文件 |
| 桥接验证 | 必须验证 `position` 是否正确桥接 `alpha` 与 `trade` | 只看 `trade/system` 跑通，不检查桥接合同 |
| 执行验证 | 必须验证 `trade` 只消费正式输入推进 | `trade` 回读上游私有过程仍被视为通过 |
| system 验证 | `system` 只读消费正式账本并形成稳定 readout | `system` 直接拼接私有中间结果 |

## 实施清单

| 切片 | 实施内容 | 交付物 |
| --- | --- | --- |
| 切片 1 | 明确 smoke 标的、窗口与 `84` 后 upstream 验收清单 | smoke checklist |
| 切片 2 | 跑通 bounded `alpha -> position -> trade -> system` 主线 | 运行命令、落表摘要 |
| 切片 3 | 核对 `position` 桥接、`trade` 只读消费、`system` 正式 readout | 差异说明、验证摘要 |
| 切片 4 | 输出 evidence、record 与供 `105` 使用的 acceptance 摘要 | execution 闭环文档 |

## A 级判定表

| 判定项 | A 级通过标准 | 阻断条件 | 对下游影响 |
| --- | --- | --- | --- |
| 新 upstream 验收 | smoke 明确承接 `84` 后官方链条 | 仍沿用旧 upstream 假设 | `105` 输入失真 |
| position 桥接 | `alpha -> position -> trade` 桥接链可追溯 | 只验证两头，不验证中层 | 执行合同不可信 |
| trade 只读消费 | `trade` 只基于正式输入推进到 exit/pnl | 仍回读私有过程 | 回测不可复算 |
| system readout | `system` 只读消费正式账本并形成稳定 readout | 依赖临时中间态 | orchestration 冻结无效 |
| 审计闭环 | 有命令、证据、record、conclusion | 只有跑通过口头说明 | 卡不可收口 |

## 收口标准

1. `104` 明确成为 `84` 之后新官方 upstream 的端到端验收卡。
2. `alpha -> position -> trade -> system` bounded 主线真实跑通。
3. `position` 桥接、`trade` 只读消费、`system` 正式 readout 三项验证通过。
4. `105` 的 orchestration acceptance 输入成立。
