# position entry t+1 open reference price correction

卡片编号：`101`
日期：`2026-04-11`
状态：`待执行`

## 需求

- 问题：
  当前 `position` 使用 `T+1 close` 作为参考价，与业务模型的 `T+1 open` 入场不一致；同时这个参考价到底由 `position` 还是 `trade` 冻结、谁能修改、谁只能只读消费，当前也没有正式写清。
- 目标结果：
  将 `position` 正式参考价切换到 `market_base.none` 的 `T+1 open`，并把它冻结为 `position -> trade` 的正式执行参考价合同。
- 为什么现在做：
  `100` 已冻结信号锚点桥接合同后，这张卡才有稳定上游可接；否则 `trade` 仍会面对“参考价到底属于谁冻结”的边界空洞。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/system/07-position-entry-t-plus-1-open-reference-price-correction-charter-20260411.md`
- 规格文档：
  - `docs/02-spec/modules/system/07-position-entry-t-plus-1-open-reference-price-correction-spec-20260411.md`
- 当前锚点结论：
  - `docs/03-execution/100-trade-signal-anchor-contract-freeze-conclusion-20260411.md`

## 层级归属

- 主层：`position`
- 次层：`trade`
- 上游输入：`100` 冻结后的 `alpha -> position` signal anchor contract
- 本卡职责：把 entry reference price 明确落在 `position` 层冻结，再由 `trade` 只读消费

## 任务分解

1. 冻结 `market_base.none` 的 `T+1 open` 为唯一正式 entry reference price。
2. 明确 `position_candidate / position_entry_plan / execution-ready contract` 中哪些字段承接并冻结这个参考价。
3. 明确 `trade_execution_plan / trade_position_leg` 只读消费该参考价，不允许改写或以 `T+1 close` 回退替代。
4. 明确 `market_base.none` 作为正式执行价格口径的原因和不接受情形。
5. 回填 `101` 文档与索引，并为 `102 / 103` 提供稳定输入。

## 参考价校正路径图

```mermaid
flowchart LR
    MB[market_base none T+1 open] --> EP[position entry reference]
    EP --> POS[position entry plan]
    POS --> EXEC[execution-ready contract]
    EXEC --> TRD[trade readonly consume]
```

## 实现边界

- 范围内：
  - `docs/01-design/modules/system/07-*`
  - `docs/02-spec/modules/system/07-*`
  - `docs/03-execution/101-*`
  - `position -> trade` 的 entry reference price 合同
- 范围外：
  - exit / pnl 账本
  - progression runner
  - system orchestration

## 历史账本约束

- 实体锚点：
  `formal_signal_nk / position_candidate_nk / execution_plan_nk`
- 业务自然键：
  entry reference price 必须绑定正式 NK，不得依赖 run 顺序或临时窗口。
- 批量建仓：
  支持对历史 bounded 窗口补齐 `position -> trade` 的参考价透传字段。
- 增量更新：
  只对新增 candidate、重物化 entry plan 或价格合同变更影响的记录重算。
- 断点续跑：
  中断后允许按正式 NK 幂等补写参考价合同字段。
- 审计账本：
  审计落在 `position_run / position_run_snapshot / trade_run` 与 `101` execution 文档。

## 正式设计清单

| 设计项 | 正式口径 | 不接受情形 |
| --- | --- | --- |
| 价格口径 | `market_base.none` 的 `T+1 open` 是唯一正式 entry reference price | 使用 `T+1 close`、复权价或运行时临时回推 |
| 冻结主权 | `position` 负责冻结 reference price 并写入 execution-ready contract | `trade` 自己补算或改写参考价 |
| 下游消费 | `trade` 只读消费 `position` 已冻结的参考价字段 | `trade` 回读 `market_base` 再做二次决定 |
| 自然键绑定 | reference price 绑定 `formal_signal_nk / candidate_nk / execution_plan_nk` 等正式 NK | 依赖 run_id 或临时文件名 |
| 价格解释 | `none` 口径服务真实执行，不替代 `backward` 的研究/信号语义 | 混用研究价与执行价 |

## 实施清单

| 切片 | 实施内容 | 交付物 |
| --- | --- | --- |
| 切片 1 | 盘点 `position / trade` 当前 entry reference price 字段与口径差异 | 字段映射表、gap list |
| 切片 2 | 冻结 `market_base.none` `T+1 open` 的正式字段合同与自然键绑定 | 设计/规格裁决、字段说明 |
| 切片 3 | 明确 `position_entry_plan / execution-ready contract -> trade` 的只读透传规则 | 目标表族说明、消费规则 |
| 切片 4 | 补单元测试与 bounded smoke，验证 `trade` 不再改写 reference price | tests、evidence 命令 |
| 切片 5 | 回填 `record / conclusion / indexes` | execution 闭环文档 |

## A 级判定表

| 判定项 | A 级通过标准 | 阻断条件 | 对下游影响 |
| --- | --- | --- | --- |
| 价格口径冻结 | `T+1 open` 成为唯一正式执行参考价 | 仍允许 `T+1 close` 或临时回退 | `102 / 103` 输入不稳定 |
| 桥接主权 | `position` 明确冻结 reference price | `trade` 仍可自行决定参考价 | 执行事实不可审计 |
| 只读消费 | `trade` 只读消费 `position` 下传值 | `trade` 自己回读行情再判断 | 回测复算不一致 |
| 历史补齐 | 历史窗口可幂等回填 reference price | 只能对新批次生效 | replay 不成立 |
| 审计闭环 | 有测试、smoke、record、conclusion | 只有口头裁决 | 卡不可收口 |

## 收口标准

1. `position -> trade` 的 entry reference price 正式合同成立。
2. `position` 被明确写成 reference price 冻结主层。
3. `trade` 明确只读消费，不再改写参考价。
4. `102 / 103` 的执行价格输入成立。
