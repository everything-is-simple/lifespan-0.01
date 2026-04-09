# position formal signal runner 与 bounded validation

卡片编号：`09`
日期：`2026-04-09`
状态：`已完成`

## 需求

- 问题：
  08 已经把 `position` 的最小表族和最小 in-process 消费入口建立起来了，但它当前仍只吃 Python 侧传入的 formal signal 样本，不会正式从官方 `alpha` 账本读取，也没有 bounded validation runner。
- 目标结果：
  为 `position` 补一个最小正式 runner，能从官方 `alpha formal signal` 读取 bounded 样本，补齐 `market_base` 参考价后写入 `candidate / capacity / sizing / family snapshot`，并留下最小 validation 证据。
- 为什么现在做：
  如果 09 不尽快补上，08 的消费入口就仍然停留在“库内 helper 可用”，还没有成为能被正式执行卡和下游模块消费的入口。

## 设计输入

- 设计文档：`docs/01-design/modules/position/01-position-funding-management-and-exit-charter-20260409.md`
- 规格文档：`docs/02-spec/modules/position/01-position-funding-management-and-exit-spec-20260409.md`
- 桥接合同：`docs/02-spec/modules/position/02-alpha-to-position-formal-signal-bridge-spec-20260409.md`
- runner 规格：`docs/02-spec/modules/position/03-position-formal-signal-runner-spec-20260409.md`
- 上轮结论：`docs/03-execution/08-position-ledger-table-family-bootstrap-conclusion-20260409.md`

## 任务分解

1. 建立 `position` 最小 runner，从官方 `alpha formal signal` bounded 读取样本并补齐 `market_base` 参考价。
2. 把 runner 接到当前 `position` bootstrap/materialization helper，而不是另起第二套落表逻辑。
3. 留下 bounded validation 的命令、落表摘要与闭环文档。

## 实现边界

- 范围内：
  - `alpha formal signal` bounded reader
  - `market_base` 参考价补齐
  - `position` 最小 runner 与 bounded validation
- 范围外：
  - `trade / system` 正式消费
  - 全市场 full replay
  - `portfolio_plan` 组合容量读模型

## 收口标准

1. `position` 有最小正式 runner
2. runner 能消费官方 `alpha formal signal`
3. bounded validation 证据具备
4. 证据写完
5. 记录写完
6. 结论写完
