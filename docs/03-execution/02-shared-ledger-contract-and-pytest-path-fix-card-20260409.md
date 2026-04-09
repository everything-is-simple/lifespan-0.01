# 历史账本共享契约与pytest路径修正

卡片编号：`02`
日期：`2026-04-09`
状态：`已完成`

## 需求

- 问题：
  目前 `pytest` 的临时目录配置仍会随运行目录漂移；同时“历史账本”仍主要停留在原则表达，缺少跨模块共享契约。
- 目标结果：
  修复 `pytest` 临时目录定位问题，并把历史账本共享契约正式写成 design/spec。
- 为什么现在做：
  如果不先钉死测试临时目录和共享账本契约，后续 `position / alpha / portfolio_plan / trade` 很容易各写各的自然键和环境口径。

## 设计输入

- 设计文档：`docs/01-design/03-historical-ledger-shared-contract-charter-20260409.md`
- 规格文档：`docs/02-spec/03-historical-ledger-shared-contract-spec-20260409.md`

## 任务分解

1. 修复 `pytest` 临时目录路径，让它不再依赖运行时 `cwd`。
2. 落历史账本共享契约设计，冻结自然键、审计字段、写入语义和路径来源优先级。
3. 落共享契约规格，明确后续模块进入正式表设计前的门槛。
4. 跑验证并把结果回填到证据、记录和结论。

## 实现边界

- 范围内：
  - `pytest` 临时目录修复
  - 共享账本 design/spec
  - 必要入口文档同步
  - 最小验证
- 范围外：
  - `doc-first gating` 检查器实现
  - `position` 正式表结构
  - 业务算法代码

## 收口标准

1. `pytest` 从仓库根目录和 `tests/` 子目录启动都能把临时产物写到 `H:\Lifespan-temp`
2. 共享账本 design/spec 写完
3. 证据写完
4. 记录写完
5. 结论写完
