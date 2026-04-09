# 文档先行硬门禁检查器

卡片编号：`03`
日期：`2026-04-09`
状态：`已完成`

## 需求

- 问题：
  仓库虽然已经写明“文档先行”，但仍缺少一个真正会拦人的硬门禁检查器；只靠口头纪律，后续进入 `position / alpha / portfolio_plan` 正式实现时很容易回滑到“先改代码、再补文档”。
- 目标结果：
  新增一个正式治理检查器，要求当前待施工卡在进入正式实现前，已经具备非占位的需求、设计、规格和任务分解，并把它串进总治理入口。
- 为什么现在做：
  历史账本共享契约已经冻结，下一步就要开始碰更重的模块实现；如果不先卡死文档门禁，后面返工成本会迅速升高。

## 设计输入

- 设计文档：`docs/01-design/04-doc-first-gating-checker-charter-20260409.md`
- 规格文档：`docs/02-spec/04-doc-first-gating-checker-spec-20260409.md`

## 任务分解

1. 新增 `check_doc_first_gating_governance.py`，验证当前待施工卡是否已具备正式输入。
2. 把 `doc-first gating` 接入 `check_development_governance.py`，成为正式治理链的一部分。
3. 扩大入口新鲜度治理触发范围，把 `docs/01-design/`、`docs/02-spec/`、`src/mlq/core/paths.py` 纳入入口联动。
4. 补齐最小单元测试，并把结果回填到证据、记录和结论。

## 实现边界

- 范围内：
  - `doc-first gating` 治理脚本
  - 设计/规格文档
  - 入口文件同步
  - 最小单元测试
  - 执行闭环回填
- 范围外：
  - `position` 业务账本设计
  - `alpha` 或 `portfolio_plan` 的正式实现
  - 历史卡片的批量回补

## 收口标准

1. `check_doc_first_gating_governance.py` 落地
2. `check_development_governance.py` 能串联新检查器
3. 入口新鲜度规则已覆盖新增正式口径入口
4. 测试写完并通过
5. 证据、记录、结论写完
