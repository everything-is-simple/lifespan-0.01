# alpha formal signal 正式出口合同与最小 producer 记录

记录编号：`10`
日期：`2026-04-09`

## 做了什么

1. 先按仓库执行纪律回读了 `09` 的结论、卡目录、completion ledger 与系统路线图，确认当前下一步应从 `position` 回到 `alpha`，而不是继续深挖 `position`。
2. 对齐了新仓 `position` 消费侧已冻结合同与老仓 `310 PAS formal signal 回接 position bootstrap` 的正式经验，把本轮范围压缩为：
   - `alpha_formal_signal_run`
   - `alpha_formal_signal_event`
   - `alpha_formal_signal_run_event`
   - 最小 producer runner
3. 新增 `alpha` 设计与规格文档，明确：
   - `trigger ledger` 与 `formal signal` 必须分层
   - `event` 是事实层，`run` 是审计层，`run_event` 是桥接层
   - `position` 后续应直接消费 `alpha_formal_signal_event`
4. 使用仓库自带脚本生成 `10` 号卡四件套，并把当前待施工卡切换为 `10-alpha-formal-signal-contract-and-producer-card-20260409.md`。
5. 手工回校索引口径，避免把草稿 conclusion 错挂成“当前正式结论”。

## 偏离项

- 自动建卡脚本会把草稿 conclusion 预填进 conclusion catalog，但当前任务只是正式开卡而非完成收口，所以本轮已把该预填回退，避免正式结论口径失真。

## 备注

- 本轮只做 doc-first 前置与执行卡切换，不宣称 `alpha` producer 已完成。
- 下一轮正式实现应直接围绕 `alpha` 官方 `formal signal` producer 展开，而不是回到 `position` 新增更多内部 family 表。
