# mainline real-data smoke regression 设计宪章

日期：`2026-04-11`
状态：`待执行`

## 背景

当前单元测试覆盖以合同验证为主，缺少基于真实数据的最小主线 smoke。没有这层证据，就很难确认 `alpha -> position -> trade -> system` 的语义在真实样本上保持一致。

## 设计目标

1. 选取 1-2 只真实股票做 bounded 主线 smoke。
2. 覆盖 `alpha -> position -> trade -> system`。
3. 生成可复验的 evidence 与导出摘要。
