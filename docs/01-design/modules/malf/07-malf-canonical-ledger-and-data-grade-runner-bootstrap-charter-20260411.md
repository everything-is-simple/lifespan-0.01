# malf canonical ledger and data-grade runner bootstrap 设计宪章

日期：`2026-04-11`
状态：`待执行`

## 背景

如果 canonical malf 只有语义合同而没有正式 runner 和历史账本，下游仍然只能依赖 bridge-v1。与此同时，你明确要求 `malf` 数据库也必须像 `data` 一样支持一次性批量建仓、后续每日增量更新和断点续跑。

## 设计目标

1. 建立 canonical malf 的正式表族与 bounded runner。
2. 让 `malf` 具备 data-grade `dirty queue / checkpoint / replay / resume`。
3. 保持 batch build、daily incremental、resume 三种运行方式在同一历史账本上成立。

## 核心裁决

1. canonical malf 不再只依赖 `run_id + upsert` 近似续跑。
2. `malf` 必须拥有显式 dirty/work queue 和正式 checkpoint ledger。
3. bridge-v1 可以短期并存，但 canonical runner 才是今后的正式主语义入口。

## 非目标

1. 本卡不完成下游全部重绑。
2. 本卡不引入 live/runtime 语义。
