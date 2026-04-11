# 全模块 checkpoint / dirty queue 对齐设计宪章

日期：`2026-04-11`
状态：`待执行`

适用执行卡：`28-system-wide-checkpoint-and-dirty-queue-alignment-card-20260411.md`

## 背景

当前仓库真实实现深度分三档：

1. `data`
   - 已具备 data-grade `file/request/instrument checkpoint + dirty queue + replay`
2. `malf`
   - 具备 `instrument + timeframe checkpoint`
   - 但缺少和 `data` 同等级的 dirty queue / replay 契约
3. `structure / filter / alpha / position / portfolio_plan / trade / system`
   - 多数仍以 `run_id + upsert + reused/rematerialized` 为主

这与“所有本地库都要和 data 一样”的正式要求不一致。

## 设计目标

1. 把 data-grade checkpoint / dirty queue 升格为全仓硬标准。
2. 为所有正式模块冻结统一的 queue / checkpoint / replay / resume 语义。
3. 明确 `105-system-runtime-orchestration-bootstrap` 必须排在本卡及其后续自然数卡之后。

## 核心裁决

1. `run_id` 只能做审计，不能再承担正式业务续跑语义。
2. dirty/work queue 与 checkpoint ledger 必须成为正式账本，不得只靠 rerun + reused/rematerialized 近似替代。
3. 本卡先于 `29-34` 的业务修补卡，是当前治理主卡。

## 非目标

1. 本卡不直接实现 `29-34` 的业务逻辑。
2. 本卡不进入 live/runtime 语义。
