# malf semantic canonical contract freeze 规格

日期：`2026-04-11`
状态：`待执行`

本规格适用于 `29-malf-semantic-canonical-contract-freeze-card-20260411.md` 及其后续 evidence / record / conclusion。

## 目标

冻结 canonical malf 的正式语义边界，使后续 runner 重写与下游 rebind 有统一依据。

## 必答问题

1. canonical malf 的正式原语有哪些。
2. bridge-v1 输出里哪些字段属于过渡兼容。
3. canonical malf 输出给 `structure / filter / alpha` 的正式字段边界是什么。
4. 旧 `pas_context_snapshot / structure_candidate_snapshot` 在 canonical 阶段保留、降级还是退役。

## 验收

1. 最新 malf 语义被写成 design/spec/card。
2. 明确指出 bridge-v1 近似实现不再等同于 canonical malf。
3. 为 `30-32` 提供正式输入。
