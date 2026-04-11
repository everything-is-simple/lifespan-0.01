# malf semantic canonical contract freeze 设计宪章

日期：`2026-04-11`
状态：`待执行`

## 背景

当前 `malf` 的边界治理已经冻结为纯语义核心加只读 sidecar，但实际 snapshot/materialization 仍主要停留在 bridge-v1 近似实现。要继续推进下游，必须先把“最新 malf 语义到底是什么”写成正式合同。

## 设计目标

1. 冻结最新 `malf core` 的正式语义对象与推进规则。
2. 明确 bridge-v1 近似实现里哪些字段只是过渡兼容，哪些必须退役。
3. 冻结 canonical malf 对 `structure / filter / alpha` 的正式输出边界。

## 核心裁决

1. `malf_context_4` 这类 MA/收益率派生上下文不得再被描述成 canonical malf 语义本体。
2. canonical malf 必须围绕 `HH / HL / LL / LH / break / count` 的正式推进语义落地。
3. `pivot-confirmed break` 与 `same-timeframe stats` 仍然保持 mechanism sidecar 身份，不反写 core。

## 非目标

1. 本卡不直接实现新 runner。
2. 本卡不直接改写 `structure / filter / alpha` 代码。
