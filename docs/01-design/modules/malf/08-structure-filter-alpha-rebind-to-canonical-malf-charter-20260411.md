# structure filter alpha rebind to canonical malf 设计宪章

日期：`2026-04-11`
状态：`待执行`

## 背景

即使 canonical malf 落地，如果 `structure / filter / alpha` 仍继续吃 bridge-v1 近似输出，下游主线依然不可信。`PAS` 虽然不是顶层模块，但它属于 `alpha` 内部能力，也会受到这次重绑影响。

## 设计目标

1. 让 `structure` 从 canonical malf 正式输出消费上游。
2. 让 `filter` 和 `alpha/PAS` 的上游口径切换到 canonical malf。
3. 清理 bridge-v1 在下游主线中的临时依赖地位。

## 核心裁决

1. 下游主线不得继续把 bridge-v1 近似输出当作长期正式真值。
2. `alpha/PAS` 的 detector 和 formal signal 上游必须与 canonical malf 重对齐。

## 非目标

1. 本卡不直接进入 `position / trade / system`。
2. 本卡不处理回测 exit/pnl。
