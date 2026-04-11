# trade exit pnl ledger bootstrap 设计宪章

日期：`2026-04-11`
状态：`待执行`

## 背景

当前 `trade` 已有 `execution_plan / position_leg / carry_snapshot`，但没有正式退出账本，导致系统只能记住“要做什么”，不能沉淀“最终发生了什么”。

## 设计目标

1. 建立最小正式退出账本。
2. 让部分退出、全退出、退出原因和 realized pnl 有正式落点。
3. 为后续逐日推进引擎提供稳定写入目标。
