# trade 模块经验冻结

日期：`2026-04-09`
状态：`生效中`

## 当前职责

- 把 `position` 给出的允许持仓转成真实交易与持仓事实
- 负责 entry、carry、exit 的执行解释
- 在新仓中以 `trade` 模块名对应 `trade_runtime` 正式账本

## 必守边界

1. `trade` 不负责发现 trigger，也不负责重写 trigger 是否发生。
2. `trade` 必须显式保存 carry，而不是窗口结束就强行抹掉剩余腿位。
3. 交易事实与 run 审计必须分层，长期事实逐步切到自然键账本。

## 已验证坑点

1. 没有 retained carry 时，rolling / bounded replay 会把真实持仓延续切断。
2. `trim_to_context_cap` 的缺样本一度误判成 `malf` 失效，根因其实落在 trade carry。
3. 如果 trade 与 `position / alpha` 的桥接链不清，system 报告只能看到 child run id，看不到真实交易解释。

## 新系统施工前提

1. 继续保留 carry_snapshot 一类正式延续事实。
2. 明确 `trade` 是模块名，`trade_runtime` 是物理账本名。
3. 不允许 trade 反向污染 `alpha` 或 `position` 的事实层。

## 来源

1. 老系统总表 `battle-tested-lessons-all-modules-and-mainline-bridging-20260408.md`
2. 老系统 `trade` 章程与 `system 123` carry 章程
