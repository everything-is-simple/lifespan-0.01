# alpha formal signal 正式出口合同与最小 producer 结论

结论编号：`10`
日期：`2026-04-09`
状态：`生效中`

## 裁决

- 接受：新仓 `alpha` 已具备官方 `formal signal` producer，正式出口冻结为 `alpha_formal_signal_run / alpha_formal_signal_event / alpha_formal_signal_run_event` 三表。
- 接受：`position` 已从“消费合同兼容表”推进到“直接消费新仓官方 `alpha_formal_signal_event`”，`M2 alpha-position 正式桥接成立` 可以收口。
- 接受：本轮 producer 只承担 bounded 正式落库与审计职责，不自动串调 `position / trade / system`，也不假装 `alpha` 全部内部家族表已经完成。
- 拒绝：把这轮结果表述成“`alpha` 五表族已经全部正式落地”或“`structure / filter / trade / system` 已经打通主线”。

## 原因

1. `09` 已证明 `position` 消费侧 runner 成立，真实缺口已经转移到上游 `alpha` 官方 formal signal producer。
2. 老仓经验已经证明 `trigger ledger -> formal signal -> position` 必须分层；本轮只把当前下游真正需要的官方出口先收口，是最小且正确的下一步。
3. bounded unit test 与 smoke 都证明：
   - producer 能稳定写入三表
   - event 能在 context 变化时被 rematerialize
   - `position` 能直接读取新仓官方上游，而不是依赖合同兼容表

## 影响

1. `alpha` 从“只有 design/spec/card”推进到“已有最小官方 producer 与正式账本出口”。
2. `position` 当前真上游已经切到 `alpha_formal_signal_event`，后续不应再为弥补上游缺口继续扩 `position` family 表。
3. 当前主线剩余阻塞已从“alpha 官方出口不存在”收缩为“alpha 内部五表族、structure/filter 正式合同、以及更下游模块尚未开工”。
