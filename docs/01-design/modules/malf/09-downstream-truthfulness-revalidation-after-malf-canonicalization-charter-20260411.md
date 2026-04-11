# downstream truthfulness revalidation after malf canonicalization 设计宪章

日期：`2026-04-11`
状态：`待执行`

## 背景

在 canonical malf 落地并重绑下游后，必须重新确认 `structure -> filter -> alpha` 的主线 truthfulness 没有被旧近似语义污染，也需要用真实数据做最小复核。

## 设计目标

1. 重新验证 canonical malf 之后的主线 truthfulness。
2. 通过真实数据 bounded 样本做最小 smoke。
3. 裁决下游 `trade / system` 是否可以恢复正式推进。

## 非目标

1. 本卡不实现 trade exit/pnl。
2. 本卡不实现 system orchestration。
