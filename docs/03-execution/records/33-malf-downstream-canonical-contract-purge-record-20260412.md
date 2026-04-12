# malf downstream canonical contract purge 记录

记录编号：`33`
日期：`2026-04-12`
状态：`已补记录`

## 做了什么

1. 将 `structure` 正式 snapshot 契约切到 canonical `malf` 字段族，移除默认 bridge 回退和旧字段输出壳，只保留显式兼容输入路径。
2. 将 `filter` 正式 snapshot 契约切到 canonical `structure` 字段族，阻断条件改为围绕 canonical 结构状态，不再把旧失败字段当正式输出。
3. 将 `alpha trigger / formal signal` 的上游指纹与正式事件切到 canonical 上下文；关闭默认 `pas_context_snapshot` fallback，并把 legacy 字段降级为派生兼容字段。
4. 将 `alpha family` payload 中的 `upstream_context_fingerprint` 结构化落地，确保 rematerialized 原因能在 family 账本中直接读到。
5. 补齐 `structure / filter / alpha / system` 相关单测与系统回归，并回填 `33` 的 evidence / record / conclusion 及执行索引。

## 偏离项

- `alpha_formal_signal_event` 暂未物理删除 `malf_context_4 / lifecycle_rank_*`，因为当前 `position` 仍在消费这些列；本次仅把它们降级为派生兼容字段，不再承载正式判断。
- `pytest` 仍会报告 `cache_dir` 未知配置告警，但不影响本次 bounded 验证。

## 备注

- `structure / filter` 的正式落表已不再保存旧 `new_* / failure_* / lifecycle_*` 判断壳。
- 当前执行指针已从 `33` 推进到 `34-malf-multi-timeframe-downstream-consumption-card-20260411.md`。

## 记录结构图

```mermaid
flowchart LR
    CANON["canonical malf fields"] --> STR["structure v2 output"]
    STR --> FLT["filter v2 output"]
    FLT --> ALPHA["alpha v2 output"]
    ALPHA --> FAMILY["family payload structured fingerprint"]
    FAMILY --> NEXT["34 继续推进"]
```
