# malf 机制层 break 确认与同级别统计 sidecar 卡

卡号：`24`
日期：`2026-04-11`
状态：`待执行`

## 上游依据

1. `docs/01-design/modules/malf/03-malf-pure-semantic-structure-ledger-charter-20260411.md`
2. `docs/02-spec/modules/malf/03-malf-pure-semantic-structure-ledger-spec-20260411.md`
3. `docs/03-execution/23-malf-pure-semantic-ledger-boundary-freeze-conclusion-20260411.md`

## 目标

在 `23` 号卡已冻结的 `malf core` 公理层之上，补齐两项机制层议题：

1. 明确 `pivot-confirmed break` 是否进入 `malf core` 的正式硬规则，以及它与 `break 触发 / 新推进确认` 的关系。
2. 明确 `same-timeframe stats sidecar` 的正式边界、实体、自然键与下游消费方式，确保其不会反向污染 `state`。

## 本卡范围

### 范围内

1. 补齐 `break` 的确认机制边界。
2. 冻结同级别统计 sidecar 的设计与规格边界。
3. 明确 `malf -> structure -> filter` 如何读取这些机制层能力，而不改写 `malf core` 公理层。

### 范围外

1. 新增 canonical pure semantic runner 代码实现。
2. 回测、胜率、收益、动作建议。
3. 多级别 `context` 回写 `malf core`。

## 历史账本约束

1. 实体锚点：`instrument + timeframe`
2. 业务自然键：待在设计/规格中冻结 `break confirmation` 与 `stats sidecar` 的自然键
3. 批量建仓：说明如何从官方 `market_base(backward)` 一次性构建
4. 增量更新：说明如何按 `instrument + timeframe` 增量续跑
5. 断点续跑：说明 checkpoint / dirty queue / replay 契约
6. 审计账本：说明 run / evidence / record / conclusion 如何留痕

## 预期交付

1. `malf` 机制层 design
2. `malf` 机制层 spec
3. `card / evidence / record / conclusion` 闭环
4. 与 `structure / filter` 上下游合同的必要同步修订

## 完成判据

1. `pivot-confirmed break` 是否进入 `malf core` 被正式写清。
2. `same-timeframe stats sidecar` 的边界被正式写清。
3. 不再把统计、背景或动作重新塞回 `malf core`。
4. 文档索引与主线入口完成同步。
