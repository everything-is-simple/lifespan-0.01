# structure/filter 正式分层与最小 snapshot

卡片编号：`11`
日期：`2026-04-09`
状态：`已完成`

## 需求

- 问题：
  `10` 已经把 `alpha -> position` 的官方 formal signal 桥接站稳了，但再往上游看，`structure / filter` 仍然没有正式账本出口。结果是后续无论继续补 `alpha` 内部五表族，还是让 `alpha` 彻底脱离旧 `malf` 兼容字段，都会卡在“没有官方结构事实层和官方 pre-trigger 准入层”。
- 目标结果：
  为新仓冻结最小正式 `structure` 与 `filter` 出口，只做：
  `structure_run / structure_snapshot / structure_run_snapshot`
  `filter_run / filter_snapshot / filter_run_snapshot`
  与最小 bounded runner、最小 downstream 对齐合同。
- 为什么现在做：
  当前主线真实剩余阻塞已经从“`alpha` 官方 formal signal 不存在”切换成“`malf -> structure -> filter -> alpha` 这条上游正式链还没有站稳”；如果继续直接补 `alpha` 五表族或下游模块，只会继续把真正缺口后推。

## 设计输入

- `docs/01-design/modules/structure/01-structure-formal-snapshot-charter-20260409.md`
- `docs/01-design/modules/filter/01-filter-formal-snapshot-charter-20260409.md`
- `docs/02-spec/modules/structure/01-structure-formal-snapshot-spec-20260409.md`
- `docs/02-spec/modules/filter/01-filter-formal-snapshot-spec-20260409.md`
- `docs/03-execution/10-alpha-formal-signal-contract-and-producer-conclusion-20260409.md`

## 任务分解

1. 冻结 `structure` 最小正式三表、自然键、最小 runner 合同与 bounded evidence 口径。
2. 冻结 `filter` 最小正式三表、最小准入字段组、最小 runner 合同与 bounded evidence 口径。
3. 让 `alpha` 后续正式实现可以优先消费 `filter_snapshot + structure_snapshot`，不再默认回读旧兼容结构/准入字段。

## 实现边界

- 范围内：
  - `structure_run / structure_snapshot / structure_run_snapshot`
  - `filter_run / filter_snapshot / filter_run_snapshot`
  - 最小 bounded runner 与脚本入口
  - `alpha` 的上游消费合同对齐
  - bounded evidence
- 范围外：
  - `alpha` PAS 五表族全量落地
  - `position / trade / system` 直接消费 `structure / filter`
  - 全量 rule backfill
  - `portfolio_plan` 正式开工

## 收口标准

1. `structure` 最小正式三表与 runner 成立。
2. `filter` 最小正式三表与 runner 成立。
3. `alpha` 已有正式上游消费口径，不再默认依赖旧兼容字段。
4. bounded validation 证据具备。
5. 证据写完。
6. 记录写完。
7. 结论写完。
