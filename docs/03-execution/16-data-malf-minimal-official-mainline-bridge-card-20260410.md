# data/malf 最小官方主线桥接

卡片编号：`16`
日期：`2026-04-10`
状态：`已完成`

## 需求

- 问题：
  当前仓库正式实物并没有把 `data -> malf` 这一段主线建起来：`raw_market` 不存在，`market_base` 与 `malf` 只有样例级数据，但下游 `structure -> trade` 已经先跑下去了。
  这会导致执行区把“空上游 + 真下游”误写成主线已通。
- 目标结果：
  为新仓补出最小官方 `data -> malf` 主线，只先做：
  - `TDX 离线股票日线 -> raw_market`
  - `raw_market -> market_base.stock_daily_adjusted`
  - `market_base -> malf.pas_context_snapshot / structure_candidate_snapshot`
  - 与现有 `structure` runner 的真实官方对接
- 为什么现在做：
  当前真正缺口不在 `trade` 或 `system`，而在主线前半段根本没有官方可跑上游。
  如果不先补 `data -> malf`，后续任何“整链已通”的说法都是假通。

## 设计输入

- `docs/01-design/modules/data/01-tdx-offline-raw-and-market-base-bridge-charter-20260410.md`
- `docs/01-design/modules/malf/01-market-base-to-malf-minimal-snapshot-bridge-charter-20260410.md`
- `docs/02-spec/modules/data/01-tdx-offline-raw-and-market-base-bridge-spec-20260410.md`
- `docs/02-spec/modules/malf/01-market-base-to-malf-minimal-snapshot-bridge-spec-20260410.md`
- `docs/02-spec/modules/structure/01-structure-formal-snapshot-spec-20260409.md`

## 任务分解

1. 冻结 `data` 的最小正式合同与 runner。
   - 建立 `raw_market` 文件级检查点与股票日线镜像。
   - 建立 `market_base.stock_daily_adjusted` 正式物化。
2. 冻结 `malf` 的最小正式合同与 runner。
   - 只消费官方 `market_base.stock_daily_adjusted`
   - 产出 `pas_context_snapshot / structure_candidate_snapshot`
3. 建立 `data -> malf -> structure` 的 bounded 官方验证。
   - 真实写入 `H:\Lifespan-data`
   - 留下 `inserted / reused / rematerialized` 证据
4. 回填执行索引、入口文件与路线图，纠正“前半段已通”的错误口径。

## 实现边界

- 范围内：
  - `src/mlq/data`
  - `src/mlq/malf`
  - `scripts/data/*`
  - `scripts/malf/*`
  - `tests/unit/data/*`
  - `tests/unit/malf/*`
  - 与 `structure` 对接所需的最小验证
- 范围外：
  - `alpha trigger candidate` 正式生产
  - `trade / system` 新功能
  - 指数与板块正式下游账本展开
  - 全市场一次性全量重建

## 收口标准

1. `raw_market` 最小正式股票日线镜像成立。
2. `market_base.stock_daily_adjusted` 能从官方 `raw_market` 物化。
3. `malf` 最小官方快照能从官方 `market_base` 物化。
4. 现有 `structure` runner 能真实消费新生成的官方 `malf` 上游。
5. 单元测试与 bounded pilot 证据具备。
6. 记录写完。
7. 结论写完。
