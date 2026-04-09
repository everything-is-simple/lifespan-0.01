# data 模块经验冻结

日期：`2026-04-09`
状态：`生效中`

## 当前职责

- `raw_market` 保存原始行情镜像
- `market_base` 保存校正后的正式行情事实
- 作为 `malf / alpha / trade / system` 的正式行情依据层

## 必守边界

1. `raw -> base` 两级结构不能合并。
2. 本地离线来源可以变化，但正式 raw 落点必须回到五根目录契约。
3. 第三方校验源只能做审计与对照，不能反向改写正式行情事实。

## 已验证坑点

1. 复权路径最怕窗口截断，基准日一断整条因子链就会断。
2. 市场代码映射不完整时，会出现“本地有数据、正式 raw 却几乎空洞”的假缺口。
3. 极端股本变更会让 base 层路径数值不稳定，表现成个股异常截断。

## 新系统施工前提

1. 下游大范围扩样前，先做 readiness 与 freshness 检查。
2. 局部问题优先 targeted repair，不要动不动全库重建。
3. 正式行情合同对齐优先于物理 rematerialization。

## 来源

1. 老系统总表 `battle-tested-lessons-all-modules-and-mainline-bridging-20260408.md`
2. 老系统 `data` 模块长期 selective rebuild / readiness 章程
