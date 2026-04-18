# malf alpha 双主轴重构范围冻结

`卡号`：`78`
`日期`：`2026-04-18`
`状态`：`待施工`

## 需求

- 问题：`raw/base` 已完成 `day/week/month` 分库，但当前 `78-84` 口径仍把 `malf` 写成单库多 timeframe，把 `structure` 写成单 day 出口，把 `filter` 写成职责未定的过渡层，也没有把 `malf` 全覆盖与 downstream bounded replay 的边界拆开。
- 目标结果：冻结新的 `malf -> structure -> filter -> alpha` 正式主链边界，明确：
  - `malf` 是 `day / week / month` 三库公共语义真值层，且必须全覆盖
  - `structure` 也跟随拆成 `day / week / month` 三个薄投影层
  - `filter` 保留一个 day 薄 gate 库，只做 objective gate + note sidecar
  - `alpha` 保留五个 PAS 日线终审库，不再为五个 trigger 各自再拆 `D/W/M`
- 为什么现在做：如果不先把这四条冻结清楚，`79-84` 就会一边做路径、一边改架构口径，最后每张卡都带半个设计决策。

## 设计输入

- 设计文档：`docs/01-design/modules/system/18-malf-alpha-dual-axis-and-timeframe-native-refactor-charter-20260418.md`
- 规格文档：`docs/02-spec/modules/system/18-malf-alpha-dual-axis-and-timeframe-native-refactor-spec-20260418.md`

## 层级归属

- 主层：`malf -> alpha` 上游 authority framework
- 次层：`78-84` 执行治理与卡组顺序冻结
- 上游输入：`18` 设计/规格、`77` 六库收口结果与现行 `Ω` 路线图
- 下游放行：`79-84` 的单卡施工顺序，以及 `84` 后是否允许恢复 `100-105`
- 本卡职责：先把双主轴、三库/三薄层/day gate/五 PAS 的正式边界写死，再让后续卡各自只做单一施工事项

## 任务分解

1. 冻结双主轴口径，确认 `malf` 是市场语义真值层，`alpha` 是决策真值层。
2. 冻结 `malf` 必须全覆盖，`79-83` 的 `2010 ~ 当前` bounded replay 只适用于 downstream，不适用于 `malf` 本体。
3. 冻结 `structure` 跟随 `malf` 拆成 `structure_day / structure_week / structure_month` 三个薄事实投影层。
4. 冻结 `filter` 保留一个 day 薄 gate 库，默认只消费 day 决策入口所需客观事实，不随 `malf` 再拆三库。
5. 冻结 `alpha` 按 `BOF / TST / PB / CPB / BPB` 五个 PAS 拆成五个日线官方库，并明确五个 trigger 不各自再拆 `D/W/M` 三套账本。
6. 把 `79-84` 切成一张卡只做一件事的顺序，并同步 `100-105` 的恢复前置条件。

## 实现边界

- 范围内：`78-84` 的模块主次关系、卡组顺序、`malf` 全覆盖例外、`filter` 的明确处理方案，以及 `alpha` 五库但不三套 trigger 库的边界。
- 范围外：本卡不直接改代码，不直接做 replay，不直接恢复 `100-105`。

## 历史账本约束

- 实体锚点：沿用 `asset_type + code` 为主锚点。
- 业务自然键：沿用各账本既有自然键；本卡只冻结主次关系与 replay 边界，不引入 `run_id` 业务主语义。
- 批量建仓：
  - `malf`：必须支持 `day / week / month` 三库全历史建仓并最终达成全覆盖
  - `structure / filter / alpha`：允许围绕 `2010-01-01` 至当前 official `market_base` 覆盖尾部做 bounded replay
- 增量更新：新口径下各模块继续由 queue/checkpoint 负责增量续跑，不允许回退成一次性全量脚本。
- 断点续跑：`malf` 三库、`structure` 三库、`filter_day` 与 `alpha` 五库都必须各自保留 checkpoint / replay 闭环。
- 审计账本：执行审计仍通过 `run / checkpoint / work_queue / summary_json + evidence / record / conclusion` 闭环。

## 正式设计清单

| 设计项 | 正式口径 | 不接受情形 |
| --- | --- | --- |
| 双主轴主权 | `malf` 负责市场语义真值，`alpha` 负责终审决策真值 | 继续把 `structure/filter` 写成隐性终审层 |
| `malf` 覆盖口径 | `malf_day / week / month` 必须全覆盖；downstream 才允许 bounded replay | 把 `malf` 也偷换成 `2010-01-01 -> 当前` 尾部 replay |
| `structure` 分层 | `structure_day / structure_week / structure_month` 三个薄投影层分别绑定对应 `malf_*` | 把 `structure` 写回 `malf_day` 单层出口 |
| `filter` 边界 | `filter_day` 保留一个 day 级薄 gate 库，只做 objective gate + note sidecar | 把 `filter` 再拆 `D/W/M` 三库，或保留终审权 |
| `alpha` 形态 | `alpha_bof / alpha_tst / alpha_pb / alpha_cpb / alpha_bpb` 五个 PAS 日线官方库 | 为五个 trigger 再拆 `5 × 3` 套 `D/W/M` 账本 |
| 卡组顺序 | `79 -> 80 -> 81 -> 82 -> 83 -> 84` 一张卡只做一件事，`84` 后才看 `100-105` | 边做路径边改 authority，或绕过 `84` 先推下游 |

## 实施清单

| 切片 | 实施内容 | 交付物 |
| --- | --- | --- |
| 切片 1 | 回读 `18` 设计/规格与 `78-84 / 100-105` 当前卡面，定位旧 authority 残留 | 差异清单 |
| 切片 2 | 冻结双主轴、`malf` 全覆盖例外与 downstream bounded replay 边界 | 设计/规格与卡面裁决 |
| 切片 3 | 冻结 `structure D/W/M`、`filter_day`、`alpha` 五 PAS 的模块职责边界 | 模块边界说明 |
| 切片 4 | 冻结 `79-84` 顺序与 `84 -> 100-105` 的放行关系 | 卡组顺序与 gate 说明 |
| 切片 5 | 回填 authority 文档、执行索引、evidence 与 record | 文档闭环 |

## A 级判定表

| 判定项 | A 级通过标准 | 阻断条件 | 对下游影响 |
| --- | --- | --- | --- |
| 双主轴冻结 | `malf` 与 `alpha` 的主权边界明确且无重叠 | `structure/filter` 仍持有隐性终审权 | `79-84` 无法单卡施工 |
| `malf` 例外写死 | `malf` 全覆盖与 downstream bounded replay 被分开定义 | 仍把两者混写成一条完成度 | `80/84` 验收失真 |
| `structure` 三薄层 | `structure_day / week / month` 被写成正式默认口径 | 仍残留 `malf_day` 单层表述 | `81/83` 上下文边界不稳 |
| `filter_day` 边界 | `filter_day` 明确只做 objective gate + note sidecar | 仍把 `filter` 写成待定层或终审层 | `82/83` 会反复回滚 |
| `alpha` 五 PAS | 五个 PAS 日线库与“不做 `5 × 3`”写清 | `alpha` 继续单库混写或膨胀成 `5 × 3` | `83` 无法稳定 cutover |
| 下游放行 | `100-105` 前置条件明确改成 `84` 接受 | 仍残留旧 gate 或旧编号 | `100-105` upstream authority 不可信 |

## 收口标准

1. `78-84` 的职责顺序冻结清楚：
   - `79` 只做 `malf` 三库路径/表族契约
   - `80` 只做 `malf` timeframe native source + 全覆盖
   - `81` 只做 `structure` 三薄层
   - `82` 只做 `filter_day` 薄 gate
   - `83` 只做 `alpha` 五 PAS 日线终审重绑
   - `84` 只做 truthfulness / cutover gate
2. `malf` 全覆盖被写成明确例外，不再和 downstream 的 bounded replay 混写成一条。
3. `structure` 被正式冻结为 `D/W/M` 三薄层，而不是只保留 `malf_day` 单出口。
4. `filter` 的处理方案明确为“保留一个 day 薄 gate 库，只拦 objective gate，只附 note sidecar”。
5. `alpha` 五个 PAS 日线库写清楚，且明确“不为五个 trigger 再拆独立 `D/W/M` 三套账本”。
6. `100-105` 的恢复前置条件明确改为 `84` 接受。

## 卡片结构图

```mermaid
flowchart LR
    G77["77 raw/base 六库收口"] --> C78["78 范围冻结"]
    C78 --> C79["79 malf 三库路径/表族"]
    C79 --> C80["80 malf native source + 全覆盖"]
    C80 --> C81["81 structure D/W/M 三薄层"]
    C81 --> C82["82 filter_day objective gate"]
    C82 --> C83["83 alpha 五 PAS 日线终审"]
    C83 --> C84["84 cutover gate"]
    C84 --> C100["100-105"]
```
