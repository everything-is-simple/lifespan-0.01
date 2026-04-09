# structure/filter 正式分层与最小 snapshot 结论

结论编号：`11`
日期：`2026-04-09`
状态：`草稿`

## 裁决

- 接受：当前下一张正式主线卡应先回到 `malf -> structure -> filter` 的正式分层，而不是继续深挖 `position` 或直接开 `trade / system`。
- 接受：本页当前只作为 `11` 号卡的草稿结论位，待正式实现、evidence、record 完成后再转为生效结论。
- 拒绝：把本页草稿结论表述成“`structure / filter` 已经正式落地”。

## 原因

- `10` 已经收口 `alpha -> position` 官方桥接，当前真实剩余阻塞转移到了更上游正式结构与准入层。
- 老仓结论已经明确：`structure` 负责结构事实，`filter` 负责 pre-trigger admission，二者都不应继续寄生在 `alpha` 或旧 `malf` 兼容字段里。

## 影响

- 当前待施工卡应切换到 `11-structure-filter-formal-contract-and-minimal-snapshot-card-20260409.md`。
- 下一轮正式实现将围绕 `structure_snapshot / filter_snapshot` 及其最小 runner 展开。
