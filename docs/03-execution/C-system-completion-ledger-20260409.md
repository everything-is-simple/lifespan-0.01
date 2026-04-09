# 系统完工账本

日期：`2026-04-09`
状态：`生效中`

1. 当前下一锤：`11-structure-filter-formal-contract-and-minimal-snapshot-card-20260409.md`
2. 正式主线剩余卡：`0`
3. 可选 Sidecar 剩余卡：`0`
4. 后置修复剩余卡：`0`

## 本轮主线

1. 修复 `pytest` 临时目录路径
2. 冻结历史账本共享合同
3. 建立 `doc-first gating` 硬门禁
4. 提炼老系统模块经验并整理执行入口命名
5. 建立系统级路线图与进度跟踪器
6. 为系统级路线图补齐老仓来源 grounding、继承方式与置信度
7. 冻结 `position` 资金管理与退出合同，并把下一锤切到表族落库与 bootstrap
8. 建立 `position` 最小账本表族、默认 policy seed 与 `alpha formal signal` 最小消费入口
9. 建立 `position` 正式 bounded runner，连通 `alpha formal signal` 读取、`market_base` 参考价 enrichment 与 bounded validation
10. 建立 `alpha` 官方 formal signal producer、冻结三表正式出口，并完成 `position` 对新仓官方上游的真实对接
11. 建立 `structure` 与 `filter` 的最小 snapshot 三表、bounded runner，并让 `alpha` 默认消费官方上游
