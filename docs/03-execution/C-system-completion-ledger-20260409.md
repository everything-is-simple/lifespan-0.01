# 系统完工账本

日期：`2026-04-09`
状态：`生效中`

1. 当前下一锤：`15-trade-minimal-runtime-ledger-and-portfolio-plan-bridge-card-20260409.md`
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
12. 建立 `alpha trigger ledger` 最小三表、bounded runner 与正式 pilot，并让 `alpha formal signal` 稳定引用官方 trigger 事实
13. 冻结 `alpha` 五表族共享 contract 与 family ledger bootstrap，先在一到两个核心 family 上证明正式账本层、bounded pilot 与 rerun 审计成立
14. 冻结 `portfolio_plan` 最小共享 contract 与 `position -> portfolio_plan` 官方桥接，建立组合层最小三表、bounded pilot 与 rerun 审计
15. 冻结 `trade_runtime` 最小共享 contract 与 `portfolio_plan -> trade` 官方桥接，建立执行层最小五表、carry 主语与 bounded pilot

当前口径：
- `15` 已完成；主线现已推进到 `trade` 最小正式账本层。
- `data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade` 的最小官方主线已经建立。
- `system` 仍未开工；如需继续主线，必须先新开执行卡，不得绕过闭环。
- 在下一张卡开出前，执行区索引仍以 `15` 作为当前锚定卡。
