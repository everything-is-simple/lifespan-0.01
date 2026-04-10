# 系统完工账本

日期：`2026-04-09`
状态：`生效中`

1. 当前下一锤：`22-data-daily-source-governance-sealing-card-20260411.md`
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
7. 冻结 `position` 资金管理与退出合同
8. 建立 `position` 最小账本表族与正式 bounded runner
9. 建立 `alpha` 官方 formal signal producer
10. 建立 `structure / filter` 最小正式 snapshot
11. 建立 `alpha trigger ledger`
12. 建立 `alpha family ledger`
13. 建立 `portfolio_plan` 最小正式账本
14. 建立 `trade_runtime` 最小正式账本
15. 打通 `data -> raw_market -> market_base -> malf -> structure` 前半段主链
16. 补齐 `raw/base` 强断点、dirty queue、run/file ledger 与 controlled replay
17. 冻结“官方日更原始事实优先、复权留在仓内物化层、txt 保留 fallback”的源头策略
18. 正式桥接 `TdxQuant(dividend_type='none')` 到股票 `raw_market`
19. 正式桥接 `index/block txt -> raw_market -> market_base`，并完成 full 初始化与 replay 验证
20. 把“批量建仓 + 日更增量 + 断点续跑 + 稳定自然键 + 审计账本”提升为全系统治理硬约束，并接入正式门禁
21. 封存 `data` 模块当前日更源头治理：`stock` 继续走 `TdxQuant(none)` 主路、`txt` 保留 fallback，`index/block` 继续走 `H:\tdx_offline_Data` txt 主路，未来统一 source adapter 必须另开新卡

## 当前口径

1. 最新生效结论锚点已经切到 `22`。
2. 当前下一锤仍保留 `22` 作为治理锚点，直到下一张卡开出。
3. 卡 `22` 已正式封存 `data` 模块当前日更 source governance，不再把 source adapter 统一视为当前已批准实施项。
4. 卡 `20` 的 `index/block raw->base` 真实初始化已经完成，并已被卡 `21/22` 继续保留为 data 治理前置事实。
