# 系统完工账本

日期：`2026-04-09`
状态：`生效中`

1. 当前下一锤：`26-mainline-truthfulness-revalidation-after-malf-sidecar-bootstrap-card-20260411.md`
2. 正式主线剩余卡：`1`
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
22. 收缩 `malf` 正式核心：按时间级别独立运行的纯语义走势账本生效，bridge v1 继续保留为兼容层
23. 冻结 `pivot-confirmed break` 与 `same-timeframe stats sidecar` 的机制层边界，正式把 break 确认和统计读数从 `malf core` 中剥离为只读 sidecar
24. 落地 bridge-era `malf` 机制层账本、bounded runner、checkpoint 与最小 `structure / filter` sidecar 接入

## 当前口径

1. 最新生效结论锚点已切到 `26`。
2. 当前治理锚点暂保留在 `26`；这不表示 `26` 未完成，而是下一张 `system` 主线卡尚未正式打开。
3. 卡 `23` 已正式把 `malf` 核心收缩为纯语义走势账本，并把高周期 `context` 与动作接口移出核心定义；`牛逆 / 熊逆` 也已收紧为本级别过渡状态。
4. 卡 `24` 已正式把 `pivot-confirmed break` 与 `same-timeframe stats sidecar` 冻结为只读机制层 sidecar，不再允许回写 `malf core`。
5. 卡 `22` 继续保留 data 日更 source governance 作为当前前置运营事实。
6. `25` 已经把机制层 sidecar 正式落成账本、runner、checkpoint 与最小下游接入。
7. `26` 已裁决：当前不需要另开后置修复卡，下一张主线卡应直接进入 `system`。
