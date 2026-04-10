# 结论目录

日期：`2026-04-09`
状态：`生效中`

本文记录当前执行区已经建立的结论入口。当前最新已生效结论锚点：`22-data-daily-source-governance-sealing-conclusion-20260411.md`

## 当前结论文档

1. `01-governance-tooling-and-environment-bootstrap-conclusion-20260409.md`
2. `02-shared-ledger-contract-and-pytest-path-fix-conclusion-20260409.md`
3. `03-doc-first-gating-checker-conclusion-20260409.md`
4. `04-module-lessons-and-execution-index-rename-conclusion-20260409.md`
5. `05-system-roadmap-and-progress-tracker-conclusion-20260409.md`
6. `06-roadmap-legacy-module-absorption-conclusion-20260409.md`
7. `07-position-funding-management-and-exit-contract-conclusion-20260409.md`
8. `08-position-ledger-table-family-bootstrap-conclusion-20260409.md`
9. `09-position-formal-signal-runner-and-bounded-validation-conclusion-20260409.md`
10. `10-alpha-formal-signal-contract-and-producer-conclusion-20260409.md`
11. `11-structure-filter-formal-contract-and-minimal-snapshot-conclusion-20260409.md`
12. `12-alpha-trigger-ledger-and-five-table-family-minimal-materialization-conclusion-20260409.md`
13. `13-alpha-five-table-family-shared-contract-and-family-ledger-bootstrap-conclusion-20260409.md`
14. `14-portfolio-plan-minimal-ledger-and-position-bridge-conclusion-20260409.md`
15. `15-trade-minimal-runtime-ledger-and-portfolio-plan-bridge-conclusion-20260410.md`
16. `16-data-malf-minimal-official-mainline-bridge-conclusion-20260410.md`
17. `17-raw-base-strong-checkpoint-and-dirty-materialization-conclusion-20260410.md`
18. `18-daily-raw-base-fq-incremental-update-source-selection-conclusion-20260410.md`
19. `19-tdxquant-daily-raw-source-ledger-bridge-conclusion-20260410.md`
20. `20-index-block-raw-base-incremental-bridge-conclusion-20260410.md`
21. `21-system-ledger-incremental-governance-hardening-conclusion-20260410.md`
22. `22-data-daily-source-governance-sealing-conclusion-20260411.md`

## 已冻结基础口径

1. 文档先行治理已经生效，正式实现继续受 `doc-first gating` 约束。
2. `position / portfolio_plan / trade` 最小正式账本与 bounded runner 已成立。
3. `data -> raw_market -> market_base -> malf -> structure` 最小官方前半段主链已成立。
4. `raw/base` 已升级为 run/file/request/checkpoint/dirty queue 齐备的正式历史账本。
5. `TdxQuant(dividend_type='none')` 已正式桥接进入股票 `raw_market`，并联动 `market_base.none`。
6. `txt -> raw_market -> market_base` 正式主链现已覆盖 `stock + index + block` 三类资产，并具备一次性建仓与每日断点续传增量更新能力。
7. `data` 模块当前日更源头治理已经封存：`stock` 以 `TdxQuant(none)` 为日更主路、`txt` 为 fallback；`index/block` 继续以 `H:\tdx_offline_Data` txt 为日更主路；未来若要统一 source adapter 必须另开新卡。
