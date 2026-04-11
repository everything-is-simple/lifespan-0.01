# 卡片目录

日期：`2026-04-09`
状态：`生效中`

1. 当前下一锤：`26-mainline-truthfulness-revalidation-after-malf-sidecar-bootstrap-card-20260411.md`
2. 当前待施工卡：`26-mainline-truthfulness-revalidation-after-malf-sidecar-bootstrap-card-20260411.md`
3. 正式主线剩余卡：`1`
4. 可选 Sidecar 剩余卡：`0`
5. 后置修复剩余卡：`0`

## 卡片总表

1. `01-governance-tooling-and-environment-bootstrap-card-20260409.md`
2. `02-shared-ledger-contract-and-pytest-path-fix-card-20260409.md`
3. `03-doc-first-gating-checker-card-20260409.md`
4. `04-module-lessons-and-execution-index-rename-card-20260409.md`
5. `05-system-roadmap-and-progress-tracker-card-20260409.md`
6. `06-roadmap-legacy-module-absorption-card-20260409.md`
7. `07-position-funding-management-and-exit-contract-card-20260409.md`
8. `08-position-ledger-table-family-bootstrap-card-20260409.md`
9. `09-position-formal-signal-runner-and-bounded-validation-card-20260409.md`
10. `10-alpha-formal-signal-contract-and-producer-card-20260409.md`
11. `11-structure-filter-formal-contract-and-minimal-snapshot-card-20260409.md`
12. `12-alpha-trigger-ledger-and-five-table-family-minimal-materialization-card-20260409.md`
13. `13-alpha-five-table-family-shared-contract-and-family-ledger-bootstrap-card-20260409.md`
14. `14-portfolio-plan-minimal-ledger-and-position-bridge-card-20260409.md`
15. `15-trade-minimal-runtime-ledger-and-portfolio-plan-bridge-card-20260409.md`
16. `16-data-malf-minimal-official-mainline-bridge-card-20260410.md`
17. `17-raw-base-strong-checkpoint-and-dirty-materialization-card-20260410.md`
18. `18-daily-raw-base-fq-incremental-update-source-selection-card-20260410.md`
19. `19-tdxquant-daily-raw-source-ledger-bridge-card-20260410.md`
20. `20-index-block-raw-base-incremental-bridge-card-20260410.md`
21. `21-system-ledger-incremental-governance-hardening-card-20260410.md`
22. `22-data-daily-source-governance-sealing-card-20260411.md`
23. `23-malf-pure-semantic-ledger-boundary-freeze-card-20260411.md`
24. `24-malf-mechanism-layer-break-confirmation-and-stats-sidecar-card-20260411.md`
25. `25-malf-mechanism-ledger-bootstrap-and-downstream-sidecar-integration-card-20260411.md`
26. `26-mainline-truthfulness-revalidation-after-malf-sidecar-bootstrap-card-20260411.md`

## 当前口径

1. `22` 已把 data 日更源头治理封存为正式结论：`stock` 继续走 `TdxQuant(none)` 主路，`index/block` 继续走 `H:\tdx_offline_Data` txt 主路，不再把“统一 source adapter”视为当前已批准工作。
2. `23` 已把 `malf` 正式核心收缩为按时间级别独立运行的纯语义走势账本，并明确 bridge v1 只是兼容层。
3. `24` 已完成机制层边界冻结：`pivot-confirmed break` 与 `same-timeframe stats sidecar` 的正式边界已成立。
4. `25` 已完成机制层实现卡：`pivot_confirmed_break_ledger / same_timeframe_stats_profile / same_timeframe_stats_snapshot`、bounded runner、checkpoint 与最小下游接入已经落地。
5. `26` 已完成主链 truthfulness 复核：`23/24/25` 之后整链仍真实成立到 `trade`，不需要另开后置修复卡。
6. 最新生效结论锚点已切到 `26`；当前治理锚点暂保留在 `26`，直到下一张 `system` 主线卡正式打开。
