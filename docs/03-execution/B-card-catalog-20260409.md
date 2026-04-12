# 卡片目录

日期：`2026-04-09`  
状态：`持续更新`

1. 当前下一锚：`36-malf-wave-life-probability-sidecar-bootstrap-card-20260411.md`
2. 当前待施工卡：`36-malf-wave-life-probability-sidecar-bootstrap-card-20260411.md`
3. 正式主线剩余卡：`6`
4. 可选 Sidecar 剩余卡：`1`
5. 后置修复剩余卡：`0`

## 正式卡目录

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
27. `27-system-mainline-bounded-acceptance-readout-and-audit-bootstrap-card-20260411.md`
28. `28-system-wide-checkpoint-and-dirty-queue-alignment-card-20260411.md`
29. `29-malf-semantic-canonical-contract-freeze-card-20260411.md`
30. `30-malf-canonical-ledger-and-data-grade-runner-bootstrap-card-20260411.md`
31. `31-structure-filter-alpha-rebind-to-canonical-malf-card-20260411.md`
32. `32-downstream-truthfulness-revalidation-after-malf-canonicalization-card-20260411.md`
33. `33-malf-downstream-canonical-contract-purge-card-20260411.md`
34. `34-malf-multi-timeframe-downstream-consumption-card-20260411.md`
35. `35-downstream-data-grade-checkpoint-alignment-after-malf-card-20260411.md`
36. `36-malf-wave-life-probability-sidecar-bootstrap-card-20260411.md`
100. `100-trade-signal-anchor-contract-freeze-card-20260411.md`
101. `101-position-entry-t-plus-1-open-reference-price-correction-card-20260411.md`
102. `102-trade-exit-pnl-ledger-bootstrap-card-20260411.md`
103. `103-trade-backtest-progression-runner-card-20260411.md`
104. `104-mainline-real-data-smoke-regression-card-20260411.md`
105. `105-system-runtime-orchestration-bootstrap-card-20260411.md`

## 当前说明

1. 最新生效结论锚点已推进到 `35-downstream-data-grade-checkpoint-alignment-after-malf-conclusion-20260412.md`。
2. 当前治理锚点仍是 `28-system-wide-checkpoint-and-dirty-queue-alignment-card-20260411.md`，但当前具体待施工卡已推进到 `36-malf-wave-life-probability-sidecar-bootstrap-card-20260411.md`。
3. `29-35` 已完成并生效；`36` 是当前剩余的 malf sidecar 卡；`100-105` 顺延为其后的 trade/system 恢复卡组。
4. 当前卡组排序调整后，`100-105` 不再紧接 `33`，而是在 `36` 收口后再恢复推进。

## 卡组顺序图

```mermaid
flowchart LR
    G01["01-06 治理入口"] --> P07["07-09 position"]
    P07 --> A10["10-13 alpha/structure/filter"]
    A10 --> T14["14-15 portfolio_plan/trade"]
    T14 --> D16["16-25 data/malf/system"]
    D16 --> M29["29-35 malf canonical downstream"]
    M29 --> M35["36 malf sidecar"]
    M35 --> POST["100-105 trade/system 恢复"]
```
