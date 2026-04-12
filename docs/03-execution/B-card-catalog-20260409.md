# 卡片目录

日期：`2026-04-09`  
状态：`持续更新`

1. 当前下一锚：`37-system-governance-historical-debt-backlog-burndown-card-20260412.md`
2. 当前待施工卡：`37-system-governance-historical-debt-backlog-burndown-card-20260412.md`
3. 正式主线剩余卡：`6`
4. 可选 Sidecar 剩余卡：`0`
5. 后置修复剩余卡：`1`

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
37. `37-system-governance-historical-debt-backlog-burndown-card-20260412.md`
100. `100-trade-signal-anchor-contract-freeze-card-20260411.md`
101. `101-position-entry-t-plus-1-open-reference-price-correction-card-20260411.md`
102. `102-trade-exit-pnl-ledger-bootstrap-card-20260411.md`
103. `103-trade-backtest-progression-runner-card-20260411.md`
104. `104-mainline-real-data-smoke-regression-card-20260411.md`
105. `105-system-runtime-orchestration-bootstrap-card-20260411.md`

## 当前说明

1. 最新生效结论锚点已推进到 `36-malf-wave-life-probability-sidecar-bootstrap-conclusion-20260412.md`。
2. 当前治理锚点仍是 `28-system-wide-checkpoint-and-dirty-queue-alignment-card-20260411.md`，但当前具体待施工卡已切换为 `37-system-governance-historical-debt-backlog-burndown-card-20260412.md`。
3. `29-36` 已完成并生效；`37` 作为系统治理清账卡承接在 `36` 之后，`100-105` 继续作为后续 trade/system 恢复卡组。
4. 当前卡组排序保持 `29 -> 30 -> 31 -> 32 -> 33 -> 34 -> 35 -> 36 -> 37 -> 100 -> 101 -> 102 -> 103 -> 104 -> 105`。

## 卡组顺序图

```mermaid
flowchart LR
    G01["01-06 治理入口"] --> P07["07-09 position"]
    P07 --> A10["10-13 alpha/structure/filter"]
    A10 --> T14["14-15 portfolio_plan/trade"]
    T14 --> D16["16-25 data/malf/system"]
    D16 --> M29["29-35 malf canonical downstream"]
    M29 --> M35["36 malf sidecar 已完成"]
    M35 --> G37["37 system governance 清账"]
    G37 --> POST["100-105 trade/system 恢复"]
```
```
