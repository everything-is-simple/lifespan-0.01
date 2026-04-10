# 结论目录

日期：`2026-04-09`
状态：`生效中`

本文记录当前执行区已经建立的结论入口。
当前最新已生效结论锚点：`17-raw-base-strong-checkpoint-and-dirty-materialization-conclusion-20260410.md`

## 当前结论文件

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

## 已冻结基础口径

1. 仓库文档骨架已经建立。
2. 文档先行治理已经生效。
3. 正式代码生成已受 `doc-first gating` 硬门禁约束。
4. 各正式模块已有新仓内的经验冻结入口，执行区三大索引采用字母命名。
5. 新仓已有正式的系统级总路线图与进度跟踪器。
6. 系统级路线图已补齐老仓来源、继承方式、置信度与未定项口径。
7. 新仓已补齐最小 `structure / filter` 官方 snapshot 合同，并让 `alpha` 默认改读官方上游。
8. 新仓 `alpha` 已补齐最小 `trigger ledger` 三表、bounded runner 与正式 pilot，`trigger ledger -> formal signal` 官方上游关系成立。
9. 新仓 `alpha` 已补齐最小 `family ledger` 三表、bounded runner 与正式 pilot，`trigger ledger -> family ledger -> formal signal` 三级正式分层成立。
10. 新仓 `portfolio_plan` 已补齐最小三表、`position -> portfolio_plan` 官方桥接、bounded pilot 与 `inserted / reused / rematerialized` 组合层审计。
11. 新仓 `trade_runtime` 已补齐最小五表、`portfolio_plan -> trade` 官方桥接、carry 主语、bounded pilot 与 `inserted / reused / rematerialized` 执行层审计。
12. 新仓已补齐缺失的 `data -> raw_market -> market_base -> malf -> structure` 最小官方前半段主线，并把 `market_base` 三套价格与“信号后复权 / 执行不复权”口径冻结到正式合同。
13. 新仓已把 `raw/base` 升级为强断点与脏标的增量账本，补齐 `raw_ingest_run / raw_ingest_file / base_dirty_instrument / base_build_run / base_build_scope / base_build_action`、`force_hash / continue_from_last_run` 与库级唯一约束口径。
