# system governance historical debt backlog burndown 记录

日期：`2026-04-12`

1. 以全仓治理扫描结果为基线，确认当前剩余历史债务清单已经落在 `scripts/system/development_governance_legacy_backlog.py`。
2. 修正 `.codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py`，使新开卡与执行索引同步逻辑回到正式口径。
3. 拆分 `src/mlq/system/runner.py` 并用 `system` 单测验证后，将其从历史 hard backlog 移除。
4. 拆分 `src/mlq/trade/runner.py` 并用 `trade` 单测验证后，将其从历史 hard backlog 移除。
5. 拆分 `src/mlq/alpha/trigger_runner.py` 并用 `alpha` 单测验证后，将其从历史 hard backlog 移除。
6. 拆分 `src/mlq/filter/runner.py` 并用 `filter` 单测验证后，将其从历史 hard backlog 移除。
7. 拆分 `src/mlq/malf/mechanism_runner.py` 并用 `malf mechanism` 单测验证后，将其从历史 hard backlog 移除。
8. 拆分 `src/mlq/malf/canonical_runner.py` 并用 `malf canonical` 单测验证后，将其从历史 hard backlog 移除。
9. 拆分 `src/mlq/structure/runner.py` 并用 `structure` 单测验证后，将其从历史 hard backlog 移除。
10. 拆分 `src/mlq/alpha/runner.py` 并用 `alpha` 单测验证后，将其从历史 hard backlog 移除。
11. 将 `src/mlq/data/runner.py` 拆为 formal orchestrator + `data_shared / data_common / data_raw_support / data_raw_runner / data_tdxquant / data_market_base_scope / data_market_base_materialization / data_market_base_runner`。
12. 将 `tests/unit/data/test_data_runner.py` 拆为 `test_raw_ingest_runner.py / test_tdxquant_runner.py / test_market_base_runner.py`。
13. 修正 `data_shared.py`、`data_tdxquant.py`、`data_market_base_scope.py` 的切片边界与中文治理锚点，消除机械拆分带来的语法与治理误差。
14. 通过 `py_compile`、改动路径治理检查与 `data` 串行单测，确认 `src/mlq/data/runner.py` 与 `tests/unit/data/test_data_runner.py` 可从历史 hard backlog 移除。
15. 将 `src/mlq/data/bootstrap.py` 的 cleanup / deduplicate / constraint repair helper 抽到 `src/mlq/data/data_bootstrap_maintenance.py`，保持正式入口与表族契约不变。
16. 通过 `py_compile`、按路径治理检查与 `data` 串行单测，确认 `src/mlq/data/bootstrap.py` 可从 `LEGACY_TARGET_OVERSIZE_BACKLOG` 移除。
17. 将 `src/mlq/malf/runner.py` 拆分为 bridge v1 bounded orchestrator + `snapshot_shared / snapshot_source / snapshot_materialization`，保持 `run_malf_snapshot_build` 外部入口、bridge v1 表族契约与兼容输出语义不变。
18. 通过 `py_compile` 确认 `src/mlq/malf/runner.py` 与新增 helper 模块可稳定落盘并通过基础语法门禁。
19. 通过按路径治理检查、全仓治理检查、doc-first gating、执行索引检查与 `tests/unit/malf/test_malf_runner.py` 串行单测，确认 `src/mlq/malf/runner.py` 可从 `LEGACY_TARGET_OVERSIZE_BACKLOG` 移除。
20. 回填 `37` 的 card / evidence / record / conclusion、入口文件与 backlog 台账，使当前正式口径回到“hard backlog 已清零、target backlog 剩余 3 项”。
