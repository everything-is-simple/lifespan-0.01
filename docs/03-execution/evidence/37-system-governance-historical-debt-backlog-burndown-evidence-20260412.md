# system governance historical debt backlog burndown 证据

证据编号：`37`
日期：`2026-04-12`

## 命令

```text
python scripts/system/check_development_governance.py
python scripts/system/check_development_governance.py AGENTS.md README.md pyproject.toml scripts/portfolio_plan/run_portfolio_plan_build.py scripts/system/development_governance_legacy_backlog.py scripts/system/run_system_mainline_readout_build.py scripts/trade/run_trade_runtime_build.py src/mlq/malf/wave_life_runner.py src/mlq/malf/wave_life_materialization.py src/mlq/malf/wave_life_shared.py src/mlq/malf/wave_life_source.py src/mlq/portfolio_plan/__init__.py src/mlq/portfolio_plan/bootstrap.py src/mlq/portfolio_plan/runner.py tests/unit/portfolio_plan/__init__.py tests/unit/portfolio_plan/test_bootstrap.py tests/unit/portfolio_plan/test_runner.py tests/unit/trade/__init__.py tests/unit/trade/test_bootstrap.py tests/unit/trade/test_trade_runner.py
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python -m py_compile src/mlq/malf/wave_life_runner.py src/mlq/malf/wave_life_shared.py src/mlq/malf/wave_life_source.py src/mlq/malf/wave_life_materialization.py scripts/portfolio_plan/run_portfolio_plan_build.py scripts/trade/run_trade_runtime_build.py scripts/system/run_system_mainline_readout_build.py src/mlq/portfolio_plan/__init__.py src/mlq/portfolio_plan/bootstrap.py src/mlq/portfolio_plan/runner.py tests/unit/portfolio_plan/__init__.py tests/unit/portfolio_plan/test_bootstrap.py tests/unit/portfolio_plan/test_runner.py tests/unit/trade/__init__.py tests/unit/trade/test_bootstrap.py tests/unit/trade/test_trade_runner.py
python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 37 --slug system-governance-historical-debt-backlog-burndown --title "system governance historical debt backlog burndown" --date 20260412 --status 待执行 --register --set-current-card
pytest tests/unit/malf/test_wave_life_runner.py tests/unit/system/test_mainline_truthfulness_revalidation.py -q
```

## 关键结果

- 全仓治理扫描通过，剩余历史债务已经显式收敛为 `LEGACY_HARD_OVERSIZE_BACKLOG` 与 `LEGACY_TARGET_OVERSIZE_BACKLOG`。
- 新开卡脚本暴露出“目录分栏标题写死错误 + 模板编号重复插入”问题，已被纳入 `37` 的已解决项登记。
- `wave_life` 拆分后的回归测试通过，当前为 `3 passed`。

## 产物

- `docs/01-design/modules/system/11-governance-historical-debt-backlog-burndown-charter-20260412.md`
- `docs/02-spec/modules/system/11-governance-historical-debt-backlog-burndown-spec-20260412.md`
- `docs/03-execution/37-system-governance-historical-debt-backlog-burndown-card-20260412.md`
- `docs/03-execution/evidence/37-system-governance-historical-debt-backlog-burndown-evidence-20260412.md`
- `docs/03-execution/records/37-system-governance-historical-debt-backlog-burndown-record-20260412.md`
- `docs/03-execution/37-system-governance-historical-debt-backlog-burndown-conclusion-20260412.md`

## 证据结构图

```mermaid
flowchart LR
    CMD[命令执行] --> OUT[关键结果]
    OUT --> ART[产物落地]
    ART --> REF[结论引用]
```
