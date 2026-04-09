# 模块经验沉淀与执行入口字母化 证据

证据编号：`04`
日期：`2026-04-09`

## 命令

```text
if (Test-Path -LiteralPath H:\Lifespan-temp\pytest-tmp) { Remove-Item -LiteralPath H:\Lifespan-temp\pytest-tmp -Recurse -Force -ErrorAction SilentlyContinue; Start-Sleep -Milliseconds 300 }; .venv\Scripts\python.exe -m pytest tests\unit\system\test_doc_first_gating_governance.py -q
if (Test-Path -LiteralPath H:\Lifespan-temp\pytest-tmp) { Remove-Item -LiteralPath H:\Lifespan-temp\pytest-tmp -Recurse -Force -ErrorAction SilentlyContinue; Start-Sleep -Milliseconds 300 }; .venv\Scripts\python.exe -m pytest tests\unit\core\test_paths.py -q
.venv\Scripts\python.exe scripts\system\check_development_governance.py
.venv\Scripts\python.exe scripts\system\check_development_governance.py AGENTS.md README.md pyproject.toml docs/README.md docs/01-design/modules/README.md docs/01-design/modules/core/00-core-module-lessons-20260409.md docs/01-design/modules/data/00-data-module-lessons-20260409.md docs/01-design/modules/malf/00-malf-module-lessons-20260409.md docs/01-design/modules/structure/00-structure-module-lessons-20260409.md docs/01-design/modules/filter/00-filter-module-lessons-20260409.md docs/01-design/modules/alpha/00-alpha-module-lessons-20260409.md docs/01-design/modules/position/00-position-module-lessons-20260409.md docs/01-design/modules/portfolio_plan/00-portfolio-plan-module-lessons-20260409.md docs/01-design/modules/trade/00-trade-module-lessons-20260409.md docs/01-design/modules/system/00-system-module-lessons-20260409.md docs/03-execution/A-execution-reading-order-20260409.md docs/03-execution/B-card-catalog-20260409.md docs/03-execution/C-system-completion-ledger-20260409.md docs/03-execution/04-module-lessons-and-execution-index-rename-card-20260409.md docs/03-execution/README.md .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py .codex/skills/lifespan-execution-discipline/references/reading-map.md .codex/skills/lifespan-execution-discipline/references/templates.md scripts/system/check_doc_first_gating_governance.py tests/unit/system/test_doc_first_gating_governance.py
.venv\Scripts\python.exe .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
rg -n "22-card-catalog-20260409|77-system-completion-ledger-20260409|00-execution-reading-order-20260409" H:\lifespan-0.01 --glob "!docs/03-execution/evidence/04-module-lessons-and-execution-index-rename-evidence-20260409.md"
```

## 关键结果

- 模块经验文档已落到 `docs/01-design/modules/`，覆盖 `core / data / malf / structure / filter / alpha / position / portfolio_plan / trade / system`。
- `03-execution` 的 3 个核心入口文件已改成 `A/B/C` 字母命名，并完成脚本、测试与文档引用迁移。
- `check_development_governance.py` 在全仓扫描和按改动范围运行时均通过。
- `check_execution_indexes.py --include-untracked` 通过，说明执行索引与新命名保持一致。
- 清理 `H:\Lifespan-temp\pytest-tmp` 后，`tests/unit/system/test_doc_first_gating_governance.py` 结果 `2 passed`，`tests/unit/core/test_paths.py` 结果 `4 passed`。
- 全仓检索旧执行入口文件名，没有残留引用。

## 产物

- `docs/01-design/modules/README.md`
- `docs/01-design/modules/core/00-core-module-lessons-20260409.md`
- `docs/01-design/modules/data/00-data-module-lessons-20260409.md`
- `docs/01-design/modules/malf/00-malf-module-lessons-20260409.md`
- `docs/01-design/modules/structure/00-structure-module-lessons-20260409.md`
- `docs/01-design/modules/filter/00-filter-module-lessons-20260409.md`
- `docs/01-design/modules/alpha/00-alpha-module-lessons-20260409.md`
- `docs/01-design/modules/position/00-position-module-lessons-20260409.md`
- `docs/01-design/modules/portfolio_plan/00-portfolio-plan-module-lessons-20260409.md`
- `docs/01-design/modules/trade/00-trade-module-lessons-20260409.md`
- `docs/01-design/modules/system/00-system-module-lessons-20260409.md`
- `docs/03-execution/A-execution-reading-order-20260409.md`
- `docs/03-execution/B-card-catalog-20260409.md`
- `docs/03-execution/C-system-completion-ledger-20260409.md`
