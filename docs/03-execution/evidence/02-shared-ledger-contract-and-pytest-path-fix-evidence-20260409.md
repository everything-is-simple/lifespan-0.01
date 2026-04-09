# 历史账本共享契约与pytest路径修正 证据

证据编号：`02`
日期：`2026-04-09`

## 命令

```text
.venv\Scripts\python.exe -m pytest tests/unit/core/test_paths.py -q
if (Test-Path -LiteralPath H:\Lifespan-temp\pytest-tmp) { Remove-Item -LiteralPath H:\Lifespan-temp\pytest-tmp -Recurse -Force }; New-Item -ItemType Directory -Force -Path H:\Lifespan-temp\pytest-tmp | Out-Null; .venv\Scripts\python.exe -m pytest tests/unit/core/test_paths.py -q
if (Test-Path -LiteralPath H:\Lifespan-temp\pytest-tmp) { Remove-Item -LiteralPath H:\Lifespan-temp\pytest-tmp -Recurse -Force }; New-Item -ItemType Directory -Force -Path H:\Lifespan-temp\pytest-tmp | Out-Null; ..\.venv\Scripts\python.exe -m pytest unit\core\test_paths.py -q
.venv\Scripts\python.exe scripts/system/check_development_governance.py
.venv\Scripts\python.exe .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
```

## 关键结果

- `pytest` 从仓库根目录启动通过，结果 `4 passed`。
- `pytest` 从 `tests/` 子目录启动通过，结果 `4 passed`。
- `pytest` 的缓存和临时目录位于 `H:\Lifespan-temp`，没有回流到仓库根目录。
- 开发治理检查与执行索引检查通过。

## 产物

- `docs/01-design/03-historical-ledger-shared-contract-charter-20260409.md`
- `docs/02-spec/03-historical-ledger-shared-contract-spec-20260409.md`
- `pyproject.toml`
- `H:\Lifespan-temp\pytest-cache`
- `H:\Lifespan-temp\pytest-tmp`
