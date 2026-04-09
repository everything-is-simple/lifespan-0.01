# 治理工具与环境重建证据

证据编号：`01`
日期：`2026-04-09`

## 命令

```text
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
.venv\Scripts\python.exe scripts/system/check_development_governance.py
.venv\Scripts\python.exe scripts/system/check_entry_freshness_governance.py AGENTS.md README.md pyproject.toml scripts\README.md scripts\system\check_development_governance.py scripts\system\check_entry_freshness_governance.py scripts\system\check_repo_hygiene_governance.py .codex\skills\lifespan-execution-discipline\SKILL.md
.venv\Scripts\python.exe -m pytest tests/unit/core/test_paths.py -q
powershell -ExecutionPolicy Bypass -File scripts/setup/rebuild_windows_env.ps1
```

## 关键结果

- 执行索引检查通过。
- 开发治理检查通过。
- 入口文件新鲜度检查通过。
- `tests/unit/core/test_paths.py` 结果为 `4 passed`。
- `scripts/setup/rebuild_windows_env.ps1` 使用 `D:\miniconda310\python.exe` 成功完成 `.venv` 重建、导入冒烟、治理检查和单元测试。
- `pytest` 的缓存与临时目录已外置到 `H:\Lifespan-temp\pytest-cache` 与 `H:\Lifespan-temp\pytest-tmp`。

## 产物

- `.codex/skills/lifespan-execution-discipline/`
- `scripts/setup/`
- `scripts/system/`
- `.venv/`
- `H:\Lifespan-temp\pytest-cache`
- `H:\Lifespan-temp\pytest-tmp`
