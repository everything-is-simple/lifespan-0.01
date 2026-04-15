# 主线本地账本标准化 bootstrap 证据

证据编号：`39`
日期：`2026-04-13`

## 命令证据

1. `python -m pytest tests/unit/data/test_mainline_standardization_bootstrap.py tests/unit/core/test_paths.py -q`
   - 结果：`6 passed in 1.97s`

## 实现证据

- `src/mlq/data/data_mainline_standardization.py`
  - 冻结主线 `10` 个正式 ledger 的标准清单
  - 为每个 ledger 绑定官方目标路径与 bootstrap 入口
  - 支持显式 `source_ledger_paths` 一次性迁移
  - 输出 JSON / Markdown 审计摘要
- `scripts/data/run_mainline_local_ledger_standardization_bootstrap.py`
  - 提供 `39` 对应的正式 CLI 入口
- `tests/unit/data/test_mainline_standardization_bootstrap.py`
  - 验证官方路径 bootstrap
  - 验证显式 legacy source 复制到标准库并保留落表事实

## 演练摘要

- 演练链路：`legacy structure.duckdb -> official H:\Lifespan-data\structure\structure.duckdb`
- 迁移动作：`copied_from_source`
- 对账方式：复制后重新 bootstrap schema，并读取 `structure_run` 行数确认迁移事实仍然存在
