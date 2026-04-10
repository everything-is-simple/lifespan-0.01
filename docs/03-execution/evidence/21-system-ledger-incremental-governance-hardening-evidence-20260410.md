# 全系统历史账本增量治理硬约束证据

证据编号：`21`
日期：`2026-04-10`

## 命令

```text
python scripts/system/check_doc_first_gating_governance.py
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card21_system tests/unit/system/test_doc_first_gating_governance.py -q
python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card20_data tests/unit/data/test_data_runner.py -q
```

## 关键结果

- 共享合同已升级：
  - [03-historical-ledger-shared-contract-charter-20260409.md](H:/lifespan-0.01/docs/01-design/03-historical-ledger-shared-contract-charter-20260409.md)
  - [03-historical-ledger-shared-contract-spec-20260409.md](H:/lifespan-0.01/docs/02-spec/03-historical-ledger-shared-contract-spec-20260409.md)
- 当前卡模板已升级，新增 `## 历史账本约束` 六条声明：
  - [00-card-template-20260409.md](H:/lifespan-0.01/docs/03-execution/00-card-template-20260409.md)
- `doc-first gating` 已升级为硬校验当前卡的：
  - 需求
  - 设计输入
  - 任务分解
  - 历史账本约束
- 门禁检查通过：
  - `check_doc_first_gating_governance.py`
  - `check_execution_indexes.py --include-untracked`
- 单测通过：
  - `tests/unit/system/test_doc_first_gating_governance.py`
  - 结果：`2 passed`
- 前置数据事实保持成立：
  - `tests/unit/data/test_data_runner.py`
  - 结果：`13 passed`
  - 说明 `stock/index/block raw->base` 的一次性建仓、增量更新、断点续跑与 replay 语义仍保持成立

## 产物

- 文档：
  - [01-system-ledger-incremental-governance-hardening-charter-20260410.md](H:/lifespan-0.01/docs/01-design/modules/system/01-system-ledger-incremental-governance-hardening-charter-20260410.md)
  - [01-system-ledger-incremental-governance-hardening-spec-20260410.md](H:/lifespan-0.01/docs/02-spec/modules/system/01-system-ledger-incremental-governance-hardening-spec-20260410.md)
- 治理脚本：
  - [check_doc_first_gating_governance.py](H:/lifespan-0.01/scripts/system/check_doc_first_gating_governance.py)
- 单测：
  - [test_doc_first_gating_governance.py](H:/lifespan-0.01/tests/unit/system/test_doc_first_gating_governance.py)
- 入口文件：
  - [AGENTS.md](H:/lifespan-0.01/AGENTS.md)
  - [README.md](H:/lifespan-0.01/README.md)
  - [pyproject.toml](H:/lifespan-0.01/pyproject.toml)
