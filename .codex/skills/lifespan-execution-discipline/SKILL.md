---
name: lifespan-execution-discipline
description: "固化 lifespan-0.01 仓库的正式执行纪律。适用于本仓库内涉及代码、文档、schema、脚本、测试或执行文档的任务；执行前必须先读当前结论与卡目录，再按卡片、证据、记录、结论和索引回填闭环收口。"
---

# Lifespan 执行纪律

## 总览

在 `H:\lifespan-0.01` 内做正式工作时使用本 skill。
不要把这个仓库当成普通代码仓库，而要把它当成受治理约束的历史账本系统。

需要确认阅读顺序时，先看 [references/reading-map.md](references/reading-map.md)。
需要套用文档模板时，先看 [references/templates.md](references/templates.md)。

## 硬规则

1. 先读现行 `conclusion`，再读卡目录和当前待施工卡。
2. 严守五根目录边界：`repo / data / temp / report / validated`。
3. 严守主链冻结口径：`data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade -> system`。
4. 正式文档默认中文；正式 Python 文件必须带必要中文注释或中文 docstring。
5. 缺少任意一件 `card / evidence / record / conclusion`，都不算正式收口。
6. `pytest` cache、`pytest` basetemp、smoke 临时产物必须进入 `H:\Lifespan-temp`。
7. 只要治理规则、环境脚手架、路径契约、测试入口、执行入口发生变化，就必须同步刷新 `AGENTS.md`、`README.md`、`pyproject.toml`。
8. 一旦要改 `src/`、`scripts/`、`.codex/` 下的正式实现，当前待施工卡必须已经具备需求、设计、规格和任务分解，并通过 `python scripts/system/check_doc_first_gating_governance.py`。

## 默认执行顺序

除非用户明确要求“只读分析”，否则默认按下面顺序推进：

1. 先读当前 `conclusion`
2. 再读 `22-card-catalog`
3. 再读 `77-system-completion-ledger`
4. 打开当前待施工卡
5. 确认卡片已链接需求、设计和任务分解
6. 在允许边界内实现
7. 跑测试或命令形成证据
8. 回填 `record / conclusion / catalogs / ledger`

## 必须先开卡的场景

遇到下面这些场景，必须先有卡再实现：

1. 新增或改动正式脚本行为
2. 修改 schema、路径契约或模块边界
3. 新增正式输出表、正式报告或正式账本
4. 新增治理脚本或环境脚手架
5. 新开一条执行线
6. 调整仓库入口文件治理口径
7. 进入任何正式代码生成或正式脚本改写

## 脚本入口

常用命令：

1. `python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 2 --slug example-task --title 示例任务 --register --set-current-card --dry-run`
2. `python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked`
3. `python scripts/system/check_development_governance.py`
4. `python scripts/system/check_doc_first_gating_governance.py`
