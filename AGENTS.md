# 仓库协作规则

本文档为自动化代理和协作者提供 `lifespan-0.01` 的最小可执行仓库规则。

## 1. 仓库定位

`lifespan-0.01` 是一个面向个人 PC 的、本地优先的历史账本系统。
系统第一目标不是短期跑通，而是长期可运行、可续跑、可复算、可审计。

必须始终服从这些现实约束：

- 数据量大
- 本地 `cpu / memory / io` 受限
- 很多计算不能反复全量重跑
- 中间事实必须长期沉淀

## 2. 权威入口与阅读顺序

进入仓库后，不要直接改代码。
默认先读：

1. `README.md`
2. `docs/README.md`
3. `docs/01-design/00-system-charter-20260409.md`
4. `docs/01-design/01-doc-first-development-governance-20260409.md`
5. `docs/01-design/α-system-roadmap-and-progress-tracker-charter-20260409.md`
6. `docs/01-design/modules/README.md`
7. `docs/02-spec/00-repo-layout-and-docflow-spec-20260409.md`
8. `docs/02-spec/01-doc-first-task-gating-spec-20260409.md`
9. `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
10. `docs/03-execution/README.md`

如果只是追当前正式口径，优先看 `conclusion`。
如果要继续正式实现，再回到对应 `card / evidence / record`。

补充理解：

- `α / β / Ω` 三份路线图文档不只负责阶段进度，也负责说明各模块主要继承自哪些老仓来源、当前继承方式与置信度是什么。

## 3. 五根目录纪律

系统采用五根目录协作：

1. `H:\lifespan-0.01`
   - 代码、文档、测试、治理脚本
2. `H:\Lifespan-data`
   - 正式数据库与长期数据资产
3. `H:\Lifespan-temp`
   - working DB、缓存、pytest、smoke、benchmark 等临时产物
4. `H:\Lifespan-report`
   - 人读报告、图表、导出物
5. `H:\Lifespan-Validated`
   - 正式验证资产快照

禁止把临时工作库、缓存、benchmark 产物长期堆在仓库内部。
`pytest` cache、`pytest` basetemp、smoke 临时产物、环境重建过程中的临时文件，都必须进入 `H:\Lifespan-temp`。

## 4. 正式模块边界

`src/mlq` 下的正式模块是：

- `core`
- `data`
- `malf`
- `structure`
- `filter`
- `alpha`
- `position`
- `portfolio_plan`
- `trade`
- `system`

当前主链冻结为：

`data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade -> system`

补充规则：

1. `PAS` 是 `alpha` 内部能力，不再是顶层模块。
2. `structure` 负责把 `malf` 结构语义沉淀为官方结构事实层。
   - 当前 `structure` 的正式 bounded runner 入口为 `scripts/structure/run_structure_snapshot_build.py`，只允许从官方 `malf` 结构候选事实与官方执行上下文物化 `structure_run / snapshot / run_snapshot`，不允许夹带 `filter / alpha / position` 判定逻辑。
3. `filter` 负责 pre-trigger 准入。
   - 当前 `filter` 的正式 bounded runner 入口为 `scripts/filter/run_filter_snapshot_build.py`，只允许消费官方 `structure snapshot` 与最小执行上下文物化 `filter_run / snapshot / run_snapshot`，不允许硬拦截研究观察或夹带 `alpha detector / position / trade` 逻辑。
4. `alpha` 负责对下游冻结正式 `formal signal` 事实。
   - 当前 `alpha` 的正式 bounded producer 入口为 `scripts/alpha/run_alpha_formal_signal_build.py`，只允许从官方触发事实与官方 `filter / structure snapshot` 上游物化 `alpha_formal_signal_run / event / run_event`，不允许夹带 `position` sizing 或 `trade / system` 逻辑。
5. `position` 负责单标的仓位计划与资金管理。
   - 当前 `position` 的正式 bounded runner 入口为 `scripts/position/run_position_formal_signal_materialization.py`，只允许消费官方 `alpha formal signal` 与 `market_base` 参考价，不允许回读 `alpha` 内部临时过程。
6. `portfolio_plan` 负责组合层计划、组合回测、容量协调。
7. `trade` 负责执行与成交账本，不承担组合研究职责。
8. `system` 负责编排、治理、审计、冻结，不保存策略事实主数据。

## 5. 历史账本原则

本系统中的数据库不是一次运行产物，而是历史账本。
正式数据库应优先满足：

1. 自然键累积
2. 增量更新
3. 断点续跑
4. 中间事实永续存储
5. 尽量减少重复 CPU/IO 成本

`run_id` 可以保留为批次与审计元数据，但不能再充当历史账本主语义。
正式运行时应优先通过 `enter_repo.ps1` 或环境变量加载五根目录；`repo_root.parent` 形式的相邻目录回退只服务测试和开发兜底，不代表正式环境默认口径。

## 6. 文档先行规则

正式实现默认遵循：

`需求 -> 设计 -> 任务分解 -> card -> implementation -> evidence -> record -> conclusion`

硬规则：

1. 先有 `design / spec`，再开 `card`，再实现。
2. 任何正式代码生成、Schema 变更、Pipeline 新增、行为改写，都必须先具备需求、设计、任务分解。
3. 缺少上述前置文档，不允许进入正式实现。
4. 缺少 `card / evidence / record / conclusion` 任意一件，不算正式完成。
5. 进入 `src/`、`scripts/`、`.codex/` 下的正式实现前，当前待施工卡必须已经通过 `doc-first gating` 检查。

## 7. 入口文件规则

下面三个文件是仓库入口，不允许长期滞后于当前治理口径：

1. `AGENTS.md`
2. `README.md`
3. `pyproject.toml`

只要治理规则、环境脚手架、路径契约、测试入口、执行入口发生变化，就必须同步刷新这三个入口文件。
其中 `docs/01-design/`、`docs/02-spec/` 与 `src/mlq/core/paths.py` 的正式口径变化，也视为入口变化。

## 8. 文档规则

正式文档默认中文。
新增文档时：

1. 不要直接复制旧系统过时口径。
2. 参考资料只放在 `docs/04-reference/`。
3. 当前正式事实必须写在 `design / spec / execution conclusion` 中。
4. 执行区默认先读 `conclusion`，不要把历史 `card` 当成当前真相。

## 9. 代码规则

1. 复杂实现必须写必要的中文注释，重点解释边界、契约、增量逻辑、断点续跑和反直觉规则。
2. 不要为了单次运行方便，破坏历史账本结构。
3. 不要把临时脚本逻辑直接混进正式运行时模块。
4. 模块之间只通过正式契约和正式输出对接，不直接依赖彼此内部实现细节。

## 10. 测试与验证

修改正式逻辑后，至少要补下面之一，最好同时具备：

1. 单元测试
2. 可复现命令
3. 运行证据
4. 落表事实或导出产物摘要

证据必须可追溯，不能只写“已验证”。

## 11. 提交与推送

提交前应确认：

1. 改动边界清晰
2. 文档闭环已补齐
3. 测试或证据已具备

提交信息建议使用：

`<area>: <summary>`
