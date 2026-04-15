# mainline official middle-ledger 2010 pilot scope freeze 证据
`证据编号`：`56`
`日期`：`2026-04-14`

## 实现与验证命令

1. `python scripts/system/check_doc_first_gating_governance.py`
   - 结果：通过
   - 说明：当前待施工卡 `56` 已具备需求、设计、规格、任务分解与历史账本约束，正式通过 doc-first gating。
2. `python scripts/system/check_development_governance.py`
   - 结果：未通过
   - 说明：全仓扫描仅报出既有文件长度债务，命中的 `src/mlq/data/data_mainline_incremental_sync.py`、`src/mlq/portfolio_plan/runner.py` 及若干目标超长文件都与本次 `56` 文档冻结无直接关系。
3. `python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked`
   - 结果：待本卡 `record / conclusion` 与索引前移补齐后执行
   - 说明：本证据先冻结 `56` 的 scope 与门禁状态，再通过索引检查验证闭环一致性。

## 冻结事实

1. 当前正式主线新增 `56 -> 66` 卡组，位于 `55` 与 `100` 之间。
2. `56` 只负责冻结 `2010-01-01 ~ 2010-12-31` pilot 的正式范围、路径和后续卡组边界，不直接进入真实写库。
3. `57` 负责真实 `malf canonical` 2010 bootstrap，`58` 负责 `structure / filter / alpha` 2010 canonical smoke，`59` 负责 pilot truthfulness gate。
4. `60 -> 64` 固定为 `2011-2013 / 2014-2016 / 2017-2019 / 2020-2022 / 2023-2025` 五段三年建库窗口，`65` 对齐 `2026 YTD`，`66` 负责 official cutover gate。
5. 当前仓库入口与路线图口径已经改为“先完成真实正式库 middle-ledger 落地，再恢复 `100 -> 105`”。

## 证据结构图

```mermaid
flowchart LR
    DOC["56 card + design/spec"] --> GATE["doc-first gating"]
    GATE --> FREEZE["2010 pilot freeze"]
    FREEZE --> NEXT["57-66 rollout"]
```
