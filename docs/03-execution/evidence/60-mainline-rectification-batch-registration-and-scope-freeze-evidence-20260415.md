# mainline rectification batch registration and scope freeze 证据
`证据编号`：`60`
`日期`：`2026-04-15`

## 校验命令

1. `python scripts/system/check_doc_first_gating_governance.py`
   - 结果：通过
   - 说明：当前待施工卡 `62-filter-pre-trigger-boundary-and-authority-reset-card-20260415.md` 已具备需求、设计、规格、任务分解与历史账本约束。
2. `python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked`
   - 结果：通过
   - 说明：conclusion / evidence / card / records / reading-order / completion-ledger 六类索引全部一致。
3. `python scripts/system/check_development_governance.py`
   - 结果：未全绿，但未发现本卡新增违规
   - 说明：剩余失败仅为历史 file-length 债务：
     - `src/mlq/data/data_mainline_incremental_sync.py`（1013 行）
     - `src/mlq/portfolio_plan/runner.py`（1705 行）
     - 以及若干超过 800 行目标上限但未超过硬上限的历史文件

## 索引与文档事实

1. `60 -> 66` 七张整改卡已全部注册到执行目录。
2. `80 -> 86` 已整体后移为整改后的 official middle-ledger 恢复卡组。
3. `100 -> 105` 仍保持在 `86` 之后，不允许越级恢复。
4. 执行入口、执行索引与系统级路线图已统一改写为：
   - `60 -> 66 -> 80 -> 86 -> 100 -> 105`

## 证据要点

1. `README.md / AGENTS.md / docs/02-spec/Ω-system-delivery-roadmap-20260409.md` 已同步到整改批次口径。
2. `docs/03-execution/00-conclusion-catalog-20260409.md / A-execution-reading-order-20260409.md / B-card-catalog-20260409.md / C-system-completion-ledger-20260409.md` 已能一致表达：
   - `60` 为整改批次登记卡
   - `61` 为下一张整改裁决卡
   - `62` 为当前 active 卡
   - `80-86` 不是当前 active 卡组

## 结论支撑

1. `60` 的职责仅限于登记批次、冻结顺序、前移施工位和收紧索引口径。
2. `60` 不直接裁决 `structure / filter / alpha / wave_life` 的实现问题；这些问题由 `61 -> 65` 分卡收口。
