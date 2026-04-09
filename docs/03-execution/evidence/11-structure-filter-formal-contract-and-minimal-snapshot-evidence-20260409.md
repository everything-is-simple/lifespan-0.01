# structure/filter 正式分层与最小 snapshot 证据

证据编号：`11`
日期：`2026-04-09`

## 命令

```text
python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 11 --slug structure-filter-formal-contract-and-minimal-snapshot --title "structure/filter 正式分层与最小 snapshot" --register --set-current-card
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python scripts/system/check_doc_first_gating_governance.py
```

## 关键结果

- 已生成 `11` 号执行卡四件套。
- 已补齐 `structure` 与 `filter` 的最小 design/spec 前置文档：
  - `docs/01-design/modules/structure/01-structure-formal-snapshot-charter-20260409.md`
  - `docs/01-design/modules/filter/01-filter-formal-snapshot-charter-20260409.md`
  - `docs/02-spec/modules/structure/01-structure-formal-snapshot-spec-20260409.md`
  - `docs/02-spec/modules/filter/01-filter-formal-snapshot-spec-20260409.md`
- 由于仓库当前 `00-conclusion-catalog-20260409.md` 不含旧脚本预期的分栏标题，自动注册中断；本轮已手工回填索引并完成恢复。
- 当前待施工卡已切到 `11-structure-filter-formal-contract-and-minimal-snapshot-card-20260409.md`。
- `check_execution_indexes.py --include-untracked` 应作为本轮最终索引一致性验收。
- `check_doc_first_gating_governance.py` 应作为本轮 card 可施工性验收。

## 产物

- `docs/01-design/modules/structure/01-structure-formal-snapshot-charter-20260409.md`
- `docs/01-design/modules/filter/01-filter-formal-snapshot-charter-20260409.md`
- `docs/02-spec/modules/structure/01-structure-formal-snapshot-spec-20260409.md`
- `docs/02-spec/modules/filter/01-filter-formal-snapshot-spec-20260409.md`
- `docs/03-execution/11-structure-filter-formal-contract-and-minimal-snapshot-card-20260409.md`
