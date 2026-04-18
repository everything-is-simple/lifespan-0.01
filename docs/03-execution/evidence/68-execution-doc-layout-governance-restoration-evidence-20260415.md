# 执行文档目录治理回迁与固化 证据
证据编号：`68`
日期：`2026-04-15`

## 校验命令

1. `python scripts/system/check_doc_first_gating_governance.py`
   - 结果：通过
   - 说明：`68` 收口后当前待施工卡已恢复为 `78-malf-alpha-dual-axis-refactor-scope-freeze-card-20260418.md`，doc-first gating 继续成立。
2. `python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked`
   - 结果：通过
   - 说明：结论目录、证据目录、卡目录、records 链、阅读顺序、完成账本与 execution 根目录布局已全部一致。
3. `python scripts/system/check_development_governance.py`
   - 结果：通过
   - 说明：本次文档治理没有引入新的 file-length、中文化或入口滞后违规。
4. `Get-ChildItem docs/03-execution -File -Filter *-evidence-*.md`
   - 结果：仅剩 `00-evidence-template-20260409.md`
   - 说明：`38-67` 错放在根目录的正式 evidence 已全部回迁到 `docs/03-execution/evidence/`。
5. `Get-ChildItem docs/03-execution -File -Filter *-record-*.md`
   - 结果：仅剩 `00-record-template-20260409.md`
   - 说明：`38-67` 错放在根目录的正式 record 已全部回迁到 `docs/03-execution/records/`。
6. `python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 69 --slug execution-doc-layout-regression-sentinel --title 执行文档目录治理回归哨兵 --register --set-current-card --dry-run`
   - 结果：通过
   - 说明：bundle 脚本已恢复按 `root card/conclusion + evidence/ + records/` 生成四件套，并能对当前 `B-card-catalog-20260409.md` 的分栏标题完成预填索引。

## 关键结果

1. `docs/03-execution/` 根目录已恢复为只保留 `card / conclusion / index / template / README` 的正式布局。
2. 共回迁 `24` 份 evidence 与 `24` 份 record；其中 `44` 的根目录完整稿正式替换了子目录占位稿。
3. `.codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py` 已修复卡目录分栏标题漂移，能够继续按当前索引结构自动登记四件套。
4. `.codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py` 已新增 `execution-layout` 检查，并修正为只看工作树实际存在文件，避免把已删除待提交项误判为错放文档。
5. `docs/README.md`、`docs/02-spec/00-repo-layout-and-docflow-spec-20260409.md`、`docs/03-execution/README.md`、`README.md`、`AGENTS.md`、`pyproject.toml` 与 `Ω` 路线图都已写入目录硬边界，不再依赖口头记忆。
6. `69` 的 dry-run 证明后续新卡仍会自动把 evidence / record 落到正式子目录，而不是回到根目录。

## 产物

- `docs/03-execution/68-execution-doc-layout-governance-restoration-card-20260415.md`
- `docs/03-execution/68-execution-doc-layout-governance-restoration-conclusion-20260415.md`
- `docs/03-execution/evidence/68-execution-doc-layout-governance-restoration-evidence-20260415.md`
- `docs/03-execution/records/68-execution-doc-layout-governance-restoration-record-20260415.md`
- `docs/03-execution/evidence/38-67 *-evidence-*.md`
- `docs/03-execution/records/38-67 *-record-*.md`
- `.codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py`
- `.codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py`
- `docs/README.md`
- `docs/02-spec/00-repo-layout-and-docflow-spec-20260409.md`
- `docs/03-execution/README.md`
- `README.md`
- `AGENTS.md`
- `pyproject.toml`

## 证据结构图
```mermaid
flowchart LR
    CMD["治理命令"] --> OUT["布局与脚本检查通过"]
    OUT --> ART["48 份文档回迁"]
    ART --> RULE["规则与入口固化"]
    RULE --> REF["68 结论引用"]
```
