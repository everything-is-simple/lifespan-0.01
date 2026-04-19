# malf canonical_materialization 修订与三库重建 证据

`证据编号`：`84`
`日期`：`2026-04-19`

## 命令

```text
python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 84 --slug malf-canonical-materialization-repair-and-three-ledger-rebuild --title "malf canonical_materialization 修订与三库重建" --date 20260419 --status 草稿
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python scripts/system/check_doc_first_gating_governance.py
```

## 关键结果

- `84` 的四件套骨架已生成并保留在正式执行目录。
- `84` 卡正文已回填为正式草稿，明确 canonical 修订、三库 rebuild、变更前/后审计对照与图示。
- 本卡当前只完成开卡，不代表 rebuild 已执行。

## 产物

- `docs/03-execution/84-malf-canonical-materialization-repair-and-three-ledger-rebuild-card-20260419.md`
- `docs/03-execution/evidence/84-malf-canonical-materialization-repair-and-three-ledger-rebuild-evidence-20260419.md`
- `docs/03-execution/records/84-malf-canonical-materialization-repair-and-three-ledger-rebuild-record-20260419.md`
- `docs/03-execution/84-malf-canonical-materialization-repair-and-three-ledger-rebuild-conclusion-20260419.md`

## 证据结构图

```mermaid
flowchart LR
    CMD["bundle + governance checks"] --> CARD["84 rebuild draft"]
    CARD --> AUD["pre/post audit preserved as hard requirement"]
    AUD --> REF["ready for later conclusion"]
```
