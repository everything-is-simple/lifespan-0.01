# position 资金管理与退出账本合同 证据

证据编号：`07`
日期：`2026-04-09`

## 命令

```text
Get-Content -Encoding UTF8 -Raw docs/01-design/00-system-charter-20260409.md
Get-Content -Encoding UTF8 -Raw docs/01-design/01-doc-first-development-governance-20260409.md
Get-Content -Encoding UTF8 -Raw docs/01-design/α-system-roadmap-and-progress-tracker-charter-20260409.md
Get-Content -Encoding UTF8 -Raw docs/01-design/modules/position/00-position-module-lessons-20260409.md
Get-Content -Encoding UTF8 -Raw docs/02-spec/00-repo-layout-and-docflow-spec-20260409.md
Get-Content -Encoding UTF8 -Raw docs/02-spec/01-doc-first-task-gating-spec-20260409.md
Get-Content -Encoding UTF8 -Raw docs/02-spec/Ω-system-delivery-roadmap-20260409.md
Get-Content -Encoding UTF8 -Raw docs/03-execution/07-position-funding-management-and-exit-contract-card-20260409.md
Get-Content -Encoding UTF8 -Raw G:\MarketLifespan-Quant\docs\01-design\modules\position\00-position-charter-20260326.md
Get-Content -Encoding UTF8 -Raw G:\MarketLifespan-Quant\docs\02-spec\modules\position\01-position-spec-20260326.md
Get-Content -Encoding UTF8 -Raw G:\EmotionQuant-gamma\positioning\02-implementation-spec\01-positioning-baseline-and-sizing-spec-20260313.md
Get-Content -Encoding UTF8 -Raw G:\EmotionQuant-gamma\positioning\02-implementation-spec\02-partial-exit-contract-spec-20260314.md
Get-Content -Encoding UTF8 -Raw G:\MarketLifespan-Quant\docs\03-execution\81-position-risk-sizing-baseline-migration-conclusion-20260325.md
Get-Content -Encoding UTF8 -Raw G:\MarketLifespan-Quant\docs\03-execution\82-position-partial-exit-contract-migration-conclusion-20260325.md
Get-Content -Encoding UTF8 -Raw G:\MarketLifespan-Quant\docs\03-execution\291-position-long-only-max-position-contract-reset-card-20260407.md
Get-Content -Encoding UTF8 -Raw G:\MarketLifespan-Quant\docs\03-execution\293-position-real-portfolio-capacity-and-total-cap-reset-card-20260407.md
Get-Content -Encoding UTF8 -Raw G:\MarketLifespan-Quant\docs\03-execution\294-position-positive-weight-and-trim-path-bounded-acceptance-conclusion-20260407.md
python scripts/system/check_development_governance.py
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python scripts/system/check_doc_first_gating_governance.py
git status --short
```

## 关键结果

1. 已新增 `position` 正式 design/spec，两份文档都把下面三类边界写死：
   - 公共账本层与资金管理分表层
   - `run_id` 只做审计、自然键承担历史主语
   - “测试仓 / 主仓 / 加码”降成 `position` 内部动作角色，而不是顶层二分
2. 已把 `trim_to_context_cap / final_allowed_position_weight / blocked candidate` 固定成显式账本事实，并把旧仓 `291 / 293 / 294` 的核心结论吸收到新合同。
3. 已补齐 07 的 card / evidence / record / conclusion，并新增 08 草卡，把执行索引切到 `position` 表族落库与 bootstrap。
4. 治理检查已实际验证：
   - 07 已脱离模板态
   - 执行索引与当前卡一致
   - 当前待施工卡 08 已具备 design/spec 链接，满足文档先行门禁入口

## 产物

1. `docs/01-design/modules/position/01-position-funding-management-and-exit-charter-20260409.md`
2. `docs/02-spec/modules/position/01-position-funding-management-and-exit-spec-20260409.md`
3. `docs/03-execution/07-position-funding-management-and-exit-contract-card-20260409.md`
4. `docs/03-execution/07-position-funding-management-and-exit-contract-conclusion-20260409.md`
5. `docs/03-execution/08-position-ledger-table-family-bootstrap-card-20260409.md`
6. `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
7. `docs/03-execution/A-execution-reading-order-20260409.md`

## 证据流图

```mermaid
flowchart LR
    OLD[老仓 position 结论吸收] --> SPEC[position design/spec 落地]
    SPEC --> GATE[门禁+索引检查通过]
    GATE --> C07[07结论+08草卡生成]
```
