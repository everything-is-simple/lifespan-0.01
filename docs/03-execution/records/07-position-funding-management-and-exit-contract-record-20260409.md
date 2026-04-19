# position 资金管理与退出账本合同 记录

记录编号：`07`
日期：`2026-04-09`

## 对应卡片

- `docs/03-execution/07-position-funding-management-and-exit-contract-card-20260409.md`

## 对应证据

- `docs/03-execution/evidence/07-position-funding-management-and-exit-contract-evidence-20260409.md`

## 实施摘要

1. 先按仓库纪律回读系统宪章、文档先行治理、路线图、执行索引和 `position` lessons，确认 07 的目标是“正式合同冻结”，不是先写代码。
2. 定向抽取旧仓 `position` 章程、规格、`positioning` 研究线 spec，以及 `81 / 82 / 291 / 293 / 294` 的已定论执行卡，作为新仓正式合同输入。
3. 新增 `docs/01-design/modules/position/01-position-funding-management-and-exit-charter-20260409.md`，把 `position` 主语、资金管理分表结构、动作角色语义和历史账本自然键原则写死。
4. 新增 `docs/02-spec/modules/position/01-position-funding-management-and-exit-spec-20260409.md`，把正式输入、正式输出、表合同、自然键、增量规则与激活方法边界写死。
5. 回填 07 的 card / conclusion，并新增 08 草卡 `position 账本表族落库与 bootstrap`，让执行索引切到真正的下一锤。
6. 同步准备治理检查命令，确保 07 的闭环和 08 的门禁入口都能被正式验证。

## 偏离项与风险

- 本轮只冻结了合同，没有进入 `src/mlq/position` 的 schema/bootstrap 实现。
- `probe_entry / confirm_add` 已有正式语义落点，但仍是预留状态；后续是否打开，仍取决于 `trade carry` 与多腿开仓桥接是否冻结。
- `remaining_portfolio_capacity_weight` 的正式来源已经从“固定常量”推进到“必须有正式快照来源”，但组合侧最终读模型仍待后续卡继续冻结。

## 流程图

```mermaid
flowchart LR
    OLD[老仓 position/PAS 结论吸收] --> CONTRACT[资金管理与退出合同冻结]
    CONTRACT --> ENTRY[08草卡 + 入口同步]
    ENTRY --> OK[07卡收口]
```
