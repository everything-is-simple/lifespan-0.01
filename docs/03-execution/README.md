# 执行入口

`docs/03-execution/` 是正式执行闭环区。
这里继承老系统“保留证据、保留记录、保留结论”的纪律，并补上索引账本与当前施工卡。

## 执行闭环

正式闭环是：

`需求 -> 设计 -> 任务分解 -> 卡片 -> 实现 -> 证据 -> 记录 -> 结论`

## 默认阅读顺序

第一次进入本目录时，默认按下面顺序阅读：

1. `00-conclusion-catalog-20260409.md`
2. `B-card-catalog-20260409.md`
3. `C-system-completion-ledger-20260409.md`
4. `00-card-execution-discipline-20260409.md`
5. `A-execution-reading-order-20260409.md`

## 默认动作

如果你只是想知道“现在正式成立了什么”，先看：

1. `00-conclusion-catalog-20260409.md`
2. 对应结论文档

如果你想继续正式实现，先看：

1. `B-card-catalog-20260409.md` 里的“当前待施工卡”
2. 卡片链接的需求、设计、任务分解
3. `C-system-completion-ledger-20260409.md`
4. 再开始实现

## 硬规则

如果执行卡没有明确指向以下内容，就不允许进行正式代码变更：

1. 需求
2. 设计
3. 任务分解

## 目录职责

- 根目录
  - 只放卡片、结论、索引、模板、README 与当前主线账本
- `evidence/`
  - 只放测试证据、命令证据、运行证据、证据目录
- `records/`
  - 只放执行记录、实现轨迹、收口说明

## 目录硬边界

1. 正式 `*-evidence-*` 文档不得停留在 `docs/03-execution/` 根目录
2. 正式 `*-record-*` 文档不得停留在 `docs/03-execution/` 根目录
3. 如发现根目录错放，必须先回迁到对应子目录，再继续后续正式施工
4. 新增执行四件套默认通过 bundle 脚本生成，并由 execution index checker 持续检查

## 收口纪律

任何正式实现任务，缺少以下任意一件，都不能算完成：

1. `card`
2. `evidence`
3. `record`
4. `conclusion`

## 流程图

```mermaid
flowchart LR
    REQ[需求] --> DES[设计] --> TASK[任务分解]
    TASK --> CARD[卡片]
    CARD --> IMPL[实现]
    IMPL --> EV[evidence 证据]
    EV --> REC[record 记录]
    REC --> CON[conclusion 结论]
```
