# system governance historical debt backlog burndown 记录

记录编号：`37`
日期：`2026-04-12`

## 做了什么

1. 以全仓治理扫描结果为基线，确认当前剩余历史债务清单已经落在 `scripts/system/development_governance_legacy_backlog.py`。
2. 新开 `37` 执行 bundle，并把当前待施工卡从 `100` 切换为 `37`。
3. 补写 `37` 对应的 design / spec / card / evidence / record / conclusion。
4. 把 2026-04-12 已完成的首批纠偏项登记为 `37` 的已解决项。
5. 修正 `new_execution_bundle.py` 的模板编号渲染和索引分栏标题错误。

## 偏离项

- `new_execution_bundle.py` 自动回填在实际执行中失败，原因是脚本使用的目录分栏标题和仓库现状不一致；本次已先修脚本，再手工补齐 `37` 索引与当前卡状态。

## 备注

- 当前最新生效结论锚点仍保持为 `36`，`37` 只是新的治理清债施工卡，不代表新的生效业务结论。
- `100-105` 未取消，只是暂时后移到 `37` 之后。

## 记录结构图

```mermaid
flowchart LR
    STEP[施工步骤] --> DEV[偏离项说明]
    DEV --> NOTE[备注]
    NOTE --> CON[结论引用]
```
