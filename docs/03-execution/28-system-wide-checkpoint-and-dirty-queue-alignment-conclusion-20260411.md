# system-wide checkpoint and dirty queue alignment 结论

结论编号：`28`
日期：`2026-04-11`
状态：`已裁决`

## 裁决

- 接受：所有本地正式模块必须以 data-grade `checkpoint + dirty/work queue + replay/resume` 作为统一治理标准，`run_id` 收回到审计角色。

## 原因

- `data` 已经率先成立 `file/request/instrument checkpoint + dirty queue + replay`，而 `malf` 与下游模块此前续跑颗粒度不一致；`28` 必须先把统一基线裁决清楚，后续正式实现才不会继续分叉。
- `29-32` 已实际完成并裁决，证明按自然数顺排先完成 `malf` 优先卡组是正确路径：先冻结 canonical 语义合同，再落 data-grade runner，再改绑 downstream，最后做 truthfulness revalidation。
- 当前测试已恢复为 `59 passed`，其中 bridge v1 兼容测试也已同步显式指定旧表输入，说明“canonical 默认入口 + bridge v1 显式回退”这一治理边界已经成立。

## 影响

- 自 `28` 起，后续卡的正式施工顺序固定为 `29 -> 30 -> 31 -> 32 -> 100 -> 101 -> 102 -> 103 -> 104 -> 105`。
- `29-32` 被正式确认为 malf 优先卡组；`100-105` 被正式确认为 malf 收口后的后置 trade/system 恢复施工卡组。
- `105-system-runtime-orchestration-bootstrap-card-20260411.md` 被正式固定为最后一张后置卡，而不是当前施工卡。
