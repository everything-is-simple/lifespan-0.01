# structure/filter 正式分层与最小 snapshot 记录

记录编号：`11`
日期：`2026-04-09`

## 做了什么

1. 在 `10` 完成后，回看系统路线图、`alpha` 结论和老仓 `malf / filter` 边界结论，确认下一张卡的真实缺口是 `structure / filter` 官方出口不存在。
2. 为 `structure` 新增最小正式 design/spec，冻结了 `structure_run / structure_snapshot / structure_run_snapshot`、结构事实字段组、自然键与最小 runner 合同。
3. 为 `filter` 新增最小正式 design/spec，冻结了 `filter_run / filter_snapshot / filter_run_snapshot`、pre-trigger 最小准入字段组与最小 runner 合同。
4. 新开 `11` 号卡，并把执行索引切换到 `structure/filter 正式分层与最小 snapshot`。

## 偏离项

- `new_execution_bundle.py` 在回填结论目录时依赖了旧分栏标题，本轮自动注册中断；已手动修复索引，不影响 11 号卡生效为当前待施工卡。

## 备注

- 本轮只完成 doc-first 开卡与正式前置冻结，没有开始实现 `structure / filter` 代码。
- 下一轮若直接继续施工，应围绕最小 snapshot 三表与 runner 展开，而不是回头扩 `position` 或直接跳去 `trade / system`。
