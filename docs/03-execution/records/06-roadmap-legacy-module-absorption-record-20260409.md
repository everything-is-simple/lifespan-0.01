# 路线图老仓来源吸收与系统总控增强 记录

记录编号：`06`
日期：`2026-04-09`

## 对应卡片

- `docs/03-execution/06-roadmap-legacy-module-absorption-card-20260409.md`

## 对应证据

- `docs/03-execution/evidence/06-roadmap-legacy-module-absorption-evidence-20260409.md`

## 实施摘要

1. 新开第 `06` 张执行卡，并把当前施工入口切到“老仓来源吸收与系统总控增强”。
2. 对 `G:\MarketLifespan-Quant` 与 `G:\EmotionQuant-gamma` 做定向梳理，按模块成熟度把来源分成核心已验证、支持性较强、研究偏少和新建边界四层。
3. 新增 `docs/04-reference/Γ-legacy-module-source-grounding-map-20260409.md`，把各模块主要来源、当前吸收方式和不应误判成“已可直接沿袭”的点写清楚。
4. 增强 `α / β / Ω` 三份路线图文档，把“主要来源 / 继承方式 / 置信度 / 当前不敢写死的点”纳入正式路线图合同。
5. 同步刷新 `README.md`、`AGENTS.md`、`docs/README.md`、`pyproject.toml` 与 `A-execution-reading-order-20260409.md`，让入口文件知道路线图已经升级。
6. 跑治理检查与执行索引检查，确认当前第 `06` 张卡满足文档先行门禁，且执行索引与当前卡一致。

## 偏离项与风险

- 本轮只增强了系统级路线图与来源图谱，没有直接进入 `position` 的正式 design/spec。
- 当前模块来源与置信度仍然是人工裁决版看板，不是自动从卡、结论和代码状态回收生成。
