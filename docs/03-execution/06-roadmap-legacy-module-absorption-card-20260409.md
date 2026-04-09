# 路线图老仓来源吸收与系统总控增强

卡片编号：`06`
日期：`2026-04-09`
状态：`已完成`

## 需求

- 问题：
  当前 `α / β / Ω` 三份路线图文档已经把系统阶段、模块状态和下一锤写出来了，但还没有把“这些判断到底建立在什么老仓来源之上、哪些模块可沿袭、哪些模块只能吸收经验、哪些地方我还不敢写死”说透。
- 目标结果：
  把 `G:\MarketLifespan-Quant` 与 `G:\EmotionQuant-gamma` 中对新系统真正有继承价值的模块来源梳理出来，沉淀为新仓参考图谱，并同步加厚 `α / β / Ω`，让路线图从“导航图”升级为“能裁决的系统总控板”。
- 为什么现在做：
  下一步就要进入 `position` 等核心模块的正式设计；如果不先把老仓来源、继承方式和置信度写清楚，后续会再次出现“口头上知道哪些结论能用，仓库里却没有权威总表”的问题。

## 设计输入

- 设计文档：`docs/01-design/α-system-roadmap-and-progress-tracker-charter-20260409.md`
- 规格文档：`docs/02-spec/β-system-roadmap-and-progress-tracker-spec-20260409.md`
- 参考材料：
  - `G:\MarketLifespan-Quant\docs\04-reference\battle-tested-lessons-all-modules-and-mainline-bridging-20260408.md`
  - `G:\MarketLifespan-Quant\docs\02-spec\02-module-responsibility-and-dependency-matrix-spec-20260320.md`
  - `G:\EmotionQuant-gamma\docs\spec\common\records\development-status.md`
  - `G:\EmotionQuant-gamma\positioning\README.md`

## 任务分解

1. 把老仓模块来源按“核心已验证 / 支持性较强 / 研究偏少 / 新边界模块”四层梳理出来，落成参考层图谱。
2. 改写 `α / β / Ω`，把模块来源、继承方式、置信度和当前不敢写死的点写进系统路线图合同。
3. 同步刷新 `README.md`、`AGENTS.md`、`docs/README.md`、`pyproject.toml` 与执行入口，确保入口文件知道路线图已经升级。
4. 跑治理检查并回填证据、记录、结论。

## 实现边界

- 范围内：
  - 路线图设计/规格/总图增强
  - 参考层来源图谱
  - 执行入口与入口文件同步
  - 最小治理验证
- 范围外：
  - `position` 正式 design/spec 本体
  - `alpha / malf` 具体表合同落地
  - 业务代码迁移与账本 bootstrap

## 收口标准

1. `α / β / Ω` 已补齐模块来源、继承方式、置信度与未定项
2. 新增一份老仓模块来源图谱参考文档
3. 入口文件与执行入口已同步到新的路线图口径
4. `check_development_governance.py` 与 `check_execution_indexes.py` 通过
5. 证据、记录、结论写完
