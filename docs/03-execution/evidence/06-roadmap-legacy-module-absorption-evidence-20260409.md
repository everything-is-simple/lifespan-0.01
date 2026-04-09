# 路线图老仓来源吸收与系统总控增强 证据

证据编号：`06`
日期：`2026-04-09`

## 命令

```text
rg --files "G:\MarketLifespan-Quant\docs" | rg "roadmap|completion|mainline|bridge|bridging|progress|milestone|backtest|charter|spec"
rg --files "G:\EmotionQuant-gamma" | rg "roadmap|completion|mainline|bridge|bridging|progress|milestone|position|portfolio|trade|system|design|spec"
python scripts\system\check_development_governance.py
python .codex\skills\lifespan-execution-discipline\scripts\check_execution_indexes.py --include-untracked
git status --short
```

## 关键结果

- 已从 `G:\MarketLifespan-Quant` 与 `G:\EmotionQuant-gamma` 抽出可直接支撑新路线图的核心材料：
  - 全模块踩坑总表
  - 模块职责与依赖矩阵
  - `position / alpha / malf` 的旧模块 design/spec
  - 研究线 `positioning / normandy / gene` 的阶段化结论
- `α / β / Ω` 已同步增强，不再只写阶段与下一锤，也开始正式记录：
  - 老仓来源分层
  - 模块来源与继承方式
  - 模块置信度
  - 当前不敢写死的点
- 已新增参考层图谱，明确 `position / alpha / malf` 是核心吸收对象，`data / system` 为支持性较强模块，`trade / core` 偏薄，`structure / filter / portfolio_plan` 属于新系统正式新建边界。
- `check_development_governance.py` 通过，说明当前第 `06` 张卡已脱离模板态，入口文件与路线图联动口径未破。
- `check_execution_indexes.py --include-untracked` 通过，说明 `A / B / C` 执行索引与当前第 `06` 张卡保持一致。

## 产物

- `docs/04-reference/Γ-legacy-module-source-grounding-map-20260409.md`
- `docs/01-design/α-system-roadmap-and-progress-tracker-charter-20260409.md`
- `docs/02-spec/β-system-roadmap-and-progress-tracker-spec-20260409.md`
- `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
- `docs/03-execution/06-roadmap-legacy-module-absorption-card-20260409.md`
