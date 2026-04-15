# structure filter tail coverage truthfulness rectification 记录
`记录编号`：`61`
`日期`：`2026-04-15`

## 执行过程概述

本卡核心工作为分析与裁决，不涉及 schema 或 runner 代码改写。主要步骤：

1. **查询正式库数据**：写临时脚本 `scripts/system/card61_evidence_query.py`，分别读取 `structure.duckdb` 和 `filter.duckdb` 的覆盖统计与 checkpoint 状态。
2. **机制溯源**：对照 `src/mlq/structure/runner.py` 与 `src/mlq/filter/runner.py` 中的 `_load_*_dirty_scopes`、`_upsert_*_checkpoint` 逻辑，确认 bounded/queue 两条路径在 checkpoint 写入上的差异。
3. **定性 truthfulness vs completeness**：基于数据事实作出正式裁决并写入 conclusion。

## 关键发现

### 发现 1：checkpoint_queue 机制不写历史，只记 tail 点

`_load_filter_dirty_scopes` 从 `structure_checkpoint` 读取，将每个标的的 `tail_start_bar_dt -> last_completed_bar_dt` 作为 filter 的 replay 窗口。由于 structure_checkpoint 中 `tail_start == last_completed`（queue 模式在完成 scope 后写入同一日期），filter queue 每个 scope 实际只处理 1 天的结构数据。这是 queue 机制的设计决定：它是为增量 dirty 更新设计的，而不是为历史全量建库设计的。

### 发现 2：两套 bounded run 来源不对等

- Structure bounded run：Jan-Apr 2010，产出 120,641 行密集覆盖，**无 checkpoint**
- Filter bounded run：Jan 4-7 2010（仅 4 天），产出 5,000 行，**无 checkpoint**

Filter bounded run 的时间窗口远窄于 structure bounded run，导致 Filter 在 Feb/Mar/Apr 共约 59 个 signal_dates（~87,851 行 structure 数据）上没有任何 admission 判定。

### 发现 3：filter_checkpoint 精确镜像 structure_checkpoint

两者的 `last_completed_bar_dt` / `tail_start_bar_dt` 分布完全一致（1,833 entries，min=2010-03-19，max=2010-12-31，31 distinct dates），说明 filter queue run 完全依赖 structure queue 的 tail 状态来驱动自身范围。

### 发现 4：结构性 completeness 缺口

2010 全年 filter 覆盖只有 6,833 行 / 35 signal_dates，其中 5,000 行（73%）来自 Jan 4-7 bounded run，其余 1,833 行（27%）来自 queue tail（每标的 1 天）。filter 对 Jan 5-7 以后、全年绝大多数 structure 密集事实的 admission 判定是**缺失的**，而不是 blocked。

## 无代码改写原因

本卡的范围是裁决和文档，不是修复。修复路径（即 `80-86` 的全量 bounded 重建）将在后续卡中执行。当前正式库的 structure/filter 数据不做破坏性修改；修复只会在正式建库时以全量 bounded run 覆盖现有 tail-sparse 状态。

## 对 59 结论表述的收紧

59 结论中描述 "structure_snapshot(2010) 落表 125,516 行" 和 "filter_snapshot(2010) 落表 6,833 行"，并把两者并列列为 "2010 pilot 的真实正式库表事实已经完整闭环" 的证据。本卡明确：
- 上述行数是准确的，但 "完整闭环" 仅指 **truthfulness**（引用链完整），不指 **completeness**（全历史覆盖）。
- filter 的 6,833 行主要来自 Jan 4-7（4 天）+ queue tail（31 天），**Feb-Apr 的密集 structure 事实在 filter 层完全缺失**。
- 正式收紧表述：`59` 证明了 middle-ledger truthfulness gate 通过，但 **2010 全年 filter completeness** 未被 `59` 证明，且当前状态明确为"不完整"。

## 变更清单

| 类型 | 路径 | 说明 |
| --- | --- | --- |
| 新增 evidence | `docs/03-execution/61-*..-evidence-20260415.md` | 数据事实固化 |
| 新增 record | `docs/03-execution/61-*..-record-20260415.md` | 本文件 |
| 新增 conclusion | `docs/03-execution/61-*..-conclusion-20260415.md` | 正式裁决 |
| card 状态更新 | `61-...-card-20260415.md` | 改为已完成 |
| 临时脚本 | `scripts/system/card61_evidence_query.py` | 用后删除 |
| 无 | `src/mlq/structure/runner.py` | 本卡不改代码 |
| 无 | `src/mlq/filter/runner.py` | 本卡不改代码 |
