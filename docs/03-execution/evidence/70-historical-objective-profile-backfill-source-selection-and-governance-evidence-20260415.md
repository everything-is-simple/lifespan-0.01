# 历史 objective profile 回补源选型与治理证据

`证据编号：70`
`日期：2026-04-15`

## 命令

```text
python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 70 --slug historical-objective-profile-backfill-source-selection-and-governance --title '历史 objective profile 回补源选型与治理' --date 20260415 --status 草稿 --register --set-current-card
Get-Content docs/03-execution/69-filter-objective-tradability-and-universe-gate-freeze-conclusion-20260415.md
Get-Content docs/02-spec/modules/filter/01-filter-formal-snapshot-spec-20260409.md
Get-Content src/mlq/filter/objective_coverage_audit.py
Get-Content src/mlq/data/data_tdxquant.py
Get-Content src/mlq/data/tdxquant.py
Get-Content H:\。reference\tushare\tushare-5000积分-官方-兜底号
Web: https://tushare.pro/document/2?doc_id=25
Web: https://tushare.pro/document/2?doc_id=214
Web: https://tushare.pro/document/2?doc_id=423
Web: https://pypi.org/project/baostock/
Wheel: baostock-0.9.1-py3-none-any.whl
```

## 关键结果

- `69` 结论已明确把“历史 objective coverage 缺口”拆出为独立治理卡，不再留在 `69` 尾项。
- 真实官方库首轮 audit 已证明当前 `filter_snapshot` 的最小缺口窗口是 `2010-01-04 -> 2026-04-08`，且当前 raw 官方库尚无 `raw_tdxquant_instrument_profile`。
- 本地 `H:\。reference\tushare\tushare-5000积分-官方-兜底号` 证明存在 Tushare 接入条件，但该备忘本身不等于历史真值能力。
- Tushare 官方文档显示：
  - `stock_basic` 可提供 `list_status / list_date / delist_date / market / exchange`
  - `suspend_d` 可按交易日查询停复牌
  - `st` 可提供按 `pub_date / imp_date` 的 ST 风险警示历史事件
- Baostock 官方 PyPI 与 wheel 显示：
  - `query_all_stock(day)` 支持按日期查询当日证券列表
  - `query_stock_basic(...)` 提供基础资料
  - `query_history_k_data_plus(...)` 示例字段包含 `tradestatus` 与 `isST`
- 当前 `TdxQuant get_stock_info(code)` 接口签名不带历史日期参数，因此尚不能证明其能承担历史时点真值回补。

## 产物

- `docs/01-design/modules/data/07-historical-objective-profile-backfill-source-selection-and-governance-charter-20260415.md`
- `docs/02-spec/modules/data/07-historical-objective-profile-backfill-source-selection-and-governance-spec-20260415.md`
- `docs/03-execution/70-historical-objective-profile-backfill-source-selection-and-governance-card-20260415.md`
- `docs/03-execution/evidence/70-historical-objective-profile-backfill-source-selection-and-governance-evidence-20260415.md`
- `docs/03-execution/records/70-historical-objective-profile-backfill-source-selection-and-governance-record-20260415.md`
- `docs/03-execution/70-historical-objective-profile-backfill-source-selection-and-governance-conclusion-20260415.md`

## 证据结构图

```mermaid
flowchart LR
    CMD["开卡与资料核对"] --> OUT["缺口窗口 + 候选源事实"]
    OUT --> ART["70 四件套 + design/spec"]
    ART --> REF["后续 probe 与结论引用"]
```
