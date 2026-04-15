# 历史 objective profile 回补源选型与治理证据

`证据编号：70`
`日期：2026-04-15`

## 命令

```text
python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 70 --slug historical-objective-profile-backfill-source-selection-and-governance --title '历史 objective profile 回补源选型与治理' --date 20260415 --status 草稿 --register --set-current-card
python -m pip install tushare
python -c "import importlib.util; print('tushare', bool(importlib.util.find_spec('tushare'))); print('baostock', bool(importlib.util.find_spec('baostock'))); print('pandas', bool(importlib.util.find_spec('pandas')))"
Get-Content docs/03-execution/69-filter-objective-tradability-and-universe-gate-freeze-conclusion-20260415.md
Get-Content docs/02-spec/modules/filter/01-filter-formal-snapshot-spec-20260409.md
Get-Content src/mlq/filter/objective_coverage_audit.py
Get-Content src/mlq/data/data_tdxquant.py
Get-Content src/mlq/data/tdxquant.py
Get-Content H:\。reference\tushare\tushare-5000积分-官方-兜底号.md
Web: https://tushare.pro/document/2?doc_id=25
Web: https://tushare.pro/document/2?doc_id=214
Web: https://tushare.pro/document/2?doc_id=423
Web: https://pypi.org/project/baostock/
Wheel: baostock-0.9.1-py3-none-any.whl
python <- bounded probe for baostock baseline
python <- bounded probe for baostock ETF/BJ/ST detail
python <- bounded probe for tushare stock_basic/suspend_d/st
python <- bounded probe for tushare delisted/BSE detail
```

## 关键结果

- `69` 结论已明确把“历史 objective coverage 缺口”拆出为独立治理卡，不再留在 `69` 尾项。
- 真实官方库首轮 audit 已证明当前 `filter_snapshot` 的最小缺口窗口是 `2010-01-04 -> 2026-04-08`，且当前 raw 官方库尚无 `raw_tdxquant_instrument_profile`。
- 本地 `H:\。reference\tushare\tushare-5000积分-官方-兜底号` 证明存在 Tushare 接入条件，但该备忘本身不等于历史真值能力。
- `tushare` 初始未安装，已在当前 Python 环境补装 `tushare==1.4.29`。
- `Tushare` 实测结果：
  - `stock_basic` 可用，返回 `list_status / list_date / delist_date / market / exchange`，并能取到 `list_status='D'` 的退市样本。
  - `stock_basic(exchange='BSE', list_status='L')` 可返回北交所样本。
  - `suspend_d(trade_date='20200312')` 可返回按交易日的停复牌记录。
  - `st(ts_code='300125.SZ')` 当前账号无接口权限，报“没有接口访问权限”。
- `Baostock` 实测结果：
  - `login()` 成功。
  - `query_stock_basic(code='sh.600000')` 返回普通股票样本，`type='1'`。
  - `query_stock_basic(code='sh.510300')` 返回 ETF 样本，`type='5'`。
  - `query_history_k_data_plus(..., 'date,code,tradestatus,isST')` 可返回日级 `tradestatus / isST`。
  - `query_all_stock(day='2024-11-18')` 本次样本仅见 `sh/sz`，未见 `bj`，也未包含测试 ETF `sh.510300`。
  - `query_stock_basic(code='bj.920021')` 本次样本返回空集，北交所覆盖仍需进一步核实。
- 当前 `TdxQuant get_stock_info(code)` 接口签名不带历史日期参数，因此尚不能证明其能承担历史时点真值回补。
- 当前阶段性判断已经出现分层：
  - `Tushare` 更接近“历史事件 + universe/list_status”来源。
  - `Baostock` 更接近“日级状态快照 / 交叉验证”来源。

## 产物

- `docs/01-design/modules/data/07-historical-objective-profile-backfill-source-selection-and-governance-charter-20260415.md`
- `docs/02-spec/modules/data/07-historical-objective-profile-backfill-source-selection-and-governance-spec-20260415.md`
- `docs/03-execution/70-historical-objective-profile-backfill-source-selection-and-governance-card-20260415.md`
- `docs/03-execution/evidence/70-historical-objective-profile-backfill-source-selection-and-governance-evidence-20260415.md`
- `docs/03-execution/records/70-historical-objective-profile-backfill-source-selection-and-governance-record-20260415.md`
- `docs/03-execution/70-historical-objective-profile-backfill-source-selection-and-governance-conclusion-20260415.md`
- `H:\Lifespan-report\data\objective-source-probe-20260415.json`
- `H:\Lifespan-report\data\objective-source-probe-20260415-tushare.json`
- `H:\Lifespan-report\data\objective-source-probe-20260415-tushare-detail.json`
- `H:\Lifespan-report\data\objective-source-probe-20260415-baostock-detail.json`
- `H:\Lifespan-report\data\objective-source-probe-20260415-summary.json`
- `H:\Lifespan-report\data\objective-source-probe-20260415.md`

## 证据结构图

```mermaid
flowchart LR
    CMD["开卡 + 双源 probe 命令"] --> OUT["Tushare/Baostock 实测结果"]
    OUT --> ART["report JSON/Markdown + 70 evidence"]
    ART --> REF["后续 source-selection 结论引用"]
```
