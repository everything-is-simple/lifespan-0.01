# raw/base 日周月分库迁移证据

证据编号：`76`
日期：`2026-04-17`

## 命令

```text
git status --porcelain=v1
Get-ChildItem H:\tdx_offline_Data\<asset>-day|-week|-month\Backward-Adjusted
duckdb 查询 raw_market.duckdb / market_base.duckdb 的 day/week/month 表、registry、run、dirty、objective/profile 覆盖
python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 76 ...
```

## 关键结果

- 当前正式库真实状态已查明：
  - `raw_market.stock_weekly_bar(backward)` 仅覆盖 `5186` 个 code；
  - `stock_file_registry(timeframe='week', adjust_method='backward')` 已登记 `5192` 个 code；
  - `raw_market.stock_monthly_bar(backward) = 0`；
  - `market_base.stock_weekly_adjusted(backward) = 0`；
  - `market_base.stock_monthly_adjusted(backward) = 0`。
- `base_dirty_instrument` 中 `stock + week + backward + pending = 5186`，证明 stock 周线 raw 已挂脏，但 base 仍未物化。
- `H:\tdx_offline_Data` 当前只有 `stock-day / index-day / block-day` 三类日线目录；所有 `*-week / *-month` 目录均不存在，因此旧 week/month 实际是在从 txt 重扫日线。
- 新执行包 `76` 已生成并注册为当前待施工卡，方向冻结为“day 保持官方日更库，week/month 改为独立派生库”。

## 产物

- 设计文档：
  [10-raw-base-day-week-month-ledger-split-charter-20260417.md](/h:/lifespan-0.01/docs/01-design/modules/data/10-raw-base-day-week-month-ledger-split-charter-20260417.md)
- 规格文档：
  [10-raw-base-day-week-month-ledger-split-spec-20260417.md](/h:/lifespan-0.01/docs/02-spec/modules/data/10-raw-base-day-week-month-ledger-split-spec-20260417.md)
- 执行卡：
  [76-raw-base-day-week-month-ledger-split-migration-card-20260417.md](/h:/lifespan-0.01/docs/03-execution/76-raw-base-day-week-month-ledger-split-migration-card-20260417.md)

## 证据结构图

```mermaid
flowchart LR
    CMD[官方库盘点] --> FACT[stock week 半成品 stock month 未开始]
    FACT --> DESIGN[76 设计与规格冻结]
    DESIGN --> CARD[当前待施工卡切到 76]
```
## 2026-04-17 第一刀实现证据

### 命令

```text
pytest tests/unit/core/test_paths.py tests/unit/data/test_timeframe_ledger_bootstrap.py tests/unit/data/test_raw_ingest_runner.py -q
python scripts/system/check_doc_first_gating_governance.py
python scripts/system/check_development_governance.py
```

### 关键结果

- `src/mlq/core/paths.py` 已正式引入六库路径契约：
  - `raw_market_day / raw_market_week / raw_market_month`
  - `market_base_day / market_base_week / market_base_month`
- 旧 day 入口保持兼容：
  - `settings.databases.raw_market` 仍指向 `raw_market_day`
  - `settings.databases.market_base` 仍指向 `market_base_day`
  - `raw_market_ledger_path()` / `market_base_ledger_path()` 仍默认返回 day 库
- `src/mlq/data/bootstrap.py` 已新增 timeframe-aware 路由与 bootstrap：
  - `raw_market_timeframe_ledger_path()` / `market_base_timeframe_ledger_path()`
  - `connect_raw_market_timeframe_ledger()` / `connect_market_base_timeframe_ledger()`
  - `bootstrap_raw_market_timeframe_ledger()` / `bootstrap_market_base_timeframe_ledger()`
- `week/month` 独立库的第一刀表族已经落地：
  - `week raw` 只建 `file_registry + weekly_bar + raw_ingest_*`
  - `month base` 只建 `monthly_adjusted + base_dirty/build_*`
- 验证结果：
  - `pytest ...` => `14 passed`
  - `check_doc_first_gating_governance.py` => `通过`
  - `check_development_governance.py` => `通过`
