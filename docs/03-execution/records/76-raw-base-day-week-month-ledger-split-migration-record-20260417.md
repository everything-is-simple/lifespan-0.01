# raw/base 日周月分库迁移记录

记录编号：`76`
日期：`2026-04-17`

## 做了什么

1. 重新盘点 `data` 模块正式入口、`H:\tdx_offline_Data` 源目录和官方 `raw/base` 两个库的真实落表状态。
2. 确认当前阻塞不是“week/month 表没有建”，而是 `stock week` 在旧单库方案下只做成了半成品，`stock month` 完全未落。
3. 基于真实库事实，新建 `76` 执行包，并补 `10-*` 设计/规格，把 `raw/base` 迁移方向冻结为日周月分库。
4. 把当前待施工卡从 `80` 暂时切到 `76`，明确 `80-86` 需等待 `76` 收口后再恢复。

## 偏离项

- 本次没有继续推进旧单库 week/month 回补，也没有尝试在现有 `raw_market.duckdb / market_base.duckdb` 上做“局部修复”。原因是这条路径已经被真实库事实证明不具备长期可运行性。

## 备注

- `76` 的核心不是“把旧周月跑完”，而是改掉导致旧周月跑不完的库形态和派生路径。
- 现有 `raw_market.duckdb / market_base.duckdb` 将被保留为 day 官方库；周月新库建成并验收后，旧 day 库中的周月表才允许删除。

## 记录结构图

```mermaid
flowchart LR
    CHECK[盘点官方库现状] --> DECIDE[冻结旧单库周月路径]
    DECIDE --> DOC[新建设计 规格 卡片]
    DOC --> NEXT[等待迁移实施]
```
## 2026-04-17 第一刀实现追加记录

1. 在 `src/mlq/core/paths.py` 中把 `raw/base` 官方库路径正式拆成六个显式字段：
   `raw_market_day / week / month` 与 `market_base_day / week / month`。
2. 保留旧 day 别名，确保现有调用方仍可继续使用：
   `settings.databases.raw_market` 与 `settings.databases.market_base`。
3. 在 `src/mlq/data/bootstrap.py` 中新增 timeframe-aware path / connect / bootstrap 入口，并把 `week/month` 的 bootstrap 表族先按物理库边界收敛到最小集合。
4. 在 `src/mlq/data/__init__.py` 中对外导出新入口，供后续 runner 逐步切换。
5. 新增 `tests/unit/data/test_timeframe_ledger_bootstrap.py`，并更新 `tests/unit/core/test_paths.py`，锁住六库路径契约与 `day` 兼容别名。
6. 运行：
   - `pytest tests/unit/core/test_paths.py tests/unit/data/test_timeframe_ledger_bootstrap.py tests/unit/data/test_raw_ingest_runner.py -q`
   - `python scripts/system/check_doc_first_gating_governance.py`
   - `python scripts/system/check_development_governance.py`
   结果全部通过。

## 当前未做

- 还没有把 `run_tdx_asset_raw_ingest.py` / `run_market_base_build.py` 等 runner 切到新 `week/month` 官方库。
- 还没有实现 `day raw -> week/month raw` 的正式派生 materialization runner。
- 还没有开始真实官方库的 `stock week/month` rebuild 与 parity 校验。
