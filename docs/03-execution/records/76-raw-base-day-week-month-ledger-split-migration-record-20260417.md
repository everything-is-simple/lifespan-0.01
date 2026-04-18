# raw/base 日周月分库迁移记录

记录编号：`76`
日期：`2026-04-17`

## 做了什么

1. 重新盘点 `data` 模块正式入口、`H:\tdx_offline_Data` 源目录和官方 `raw/base` 两个库的真实落表状态。
2. 确认当前阻塞不是“week/month 表没有建”，而是 `stock week` 在旧单库方案下只做成了半成品，`stock month` 完全未落。
3. 基于真实库事实，新建 `76` 执行包，并补 `10-*` 设计/规格，把 `raw/base` 迁移方向冻结为日周月分库。
4. 把当前待施工卡从 `90` 暂时切到 `76`，明确 `78-84` 需等待 `76` 收口后再恢复。

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

## 2026-04-17 第二刀追加记录

1. 新增 `src/mlq/data/ledger_timeframe.py`，把 raw/base 日周月官方库的路径与连接 helper 独立出来。
2. `data_shared.py` 改为通过该 helper 解析 `raw_market` / `market_base` 的 `day/week/month` 物理库路径。
3. `data_raw_runner.py` 已切到 timeframe-aware 路由：
   `week/month raw` 现在直接连接对应 `raw_market_week/month.duckdb`，并把 dirty 挂到对应 `market_base_week/month.duckdb`。
4. `data_market_base_runner.py` 已切到 timeframe-aware 路由：
   `week/month base` 现在从对应 `raw_market_week/month.duckdb` 读取，并写入 `market_base_week/month.duckdb`。
5. 扩展 `tests/unit/data/test_timeframe_ledger_bootstrap.py`，补 raw/base runner 新库落点回归。

## 仍待后续

- `week/month raw` 当前仍允许从 day txt fallback 聚合；真正的 `day raw -> week/month raw` 派生 runner 还没做。
- `bootstrap.py` 仍处在 `1000` 行硬上限边缘；helper 已拆出，但 bootstrap 自身 schema 切片还没继续下刀。
## 2026-04-17 第三刀追加记录

1. 在 `src/mlq/data/data_raw_runner.py` 中新增 `day raw` 派生 helper，把 `week/month raw` 的正式 source 改成 `raw_market_day.duckdb`。
2. `run_tdx_asset_raw_ingest()` 现在按 timeframe 分流：
   `day` 继续读离线 txt；`week/month` 直接走 `day raw -> 聚合 -> week/month raw`。
3. `resolve_tdx_asset_pending_registry_scope()` 与 `run_tdx_asset_raw_ingest_batched()` 已同步改成从 `day raw` 盘点 candidate/pending code，不再依赖 `week/month txt` 目录。
4. 测试口径同步更新：
   - `tests/unit/data/test_timeframe_ledger_bootstrap.py`
   - `tests/unit/data/test_raw_ingest_runner.py`
   - `tests/unit/data/test_market_base_timeframe_runner.py`
   现在都显式验证“先建 day raw，再派生 week/month raw”的正式链路。
5. 串行验证已跑：
   - `pytest tests/unit/core/test_paths.py tests/unit/data/test_timeframe_ledger_bootstrap.py tests/unit/data/test_raw_ingest_runner.py tests/unit/data/test_market_base_timeframe_runner.py -q`
   - `python scripts/system/check_doc_first_gating_governance.py`
   - `python scripts/system/check_development_governance.py`
   结果均通过。

## 更新后的仍待后续

- 真实官方库的 `stock week/month` rebuild 还没开始。
- rebuild 后的 parity 校验还没开始。
- 旧 `day` 官方库中的周月表和周月数据清理还没开始。
- `bootstrap.py` 的进一步切片仍待后续治理卡或同卡后续刀次继续处理。

## 2026-04-18 第四刀追加记录

1. �?`src/mlq/data/data_market_base_governance.py` �?dirty queue 消费改成启用 `dirty_nk + asset_type + timeframe + code + adjust_method` 的自然键兼容更新，并返回真实 `updated_count`�?
2. �?`src/mlq/data/data_market_base_runner.py` �?dirty queue 清账改成“计数不一致就立即失败”，避免出现 summary 报了 `consumed_dirty_count`，但库内仍然留有 `pending` 的假成功�?
3. 修复�?`src/mlq/data/data_shared.py` �?`mark_base_instrument_dirty()` �?timeframe 初始化顺序，避免 `normalized_timeframe` 在赋值前被使用�?
4. 新增 `tests/unit/data/test_market_base_dirty_consumption.py`，把“stale dirty_nk 仍能按自然键清账”和“dirty 消费计数不一致必须失败”两条行为锁住�?
5. 运行�?   - `python -m pytest tests/unit/data/test_market_base_runner.py tests/unit/data/test_market_base_timeframe_runner.py tests/unit/data/test_market_base_dirty_consumption.py -q`
   - `python scripts/system/check_doc_first_gating_governance.py`
   - `python scripts/system/check_development_governance.py`
   - `python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked`
   结果全部通过�?
6. 真实库收口方面，先把 `base-stock-month-split-resume2-20260417c-b0001` 以及 2026-04-18 为了清理尾巴生成的两�?cleanup/debug run audit 补记�?`failed`�?
7. 再将 `market_base_month.duckdb` �?`688126.SH` ~ `688136.SH` �?`10` �?pending dirty` 回填�?`consumed`，并�?`last_consumed_run_id` 回填到已经实际完成这�?10 �?code reuse 物化�?`base-stock-month-dirty-finalize-20260417e`�?
8. 最终真实库盘点结果是：`stock backward day/week/month raw/base` 数据全部对齐，`base week dirty = consumed 5501`，`base month dirty = consumed 5501`�?
