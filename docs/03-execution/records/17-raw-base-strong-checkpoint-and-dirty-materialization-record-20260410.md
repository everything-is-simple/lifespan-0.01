# raw/base 强断点与脏标的物化增强 记录

记录编号：`17`
日期：`2026-04-10`

## 做了什么

1. 在 `market_base` 正式账本中新增 `base_dirty_instrument / base_build_run / base_build_scope / base_build_action` 四张增强表，补齐卡 17 切片 1 需要的最小 `base` 运行账本。
2. 增强 `run_market_base_build(...)`，新增 `build_mode / consume_dirty_only / mark_clean_on_success` 参数，并让 `incremental` 模式默认只消费 `base_dirty_instrument.pending`。
3. 新增 `mark_base_instrument_dirty(...)`，作为当前切片内的最小脏标的写入口，先为后续 raw 侧联动保留正式接口。
4. 增强 `scripts/data/run_market_base_build.py`，补齐 `full / incremental` 双模式 CLI 入口。
5. 在 `raw_market` 正式账本中新增 `raw_ingest_run / raw_ingest_file` 两张增强表，补齐卡 17 切片 2 需要的最小 `raw` 运行账本。
6. 增强 `run_tdx_stock_raw_ingest(...)`，改成“run 行常驻 + 文件级事务 + 每文件显式动作记录”的执行模型。
7. 增强 `scripts/data/run_tdx_stock_raw_ingest.py`，补齐 `run_mode` CLI 参数。
8. 让 `run_tdx_stock_raw_ingest(...)` 在成功写入 `raw_market` 后自动 upsert `base_dirty_instrument`，以 `(code, adjust_method)` 为最小脏标的粒度建立 `raw -> base` 正式联动。
9. 为 `run_tdx_stock_raw_ingest(...)` 新增 `force_hash / continue_from_last_run`：
   - `force_hash` 可在显式指定时绕过 `size + mtime` 快路径，强制做内容哈希校验
   - `continue_from_last_run` 会读取最近一次失败的 `raw_ingest_run`，跳过该 run 中已完成文件，只续跑剩余文件
10. 在 `bootstrap_raw_market_ledger(...) / bootstrap_market_base_ledger(...)` 中加入历史脏数据清理、唯一索引与 `NOT NULL` 升级：
   - `stock_file_registry.file_nk`
   - `stock_daily_bar.bar_nk`
   - `stock_daily_adjusted(code, trade_date, adjust_method)`
   - `base_dirty_instrument.dirty_nk`
11. 调整 raw/base 物化路径，使其与唯一约束兼容：
   - raw 改为“删除已消失旧 bar + upsert 当前文件 bar”
   - base 改为 `MERGE` 物化，并在 `full` 模式下清理 source 中已不存在的旧键
12. 补充 `tests/unit/data/test_data_runner.py`，验证：
   - 卡 16 的既有 `full` 物化行为仍成立
   - `incremental` 模式只消费 dirty queue
   - 成功后 `base_build_run / base_build_scope / base_build_action / base_dirty_instrument` 均留下正式记录
   - 成功与失败两种 `raw` run 都会留下 `raw_ingest_run / raw_ingest_file` 审计记录
   - `force_hash / continue_from_last_run / raw -> dirty queue` 自动联动成立
   - bootstrap 清理重复/脏行并成功补齐唯一约束

## 偏离项

- `continue_from_last_run` 当前按“最近一次失败 run + 同 `adjust_method` + 同 `source_root`”恢复，不额外引入更复杂的多 run 合并策略。
- `raw -> dirty queue` 目前只在 `inserted / rematerialized` 时标脏；`reused / skipped_unchanged` 不会重新标脏。
- 为释放因超时中断遗留的 `market_base.duckdb.wal`，先把 `market_base.duckdb` 与 `.wal` 备份到 `H:\Lifespan-temp\recovery\card17-market-base-20260410162325`，再移走孤儿 WAL；修复后正式库可正常重开。

## 备注

- official bounded pilot evidence 已补齐：正式库使用 `H:\tdx_offline_Data / H:\Lifespan-data`，受控 replay 使用 `H:\Lifespan-temp\card17-controlled`。
- 当前最新已生效结论锚点已推进到 `17`。
- 执行索引已同步回填，且 `check_execution_indexes.py --include-untracked` 与 `check_doc_first_gating_governance.py` 在收口后重新通过。
- 当前 `raw` 失败文件会被显式记录为 `failed`，并把 run 状态更新为 `failed`；后续续跑已可直接引用这一锚点。
- 当前 `full` 模式会自动清理其覆盖范围内的 pending dirty，避免老 dirty 泄漏到后续 `incremental` 运行。
