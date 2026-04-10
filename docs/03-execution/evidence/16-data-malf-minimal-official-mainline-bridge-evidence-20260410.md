# data/malf 最小官方主线桥接证据

证据编号：`16`
日期：`2026-04-10`

## 命令

```text
python -m compileall src/mlq/data src/mlq/malf scripts/data scripts/malf
pytest tests/unit/data/test_data_runner.py tests/unit/malf/test_malf_runner.py -q
pytest tests/unit/position/test_position_runner.py -q
pytest tests/unit/trade/test_trade_runner.py -q
python scripts/system/check_doc_first_gating_governance.py
python scripts/system/check_development_governance.py
```

## 关键结果

1. `raw_market` 官方 ingest 已用真实 `H:\tdx_offline_Data` 跑出三套价格口径：
   - `raw-backward-001.json`：`candidate_file_count=2`，`ingested_file_count=2`，`bar_inserted_count=14628`
   - `raw-none-001.json`：`candidate_file_count=2`，`ingested_file_count=2`，`bar_inserted_count=14628`
   - `raw-forward-001.json`：`candidate_file_count=2`，`ingested_file_count=1`，`skipped_unchanged_file_count=1`，`bar_inserted_count=6745`，`bar_reused_count=1601`
2. `market_base.stock_daily_adjusted` 已从官方 `raw_market` 物化三套价格：
   - `base-backward-001.json`：`source_row_count=14628`，`inserted_count=14627`，`rematerialized_count=1`
   - `base-none-001.json`：`source_row_count=14628`，`inserted_count=14628`
   - `base-forward-001.json`：`source_row_count=14628`，`inserted_count=14628`
3. `malf` 已从官方 `market_base(backward)` 物化最小语义快照：
   - `malf-001.json`：`bounded_instrument_count=2`，`context_snapshot_count=8`，`structure_candidate_count=8`，`context_inserted_count=7`，`context_rematerialized_count=1`，`structure_inserted_count=7`，`structure_rematerialized_count=1`
   - `malf-002.json`：`context_reused_count=8`，`structure_reused_count=8`
4. 现有 `structure` runner 已能消费新生成的官方 `malf` 上游：
   - `structure-001.json`：`candidate_input_count=8`，`materialized_snapshot_count=8`，`inserted_count=8`
   - `structure-002.json`：`reused_count=8`
5. 单元测试已覆盖：
   - `TDX -> raw_market -> market_base` 增量跳过、变更重物化、多复权口径
   - `market_base -> malf -> structure` 最小官方桥接
   - `position` 与 `trade` 执行参考价默认口径切换到 `none`
6. `python scripts/system/check_development_governance.py` 仍会报告仓内历史债务，包括既有超长文件与既有中文化缺口；本轮未新增新的入口 freshness 或 doc-first 问题。

## 产物

1. `H:\Lifespan-report\data\card16\raw-backward-001.json`
2. `H:\Lifespan-report\data\card16\raw-none-001.json`
3. `H:\Lifespan-report\data\card16\raw-forward-001.json`
4. `H:\Lifespan-report\data\card16\base-backward-001.json`
5. `H:\Lifespan-report\data\card16\base-none-001.json`
6. `H:\Lifespan-report\data\card16\base-forward-001.json`
7. `H:\Lifespan-report\data\card16\malf-001.json`
8. `H:\Lifespan-report\data\card16\malf-002.json`
9. `H:\Lifespan-report\data\card16\structure-001.json`
10. `H:\Lifespan-report\data\card16\structure-002.json`
