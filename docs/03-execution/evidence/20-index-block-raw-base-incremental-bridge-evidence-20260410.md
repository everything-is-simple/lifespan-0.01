# 指数与板块 raw/base 增量桥接证据

证据编号：`20`
日期：`2026-04-10`

## 命令

```text
python scripts/system/check_doc_first_gating_governance.py
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card20_data tests/unit/data/test_data_runner.py -q

python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --source-root H:\tdx_offline_Data --adjust-method backward --run-mode full --limit 0 --run-id raw-index-backward-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_index_backward_card20_full_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --source-root H:\tdx_offline_Data --adjust-method none --run-mode full --limit 0 --run-id raw-index-none-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_index_none_card20_full_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --source-root H:\tdx_offline_Data --adjust-method forward --run-mode full --limit 0 --run-id raw-index-forward-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_index_forward_card20_full_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --source-root H:\tdx_offline_Data --adjust-method backward --run-mode full --limit 0 --run-id raw-block-backward-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_block_backward_card20_full_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --source-root H:\tdx_offline_Data --adjust-method none --run-mode full --limit 0 --run-id raw-block-none-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_block_none_card20_full_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --source-root H:\tdx_offline_Data --adjust-method forward --run-mode full --limit 0 --run-id raw-block-forward-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_block_forward_card20_full_001.json

python scripts/data/run_market_base_build.py --asset-type index --adjust-method backward --build-mode full --limit 0 --run-id base-index-backward-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_index_backward_card20_full_001.json
python scripts/data/run_market_base_build.py --asset-type index --adjust-method none --build-mode full --limit 0 --run-id base-index-none-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_index_none_card20_full_001.json
python scripts/data/run_market_base_build.py --asset-type index --adjust-method forward --build-mode full --limit 0 --run-id base-index-forward-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_index_forward_card20_full_001.json
python scripts/data/run_market_base_build.py --asset-type block --adjust-method backward --build-mode full --limit 0 --run-id base-block-backward-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_block_backward_card20_full_001.json
python scripts/data/run_market_base_build.py --asset-type block --adjust-method none --build-mode full --limit 0 --run-id base-block-none-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_block_none_card20_full_001.json
python scripts/data/run_market_base_build.py --asset-type block --adjust-method forward --build-mode full --limit 0 --run-id base-block-forward-card20-full-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_block_forward_card20_full_001.json

python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --source-root H:\tdx_offline_Data --adjust-method backward --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-index-backward-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_index_backward_card20_incremental_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --source-root H:\tdx_offline_Data --adjust-method none --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-index-none-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_index_none_card20_incremental_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --source-root H:\tdx_offline_Data --adjust-method forward --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-index-forward-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_index_forward_card20_incremental_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --source-root H:\tdx_offline_Data --adjust-method backward --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-block-backward-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_block_backward_card20_incremental_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --source-root H:\tdx_offline_Data --adjust-method none --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-block-none-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_block_none_card20_incremental_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --source-root H:\tdx_offline_Data --adjust-method forward --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-block-forward-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\raw_block_forward_card20_incremental_001.json

python scripts/data/run_market_base_build.py --asset-type index --adjust-method backward --build-mode incremental --limit 0 --run-id base-index-backward-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_index_backward_card20_incremental_001.json
python scripts/data/run_market_base_build.py --asset-type index --adjust-method none --build-mode incremental --limit 0 --run-id base-index-none-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_index_none_card20_incremental_001.json
python scripts/data/run_market_base_build.py --asset-type index --adjust-method forward --build-mode incremental --limit 0 --run-id base-index-forward-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_index_forward_card20_incremental_001.json
python scripts/data/run_market_base_build.py --asset-type block --adjust-method backward --build-mode incremental --limit 0 --run-id base-block-backward-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_block_backward_card20_incremental_001.json
python scripts/data/run_market_base_build.py --asset-type block --adjust-method none --build-mode incremental --limit 0 --run-id base-block-none-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_block_none_card20_incremental_001.json
python scripts/data/run_market_base_build.py --asset-type block --adjust-method forward --build-mode incremental --limit 0 --run-id base-block-forward-card20-incremental-001 --summary-path H:\Lifespan-temp\data\summary\card20\base_block_forward_card20_incremental_001.json
```

## 关键结果

- 治理检查通过：
  - `check_doc_first_gating_governance.py`
  - `check_execution_indexes.py --include-untracked`
- 单测通过：
  - `tests/unit/data/test_data_runner.py`
  - 结果：`13 passed`
- full raw 初始化成功：
  - `index`
    - 三套复权各 `100` 文件
    - 每套 `377711` 行
  - `block`
    - 三套复权各 `127` 文件
    - 每套 `468542` 行
- full base 物化成功：
  - `index_daily_adjusted`
    - `backward / none / forward` 各 `377711` 行
  - `block_daily_adjusted`
    - `backward / none / forward` 各 `468542` 行
- incremental replay 成功：
  - raw replay
    - `index` 三套分别 `skipped_unchanged_file_count = 100`
    - `block` 三套分别 `skipped_unchanged_file_count = 127`
  - base replay
    - 六组均为 `source_scope_kind = dirty_queue`
    - 六组均为 `source_row_count = 0`
    - 六组均为 `consumed_dirty_count = 0`
- 落表摘要：
  - `raw_market.index_daily_bar`
    - `backward / forward / none` 各 `377711` 行
  - `raw_market.block_daily_bar`
    - `backward / forward / none` 各 `468542` 行
  - `market_base.index_daily_adjusted`
    - `backward / forward / none` 各 `377711` 行
  - `market_base.block_daily_adjusted`
    - `backward / forward / none` 各 `468542` 行
  - `base_dirty_instrument`
    - `index` 三套各 `100` 行，状态全部 `consumed`
    - `block` 三套各 `127` 行，状态全部 `consumed`

## 产物

- summary JSON：
  - `H:\Lifespan-temp\data\summary\card20\raw_*_card20_full_001.json`
  - `H:\Lifespan-temp\data\summary\card20\base_*_card20_full_001.json`
  - `H:\Lifespan-temp\data\summary\card20\raw_*_card20_incremental_001.json`
  - `H:\Lifespan-temp\data\summary\card20\base_*_card20_incremental_001.json`
- 正式账本：
  - `H:\Lifespan-data\raw\raw_market.duckdb`
  - `H:\Lifespan-data\base\market_base.duckdb`
