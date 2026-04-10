# data 日更源头治理封存 证据

证据编号：`22`
日期：`2026-04-11`

## 命令

```text
Get-Content docs/03-execution/18-daily-raw-base-fq-incremental-update-source-selection-conclusion-20260410.md
Get-Content docs/03-execution/19-tdxquant-daily-raw-source-ledger-bridge-conclusion-20260410.md
Get-Content docs/03-execution/20-index-block-raw-base-incremental-bridge-conclusion-20260410.md
Get-Content docs/03-execution/21-system-ledger-incremental-governance-hardening-conclusion-20260410.md

Get-ChildItem H:\tdx_offline_Data -Recurse -File | Sort-Object LastWriteTime -Descending | Select-Object -First 20 FullName,LastWriteTime,Length

$env:PYTHONPATH='src;H:\new_tdx64\PYPlugins\user'
python scripts/data/run_tdxquant_daily_raw_sync.py --strategy-path H:\Lifespan-temp\data\tdxquant\strategy\card19_official_pilot_20260410_002.py --instrument 000001.SZ --instrument 920021.BJ --instrument 510300.SH --no-registry-scope --end-trade-date 2026-04-10 --count 5 --limit 3 --run-id tq-official-20260410-replay-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\tq_official_20260410_replay_001.json

$env:PYTHONPATH='src'
python scripts/data/run_market_base_build.py --asset-type stock --adjust-method none --build-mode incremental --limit 0 --run-id base-stock-none-20260410-replay-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\base_stock_none_20260410_replay_001.json

python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --source-root H:\tdx_offline_Data --adjust-method backward --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-index-backward-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\raw_index_backward_20260410_eod_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --source-root H:\tdx_offline_Data --adjust-method none --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-index-none-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\raw_index_none_20260410_eod_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --source-root H:\tdx_offline_Data --adjust-method forward --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-index-forward-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\raw_index_forward_20260410_eod_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --source-root H:\tdx_offline_Data --adjust-method backward --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-block-backward-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\raw_block_backward_20260410_eod_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --source-root H:\tdx_offline_Data --adjust-method none --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-block-none-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\raw_block_none_20260410_eod_001.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --source-root H:\tdx_offline_Data --adjust-method forward --run-mode incremental --continue-from-last-run --limit 0 --run-id raw-block-forward-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\raw_block_forward_20260410_eod_001.json

python scripts/data/run_market_base_build.py --asset-type index --adjust-method backward --build-mode incremental --limit 0 --run-id base-index-backward-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\base_index_backward_20260410_eod_001.json
python scripts/data/run_market_base_build.py --asset-type index --adjust-method none --build-mode incremental --limit 0 --run-id base-index-none-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\base_index_none_20260410_eod_001.json
python scripts/data/run_market_base_build.py --asset-type index --adjust-method forward --build-mode incremental --limit 0 --run-id base-index-forward-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\base_index_forward_20260410_eod_001.json
python scripts/data/run_market_base_build.py --asset-type block --adjust-method backward --build-mode incremental --limit 0 --run-id base-block-backward-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\base_block_backward_20260410_eod_001.json
python scripts/data/run_market_base_build.py --asset-type block --adjust-method none --build-mode incremental --limit 0 --run-id base-block-none-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\base_block_none_20260410_eod_001.json
python scripts/data/run_market_base_build.py --asset-type block --adjust-method forward --build-mode incremental --limit 0 --run-id base-block-forward-20260410-eod-001 --summary-path H:\Lifespan-temp\data\summary\daily_update\20260410\base_block_forward_20260410_eod_001.json

python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python scripts/system/check_doc_first_gating_governance.py
```

## 关键结果

- 卡 `18/19/20/21` 的正式结论已经形成连续口径：
  - `TdxQuant(none)` 当前只在 `stock` 上形成 official raw bridge
  - `index/block txt -> raw_market -> market_base` 已形成正式主链
  - `forward/backward` 继续留在仓内 `market_base` 物化层
- `H:\tdx_offline_Data` 在 `2026-04-10 22:09` 后存在最新更新文件，满足当前 `index/block` 每日 txt 主链的 operator 前置条件。
- `2026-04-10` 收盘后真实联动更新结果为：
  - `tq-official-20260410-replay-001`：`processed_instrument_count=3`、`reused_bar_count=15`、`dirty_mark_count=0`
  - `base-stock-none-20260410-replay-001`：`source_scope_kind=dirty_queue`、`source_row_count=0`
  - `raw-index-{backward,none,forward}-20260410-eod-001`：每组 `processed_file_count=100`、`bar_rematerialized_count=100`
  - `raw-block-{backward,none,forward}-20260410-eod-001`：每组 `processed_file_count=127`、`bar_rematerialized_count=127`
  - `base-index-{backward,none,forward}-20260410-eod-001`：每组 `consumed_dirty_count=100`
  - `base-block-{backward,none,forward}-20260410-eod-001`：每组 `consumed_dirty_count=127`
- 截至本卡封存时，`stock/index/block` 的 `raw none` 与 `base none` 最大交易日均为 `2026-04-10`，说明当前分工下两条日更 source adapter 都能把账本推进到同一交易日。
- 当前可确认的问题不是历史账本机制分裂，而是 source adapter 仍处于阶段性分工；因此“是否统一”应当被视为后续治理扩展，而不是当前实施缺口。

## 产物

- 每日更新 summary：
  - `H:\Lifespan-temp\data\summary\daily_update\20260410\`
- 正式账本：
  - `H:\Lifespan-data\raw\raw_market.duckdb`
  - `H:\Lifespan-data\base\market_base.duckdb`
- 本卡文档：
  - `docs/03-execution/22-data-daily-source-governance-sealing-card-20260411.md`
  - `docs/03-execution/evidence/22-data-daily-source-governance-sealing-evidence-20260411.md`
  - `docs/03-execution/records/22-data-daily-source-governance-sealing-record-20260411.md`
  - `docs/03-execution/22-data-daily-source-governance-sealing-conclusion-20260411.md`
