# raw/base 日周月分库迁移尾收口 证据

证据编号：`77`
日期：`2026-04-18`

## 命令

```text
python -m pytest tests/unit/data/test_timeframe_split_tail_cleanup.py tests/unit/data/test_timeframe_ledger_bootstrap.py tests/unit/data/test_market_base_timeframe_runner.py tests/unit/data/test_market_base_batched_runner.py -q
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --timeframe week --adjust-method backward --run-mode full --pending-only-from-registry --batch-size 25 --run-id raw-index-week-split-20260418 --summary-path H:/Lifespan-temp/77-split-tail/raw-index-week-summary.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type index --timeframe month --adjust-method backward --run-mode full --pending-only-from-registry --batch-size 25 --run-id raw-index-month-split-20260418 --summary-path H:/Lifespan-temp/77-split-tail/raw-index-month-summary.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --timeframe week --adjust-method backward --run-mode full --pending-only-from-registry --batch-size 32 --run-id raw-block-week-split-20260418 --summary-path H:/Lifespan-temp/77-split-tail/raw-block-week-summary.json
python scripts/data/run_tdx_asset_raw_ingest.py --asset-type block --timeframe month --adjust-method backward --run-mode full --pending-only-from-registry --batch-size 32 --run-id raw-block-month-split-20260418 --summary-path H:/Lifespan-temp/77-split-tail/raw-block-month-summary.json
python scripts/data/run_market_base_build.py --asset-type index --timeframe week --adjust-method backward --build-mode full --batch-size 25 --limit 0 --run-id base-index-week-split-20260418 --summary-path H:/Lifespan-temp/77-split-tail/base-index-week-summary.json
python scripts/data/run_market_base_build.py --asset-type index --timeframe month --adjust-method backward --build-mode full --batch-size 25 --limit 0 --run-id base-index-month-split-20260418 --summary-path H:/Lifespan-temp/77-split-tail/base-index-month-summary.json
python scripts/data/run_market_base_build.py --asset-type block --timeframe week --adjust-method backward --build-mode full --batch-size 32 --limit 0 --run-id base-block-week-split-20260418 --summary-path H:/Lifespan-temp/77-split-tail/base-block-week-summary.json
python scripts/data/run_market_base_build.py --asset-type block --timeframe month --adjust-method backward --build-mode full --batch-size 32 --limit 0 --run-id base-block-month-split-20260418 --summary-path H:/Lifespan-temp/77-split-tail/base-block-month-summary.json
python scripts/data/run_timeframe_split_tail_cleanup.py --summary-path H:/Lifespan-temp/77-split-tail/day-tail-cleanup-dry-run.json
python scripts/data/run_timeframe_split_tail_cleanup.py --execute --summary-path H:/Lifespan-temp/77-split-tail/day-tail-cleanup-executed.json
python scripts/data/run_timeframe_split_tail_cleanup.py --execute --summary-path H:/Lifespan-temp/77-split-tail/day-tail-cleanup-post-bootstrap-fix.json
python inline duckdb 汇总六库 row_count / code_count / date_range、pending scope、day tail state
```

## 关键结果

1. 单测通过：`15 passed`。
2. `index/block week/month raw` 已迁入新库：
   - `index week raw = 79,398 rows / 100 codes`
   - `index month raw = 18,774 rows / 100 codes`
   - `block week raw = 98,719 rows / 127 codes`
   - `block month raw = 23,260 rows / 127 codes`
3. `index/block week/month base` 已迁入新库：
   - `index week base = 79,398 rows / 100 codes`
   - `index month base = 18,774 rows / 100 codes`
   - `block week base = 98,719 rows / 127 codes`
   - `block month base = 23,260 rows / 127 codes`
4. 六库完成度矩阵最终为：
   - `raw day = stock 16,348,113 / index 377,711 / block 468,542`
   - `raw week = stock 3,453,967 / index 79,398 / block 98,719`
   - `raw month = stock 826,336 / index 18,774 / block 23,260`
   - `base day = stock 16,348,113 / index 377,711 / block 468,542`
   - `base week = stock 3,453,967 / index 79,398 / block 98,719`
   - `base month = stock 826,336 / index 18,774 / block 23,260`
5. `resolve_tdx_asset_pending_registry_scope` 最终返回：
   - `index week/month = 100 candidate / 100 existing / 0 pending`
   - `block week/month = 127 candidate / 127 existing / 0 pending`
6. 旧 day 库 tail cleanup 结果：
   - `raw_market.duckdb` 无任何 `*_weekly_bar / *_monthly_bar`
   - `market_base.duckdb` 无任何 `*_weekly_adjusted / *_monthly_adjusted`
   - `raw_ingest_run / raw_ingest_file / *_file_registry / base_*` 只剩 `timeframe='day'`
7. 额外发现并修正：
   - 原 day bootstrap 会在 purge 后重建空 `week/month` 表
   - 已通过 `bootstrap.py` day-only table selection 与 cleanup 单测把该行为封死

## 产物

- `H:\Lifespan-temp\77-split-tail\*.json`：raw/base child summary 与 day tail cleanup summary
- 当前卡片：
  [77-raw-base-timeframe-split-tail-completion-card-20260418.md](/H:/lifespan-0.01/docs/03-execution/77-raw-base-timeframe-split-tail-completion-card-20260418.md)
- 当前结论：
  [77-raw-base-timeframe-split-tail-completion-conclusion-20260418.md](/H:/lifespan-0.01/docs/03-execution/77-raw-base-timeframe-split-tail-completion-conclusion-20260418.md)

## 证据结构图

```mermaid
flowchart LR
    A["slice 1 六库盘点"] --> B["slice 2/3 index/block week/month raw/base 迁移"]
    B --> C["slice 4 六库矩阵与 pending=0 校验"]
    C --> D["slice 5 purge day week/month tail"]
    D --> E["修正 day bootstrap 避免空表回生"]
    E --> F["77 接受并恢复 80-86"]
```
