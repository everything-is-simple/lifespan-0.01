# raw/base 周月线正式账本扩展记录

记录编号：`75`
日期：`2026-04-16`

## 做了什么

1. 在 `src/mlq/data/data_raw_runner.py` 增加 `resolve_tdx_asset_pending_registry_scope(...)`，统一用正式 `raw_market` registry 计算 source folder 的候选、已覆盖和待补标的集合。
2. 在 `scripts/data/run_tdx_asset_raw_ingest.py` 增加 `--pending-only-from-registry`，把 stock/index/block 的 day/week/month raw 续跑收口到正式 CLI，而不是继续依赖 inline `python -` 或临时脚本。
3. 调整 CLI 行为：启动前输出 `total_codes / existing_codes / pending_codes` 摘要；真实运行结束后把 `pending_only_from_registry` 同步写入 stdout 与 `summary_path`；当 pending 为空时直接写空 summary 并返回。
4. 补充 runner/CLI 单测，并执行 doc-first / development governance 检查。

## 偏离项

- 本次没有把临时续跑脚本直接迁入仓库；改为把同等能力下沉到正式 CLI，避免再出现“仓库内正式入口”和“临时脚本入口”双轨并存。

## 备注

- 该能力的正式使用方式应固定为：

```text
python scripts/data/run_tdx_asset_raw_ingest.py ^
  --asset-type stock ^
  --timeframe week ^
  --adjust-method backward ^
  --run-mode full ^
  --batch-size 50 ^
  --pending-only-from-registry ^
  --summary-path H:/Lifespan-report/raw-base-timeframe-backfill-20260416/raw-stock-week-summary-b050.json ^
  > H:/Lifespan-report/raw-base-timeframe-backfill-20260416/raw-stock-week.stdout.log ^
  2> H:/Lifespan-report/raw-base-timeframe-backfill-20260416/raw-stock-week.stderr.log
```

- 上述口径同样适用于 `index/block` 和 `month`，只需替换 `asset-type/timeframe/summary-path`。

## 记录结构图

```mermaid
flowchart LR
    SCOPE[registry 缺口解析] --> CLI[正式 CLI 续跑]
    CLI --> TEST[单测与治理检查]
    TEST --> CON[结论引用]
```
