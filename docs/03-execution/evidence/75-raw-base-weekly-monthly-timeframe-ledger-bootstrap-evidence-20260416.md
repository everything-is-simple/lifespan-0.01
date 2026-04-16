# raw/base 周月线正式账本扩展证据

证据编号：`75`
日期：`2026-04-16`

## 命令

```text
pytest tests/unit/data/test_raw_ingest_runner.py tests/unit/data/test_raw_ingest_cli_entrypoint.py -q
python scripts/system/check_doc_first_gating_governance.py
python scripts/system/check_development_governance.py
```

## 关键结果

- `tests/unit/data/test_raw_ingest_runner.py` 新增 `resolve_tdx_asset_pending_registry_scope` 覆盖，确认正式 runner 可按 `raw_market.{asset}_file_registry` 解析 source folder 相对官方账本的缺口标的集合。
- `tests/unit/data/test_raw_ingest_cli_entrypoint.py` 新增 CLI 入口测试，确认 `scripts/data/run_tdx_asset_raw_ingest.py --pending-only-from-registry` 会：
  - 先打印 `total_codes / existing_codes / pending_codes` 启动摘要；
  - 仅把 `pending_instruments` 透传给 batched raw runner；
  - 当 `pending_codes = 0` 时直接写零工作量 summary，不误触发全量扫描。
- `check_doc_first_gating_governance.py` 与 `check_development_governance.py` 通过；development governance 仅保留既有历史债务盘点，无新增违规。

## 产物

- 正式 CLI：`scripts/data/run_tdx_asset_raw_ingest.py`
- 正式 runner helper：`src/mlq/data/data_raw_runner.py`
- 单测：
  - `tests/unit/data/test_raw_ingest_runner.py`
  - `tests/unit/data/test_raw_ingest_cli_entrypoint.py`

## 证据结构图

```mermaid
flowchart LR
    CMD[正式命令验证] --> TEST[runner/CLI 单测通过]
    TEST --> GOV[治理检查通过]
    GOV --> CLI[官方续跑入口冻结]
```
