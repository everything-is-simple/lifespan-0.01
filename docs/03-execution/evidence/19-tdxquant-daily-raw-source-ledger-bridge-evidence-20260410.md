# TdxQuant 日更原始事实接入 raw/base 账本桥接 证据

证据编号：`19`
日期：`2026-04-10`

## 命令

```text
python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 19 --slug tdxquant-daily-raw-source-ledger-bridge --title "TdxQuant 日更原始事实接入 raw/base 账本桥接" --date 20260410 --register --set-current-card
Get-Content docs/03-execution/18-daily-raw-base-fq-incremental-update-source-selection-conclusion-20260410.md
Get-Content docs/01-design/modules/data/03-daily-raw-base-fq-incremental-update-source-selection-charter-20260410.md
Get-Content docs/02-spec/modules/data/03-daily-raw-base-fq-incremental-update-source-selection-spec-20260410.md
python scripts/system/check_doc_first_gating_governance.py
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python -m pytest -p no:cacheprovider --basetemp H:\Lifespan-temp\pytest\card19_data tests/unit/data/test_data_runner.py -q
```

## 关键结果

- 卡 `19` 四件套已落盘；自动回填索引时，仍被旧版索引标题名不一致卡住。
- 已手动补出 `data/04` 号 design/spec，并把卡 `19` 的问题、任务与边界写成正式口径。
- 已手动把 `00-conclusion-catalog / 00-evidence-catalog / B-card-catalog / C-system-completion-ledger / A-execution-reading-order` 切到卡 `19`。
- 卡 `19` 当前冻结的实现方向是：
  - 接受：`TdxQuant` 作为日更原始事实源头进入 `raw_market`
  - 拒绝：直接把 `TdxQuant(front/back)` 当作正式 `raw_forward / raw_backward`
  - 保留：现有 `txt -> raw_market -> market_base` 继续作为正式 fallback
- `check_doc_first_gating_governance.py` 与 `check_execution_indexes.py --include-untracked` 已在卡 `19` 开卡后重新通过。
- 已落地卡 `19` 切片 2 的最小正式实现：
  - `raw_market` 新增 `raw_tdxquant_run / raw_tdxquant_request / raw_tdxquant_instrument_checkpoint`
  - 新增 `run_tdxquant_daily_raw_sync(...)` 与 `scripts/data/run_tdxquant_daily_raw_sync.py`
  - `TdxQuant(dividend_type='none')` 已可按 request/checkpoint 账本语义桥接到 `stock_daily_bar(adjust_method='none')`
  - 变更后只标记 `base_dirty_instrument(adjust_method='none')`，不伪造 `forward/backward` 已更新
- 已补两组卡 `19` 单测：
  - registry scope + onboarding union + checkpoint skip unchanged
  - failed request / failed run 审计落表
- `tests/unit/data/test_data_runner.py` 已通过：`8 passed`
- 本轮仍未形成真实 `TdxQuant` official pilot，因此卡 `19` 结论仍保持草稿态。

## 产物

- `docs/01-design/modules/data/04-tdxquant-daily-raw-source-ledger-bridge-charter-20260410.md`
- `docs/02-spec/modules/data/04-tdxquant-daily-raw-source-ledger-bridge-spec-20260410.md`
- `docs/03-execution/19-tdxquant-daily-raw-source-ledger-bridge-card-20260410.md`
- `docs/03-execution/evidence/19-tdxquant-daily-raw-source-ledger-bridge-evidence-20260410.md`
- `docs/03-execution/records/19-tdxquant-daily-raw-source-ledger-bridge-record-20260410.md`
- `docs/03-execution/19-tdxquant-daily-raw-source-ledger-bridge-conclusion-20260410.md`
- `src/mlq/data/bootstrap.py`
- `src/mlq/data/runner.py`
- `src/mlq/data/tdxquant.py`
- `src/mlq/data/__init__.py`
- `scripts/data/run_tdxquant_daily_raw_sync.py`
- `tests/unit/data/test_data_runner.py`
- `AGENTS.md`
- `README.md`
- `pyproject.toml`
