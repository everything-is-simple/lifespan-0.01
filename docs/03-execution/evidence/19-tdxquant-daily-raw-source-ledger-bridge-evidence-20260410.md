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
$env:PYTHONPATH='src;H:\new_tdx64\PYPlugins\user'; python scripts/data/run_tdxquant_daily_raw_sync.py --strategy-path H:\Lifespan-temp\data\tdxquant\strategy\card19_official_pilot_20260410_001.py --instrument 000001.SZ --instrument 920021.BJ --instrument 510300.SH --no-registry-scope --end-trade-date 2026-04-10 --count 5 --limit 3 --run-id tq-official-card19-001 --summary-path H:\Lifespan-temp\data\summary\card19_tq_sync_20260410_001.json
$env:PYTHONPATH='src;H:\new_tdx64\PYPlugins\user'; python scripts/data/run_tdxquant_daily_raw_sync.py --strategy-path H:\Lifespan-temp\data\tdxquant\strategy\card19_official_pilot_20260410_002.py --instrument 000001.SZ --instrument 920021.BJ --instrument 510300.SH --no-registry-scope --end-trade-date 2026-04-10 --count 5 --limit 3 --run-id tq-official-card19-002 --summary-path H:\Lifespan-temp\data\summary\card19_tq_sync_20260410_002.json
$env:PYTHONPATH='src'; python scripts/data/run_market_base_build.py --adjust-method none --build-mode incremental --run-id base-card19-none-001 --summary-path H:\Lifespan-temp\data\summary\card19_market_base_none_001.json
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
- 已补真实 `TdxQuant` bounded official pilot：
  - `tq-official-card19-001` 在真实 `TdxW.exe` 运行环境下成功完成 `3` 个 onboarding 请求
  - `000001.SZ / 920021.BJ / 510300.SH` 共返回 `15` 行 `none` 原始事实
  - 首轮 run 结果为 `inserted_bar_count=5 / rematerialized_bar_count=10 / dirty_mark_count=3`
  - 第二轮 replay `tq-official-card19-002` 在相同窗口下得到 `reused_bar_count=15 / dirty_mark_count=0`，`request` 状态全部为 `skipped_unchanged`
  - `raw_tdxquant_instrument_checkpoint` 已把三只标的的 `last_success_run_id` 推进到 `tq-official-card19-002`
- 已补 `raw -> base` 真实联动：
  - `base-card19-none-001` 成功以 `dirty_queue` 模式消费 `3` 个 `adjust_method='none'` 脏标的
  - 本轮 official pilot 同时暴露旧逻辑中 `dirty_queue` 仍被全局 `limit=1000` 截断的问题
  - 该问题已在本轮修复为“dirty_queue stage 不再受全局 row limit 截断”，并补回归单测
- `tests/unit/data/test_data_runner.py` 已通过：`9 passed`
- 本轮已经形成真实 `TdxQuant` official pilot；但卡 `19` 的结论文件当前仍保持草稿态，尚未改写为正式生效口径。

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
