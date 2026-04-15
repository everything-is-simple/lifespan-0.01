# 历史 objective profile 回补执行 证据

`证据编号：72`
`日期：2026-04-15`

## 命令

```powershell
python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 72 --slug historical-objective-profile-backfill-execution --title "历史 objective profile 回补执行" --date 20260415 --status 草稿 --register --set-current-card
python scripts/system/check_development_governance.py
python scripts/system/check_doc_first_gating_governance.py
python scripts/filter/run_filter_objective_coverage_audit.py --signal-start-date 2010-01-04 --signal-end-date 2026-04-08 --summary-path H:\Lifespan-temp\filter\coverage-audit-20100104-20260408-summary.json --report-path H:\Lifespan-report\filter\coverage-audit-20100104-20260408-report.md
@'
from mlq.data.tushare import open_tushare_client
try:
    client = open_tushare_client()
except Exception as exc:
    print(type(exc).__name__)
    print(str(exc))
else:
    print("OK")
    client.close()
'@ | python -
$env:TUSHARE_TOKEN='***'; python scripts/data/run_tushare_objective_source_sync.py --signal-start-date 2010-01-04 --signal-end-date 2010-12-31 --run-id 72-backfill-source-2010a --summary-path H:\Lifespan-temp\data\72-backfill-source-2010a-summary.json
python scripts/data/run_tushare_objective_profile_materialization.py --signal-start-date 2010-01-04 --signal-end-date 2010-12-31 --run-id 72-backfill-profile-2010a --summary-path H:\Lifespan-temp\data\72-backfill-profile-2010a-summary.json
python -m pytest tests/unit/data/test_tushare_objective_runner.py -q
python scripts/data/run_tushare_objective_profile_materialization.py --signal-start-date 2010-01-04 --signal-end-date 2010-12-31 --run-id 72-backfill-profile-2010b --summary-path H:\Lifespan-temp\data\72-backfill-profile-2010b-summary.json
$codes = @('600790.SH','600791.SH','600792.SH','600793.SH','600794.SH','600795.SH','600796.SH','600797.SH','600798.SH','600800.SH','600801.SH','600802.SH','600803.SH','600805.SH','600807.SH','600808.SH','600809.SH','600810.SH','600812.SH','600814.SH','600815.SH','600816.SH','600817.SH','600818.SH','600819.SH','600820.SH','600821.SH','600822.SH','600824.SH','600825.SH','600826.SH','600827.SH','600828.SH','600829.SH','600830.SH','600831.SH','600833.SH','600834.SH','600835.SH','600838.SH','600839.SH','600841.SH','600843.SH','600844.SH','600845.SH','600846.SH','600847.SH','600848.SH','600850.SH','600851.SH','600853.SH','600854.SH','600855.SH','600857.SH','600858.SH','600859.SH','600860.SH','600861.SH','600862.SH','600863.SH','600864.SH','600865.SH','600866.SH','600867.SH','600868.SH','600869.SH','600871.SH','600872.SH','600873.SH','600874.SH','600875.SH','600876.SH','600877.SH','600879.SH','600880.SH','600881.SH','600882.SH','600883.SH','600884.SH','600885.SH','600886.SH','600887.SH','600888.SH','600889.SH','600892.SH','600893.SH','600894.SH','600895.SH','600897.SH','600900.SH','600960.SH','600961.SH','600962.SH','600963.SH','600965.SH','600966.SH','600967.SH','600969.SH','600970.SH','600971.SH','600973.SH','600975.SH','600976.SH','600979.SH','600980.SH','600981.SH','600982.SH','600983.SH','600984.SH','600985.SH','600986.SH','600987.SH','600988.SH','600990.SH','600992.SH','600993.SH','600995.SH','600997.SH','600998.SH','600999.SH','601000.SH','601001.SH','601002.SH','601003.SH','601005.SH','601006.SH','601007.SH','601008.SH','601009.SH','601018.SH','601088.SH','601098.SH','601099.SH','601101.SH','601106.SH','601107.SH','601111.SH','601117.SH','601126.SH','601139.SH','601158.SH','601166.SH','601168.SH','601169.SH','601177.SH','601179.SH','601186.SH','601188.SH','601288.SH','601318.SH','601328.SH','601333.SH','601369.SH','601377.SH','601390.SH','601398.SH','601518.SH','601588.SH','601600.SH','601601.SH','601607.SH','601618.SH','601628.SH','601666.SH','601668.SH','601678.SH','601688.SH','601699.SH','601717.SH','601718.SH','601727.SH','601766.SH','601777.SH','601788.SH','601801.SH','601808.SH','601818.SH','601857.SH','601866.SH','601872.SH','601877.SH','601880.SH','601888.SH','601890.SH','601898.SH','601899.SH','601918.SH','601919.SH','601933.SH','601939.SH','601958.SH','601988.SH','601991.SH','601998.SH','601999.SH'); $args = @('scripts/data/run_tushare_objective_profile_materialization.py','--signal-start-date','2010-01-04','--signal-end-date','2010-12-31','--run-id','72-backfill-profile-2010c','--summary-path','H:\Lifespan-temp\data\72-backfill-profile-2010c-summary.json'); foreach ($code in $codes) { $args += @('--instrument', $code) }; python @args
python scripts/filter/run_filter_objective_coverage_audit.py --signal-start-date 2010-01-04 --signal-end-date 2026-04-08 --summary-path H:\Lifespan-temp\filter\coverage-audit-20100104-20260408-after-72-2010a-summary.json --report-path H:\Lifespan-report\filter\coverage-audit-20100104-20260408-after-72-2010a-report.md
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
```

## 关键结果

- 已正式新开 `72-historical-objective-profile-backfill-execution-card-20260415.md`。
- 当前待施工卡已从 `71` 切换到 `72`。
- `72` 卡面已脱离模板态，明确：
  - 全窗口回补范围是 `2010-01-04 -> 2026-04-08`
  - 本卡职责是“历史回补执行”，而不是继续在 `71` 内做更大 bounded smoke
  - 每个批次必须走 `source sync -> profile materialization -> coverage audit`
- `check_development_governance.py` 通过。
- `check_doc_first_gating_governance.py` 通过。
- `2026-04-15` 对正式库执行 full-window coverage audit：
  - `filter_snapshot_count = 6835`
  - `covered_objective_count = 2`
  - `missing_objective_count = 6833`
  - `missing_ratio = 0.9997073884418435`
  - `suggested_backfill_start_date = 2010-01-04`
  - `suggested_backfill_end_date = 2010-12-31`
- baseline readout 显示首批建议窗口 `2010-01-04 -> 2010-12-31` 对应：
  - `1833` 个 distinct `code`
  - `242` 个 distinct `trade_date`
- 当前 shell 直接探测 `open_tushare_client()` 失败，阻塞信息固定为：
  - `ValueError`
  - `Tushare token is required; pass token= or set environment variable TUSHARE_TOKEN.`
- 用户随后提供正式 token 来源，本卡按首批窗口 `2010-01-04 -> 2010-12-31` 完成正式执行：
  - `72-backfill-source-2010a`：`2326` 个 cursor 全部完成，`inserted_event_count = 16497`，`failed_request_count = 0`
  - `72-backfill-profile-2010a`：首轮整窗 materialization 因外层命令超时被中断，形成已提交 partial profile
  - `72-backfill-profile-2010b`：在预加载 existing state 与分批提交修复后再次整窗重跑，仍在整窗 reuse 阶段被外层命令超时中断
  - `72-backfill-profile-2010c`：改为只对 `195` 个剩余缺口标的重跑，`candidate_profile_count = 42837`，`inserted_profile_count = 42720`，`reused_profile_count = 117`
- `tests/unit/data/test_tushare_objective_runner.py -q` 通过，验证最小性能修复没有改坏 `71` 合同。
- full-window coverage audit 最新读数：
  - `filter_snapshot_count = 6835`
  - `covered_objective_count = 6835`
  - `missing_objective_count = 0`
  - `missing_ratio = 0.0`
- 正式库 `raw_tdxquant_instrument_profile` 当前累计：
  - `objective_profile_row_count = 392488`
  - `objective_profile_instrument_count = 1833`
  - `2010-01-04 -> 2010-12-31` 窗口 `profile_count = 392478`

## 产物

- `docs/03-execution/72-historical-objective-profile-backfill-execution-card-20260415.md`
- `docs/03-execution/evidence/72-historical-objective-profile-backfill-execution-evidence-20260415.md`
- `docs/03-execution/records/72-historical-objective-profile-backfill-execution-record-20260415.md`
- `docs/03-execution/72-historical-objective-profile-backfill-execution-conclusion-20260415.md`
- `H:\Lifespan-temp\data\72-backfill-source-2010a-summary.json`
- `H:\Lifespan-temp\data\72-backfill-profile-2010c-summary.json`
- `H:\Lifespan-temp\filter\coverage-audit-20100104-20260408-summary.json`
- `H:\Lifespan-report\filter\coverage-audit-20100104-20260408-report.md`
- `H:\Lifespan-temp\filter\coverage-audit-20100104-20260408-after-72-2010a-summary.json`
- `H:\Lifespan-report\filter\coverage-audit-20100104-20260408-after-72-2010a-report.md`

## 证据结构图

```mermaid
flowchart LR
    OPEN["72 开卡"] --> INDEX["索引切换到 72"]
    INDEX --> CARD["72 卡面冻结回补边界"]
    CARD --> GOV["治理检查通过"]
    GOV --> AUDIT["full-window coverage audit baseline"]
    AUDIT --> TOKEN["加载正式 TUSHARE_TOKEN"]
    TOKEN --> SRC["2010 source sync 完成"]
    SRC --> TRY1["2010a / 2010b 整窗 materialization 超时"]
    TRY1 --> FIX["最小性能修复 + 剩余缺口定向重跑"]
    FIX --> DONE["2010 coverage 补齐，full-window missing=0"]
```
