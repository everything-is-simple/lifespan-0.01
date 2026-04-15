# malf canonical official 2010 bootstrap and replay 证据
`证据编号`：`57`
`日期`：`2026-04-14`

## 实现与验证命令

1. `python scripts/data/run_market_base_build.py --asset-type stock --adjust-method backward --start-date 2010-01-01 --end-date 2010-12-31 --build-mode full --limit 500000 --run-id card57-data-prereq-backward-2010-001 --summary-path H:\Lifespan-temp\reports\card57-data-prereq-backward-2010-summary.json`
   - 结果：通过
   - 说明：先补齐 `market_base.stock_daily_adjusted(adjust_method='backward')` 的 `2010-01-01 ~ 2010-12-31` 正式窗口，写入 `392,478` 行、覆盖 `1,833` 个股票代码。
2. `python scripts/malf/run_malf_canonical_build.py --signal-start-date 2010-01-01 --signal-end-date 2010-12-31 --limit 5000 --run-id card57-malf-canonical-2010-001 --summary-path H:\Lifespan-temp\reports\card57-malf-canonical-2010-summary.json`
   - 结果：通过
   - 说明：真实正式 `malf.duckdb` 完成 `2010` canonical 首跑，`D/W/M` 三个级别共 `5,499` 个 scope 全部完成。
3. `python scripts/malf/run_malf_canonical_build.py --signal-start-date 2010-01-01 --signal-end-date 2010-12-31 --limit 5000 --run-id card57-malf-canonical-2010-002 --summary-path H:\Lifespan-temp\reports\card57-malf-canonical-2010-replay-summary.json`
   - 结果：通过
   - 说明：同窗 replay 为严格 no-op，`queue_enqueued_count=0`、`claimed_scope_count=0`，checkpoint 已生效。
4. 真实库核对：
   - `malf_canonical_work_queue`：`5,499` 行且全部 `completed`
   - `malf_canonical_checkpoint`：`5,499` 行，`D/W/M` 各 `1,833`
   - `malf_pivot_ledger`：`164,263`
   - `malf_wave_ledger`：`221,628`
   - `malf_extreme_progress_ledger`：`103,864`
   - `malf_state_snapshot`：`496,777`
   - `malf_same_level_stats`：`60,988`

## 冻结事实

1. `H:\Lifespan-data\malf\malf.duckdb` 已从仅有 bridge-v1 五张旧表，扩展为包含 canonical run / queue / checkpoint 与五个核心账本的正式库。
2. `2010` canonical 首跑成功后，正式 `malf` 主线已经具备真实 `bootstrap + replay` 能力。
3. `57` 的实际阻塞点不在 `malf` 模块本身，而在 `data` 侧 `backward` 正式口径缺失 `2010` 窗口；该缺口已通过正式 `market_base` runner 补齐。

## 证据结构图

```mermaid
flowchart LR
    DATA["2010 backward market_base prerequisite"] --> MALF["57 canonical bootstrap"]
    MALF --> QCP["queue/checkpoint"]
    QCP --> REPLAY["2010 replay no-op"]
```
