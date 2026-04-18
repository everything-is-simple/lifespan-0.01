# malf timeframe native base source 重绑 记录

`记录编号`：`91`
`日期`：`2026-04-18`

## 做了什么

1. 把 `canonical_runner` 从“单 day `market_base` + `_resample_bars_by_timeframe`”改成“按 `D/W/M` 分别绑定 `market_base_day/week/month` 与对应 `stock_daily_adjusted / stock_weekly_adjusted / stock_monthly_adjusted`”。
2. 把 canonical 写库从 legacy 单库改成 official native 三库：`malf_day / malf_week / malf_month` 分别 bootstrap、分别 enqueue/claim/checkpoint、分别写 `malf_canonical_run.summary_json`。
3. 在 canonical source 里恢复 `limit=0` 的 full coverage 入口，使 `run_malf_canonical_build --limit 0` 可以覆盖全部 `5501` 个官方 scope，而不是被默认 row limit 截断。
4. 补 `tests/unit/malf/test_canonical_runner.py`，覆盖 native source 选择、周/月不再走 day resample、三库独立落表与 requeue/checkpoint 行为。
5. 执行官方 full coverage build，把 `market_base_day/week/month(backward)` 全量物化为 `malf_day/week/month`，并输出审计 JSON。

## 偏离项

- 通过桌面 shell 直接执行 full coverage 时，1 小时超时包装器先返回超时，但 child `python scripts/malf/run_malf_canonical_build.py --limit 0 ...` 继续在后台完成 `W/M`。最终按 run 表与落盘审计确认三库都完成 `run_status='completed'`，未做人工中断或手工补写账本。

## 备注

- `malf_day / week / month` 当前都是首次 official native full coverage 建仓；`D/W/M` 最新 checkpoint 都追平到 `2026-04-10`，每库 `checkpoint_count=5501`。
- `snapshot / mechanism / wave_life` 仍保持 explicit legacy 单库回退位；`91` 只收 canonical native source 与 full coverage，不提前替 `92-95` 做 downstream 重绑。
- `91` 收口后，当前待施工位推进到 `92`。

## 记录结构图

```mermaid
flowchart LR
    CODE["native source refactor"] --> TEST["pytest + compileall"]
    TEST --> BUILD["official full coverage build"]
    BUILD --> AUD["audit json"]
    AUD --> CON["81 conclusion"]
```



