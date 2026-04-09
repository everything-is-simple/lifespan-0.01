# alpha formal signal 正式出口合同与最小 producer 记录

记录编号：`10`
日期：`2026-04-09`

## 做了什么

1. 以 `10` 号卡为边界，把实现范围压到 `alpha_formal_signal_run / alpha_formal_signal_event / alpha_formal_signal_run_event`、最小 producer runner、脚本入口和 bounded evidence。
2. 参考 `G:\EmotionQuant-gamma\normandy\` 与 `G:\MarketLifespan-Quant` 老仓 `alpha` 结论，沿袭了“trigger ledger -> formal signal -> position”的分层、run/event/run_event 三表身份，以及 bounded 分批物化的审计方式。
3. 在 `src/mlq/alpha/bootstrap.py` 冻结新仓 `alpha formal signal` 官方三表；在 `src/mlq/alpha/runner.py` 落下最小 producer，支持：
   - bounded 日期窗口
   - 按 instrument 分批
   - 读取官方 trigger 表与官方 context 表
   - 生成稳定 `signal_nk`
   - 对 event 做 `inserted / reused / rematerialized` 判定
   - 回写 run summary
4. 新增 `scripts/alpha/run_alpha_formal_signal_build.py` 作为正式脚本入口，并把 `README.md`、`AGENTS.md`、`scripts/README.md`、`pyproject.toml` 同步到“alpha producer 已成立”的入口口径。
5. 新增 `tests/unit/alpha/test_runner.py`，覆盖：
   - 三表落库
   - context 改变后的 rematerialized
   - `position` 对新仓官方上游的真实消费

## 实现取舍

- 本轮没有把老仓 PAS 五表族整包迁入，而是只冻结当前下游真实在消费的 `formal signal` 官方出口。
- producer 当前允许对 trigger/context 源表做最小列名兼容，但不允许反向读取 `alpha` 内部临时过程，也不自动串调 `position / trade / system`。
- `position` runner 本轮没有改动消费主逻辑，只依赖它已支持的 `alpha_formal_signal_event` 合同与 `last_materialized_run_id -> source_signal_run_id` 回退口径。

## 备注

- 这张卡完成后，`M2 alpha-position 正式桥接成立` 可以正式标完成。
- 下一张正式主线卡不应再回到 `position` 深挖 family 表，而应围绕 `structure / filter` 或更下游模块继续开卡。
