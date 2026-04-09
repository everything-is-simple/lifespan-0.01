# alpha trigger ledger 与五表族最小物化记录

记录编号：`12`
日期：`2026-04-09`

## 做了什么？
1. 在 `src/mlq/alpha/bootstrap.py` 中把 `alpha` 正式账本补成两层表族：
   - `alpha_trigger_run / alpha_trigger_event / alpha_trigger_run_event`
   - `alpha_formal_signal_run / alpha_formal_signal_event / alpha_formal_signal_run_event`
2. 新增 `src/mlq/alpha/trigger_runner.py`，提供 `run_alpha_trigger_build(...)`，支持：
   - 从 `alpha_trigger_candidate` bounded 读取 detector 输入
   - 绑定官方 `filter_snapshot / structure_snapshot`
   - 物化 `inserted / reused / rematerialized`
3. 新增 `scripts/alpha/run_alpha_trigger_ledger_build.py` 作为正式脚本入口，并同步刷新 `AGENTS.md`、`README.md`、`scripts/README.md`、`pyproject.toml` 到“alpha 已有 trigger ledger + formal signal 两级 runner”的入口口径。
4. 改写 `tests/unit/alpha/test_runner.py`：
   - 不再手工把裸触发表直接喂给 `formal signal`
   - 改成先跑官方 `alpha trigger ledger`，再进入 `formal signal`
   - 同时补上 `reused / rematerialized` 的 trigger ledger 单测
5. 在 `H:\Lifespan-data` 下建立最小 official pilot 样本，按正式主链顺序跑通：
   - `structure`
   - `filter`
   - `alpha trigger ledger`
   - `alpha formal signal`

## 偏离项
- 本轮没有直接把五家族拆成五套正式细节专表，而是先冻结共享的最小 trigger ledger 三表；这属于 12 号卡设计中允许的“先厚公共历史账本层”的裁剪，不是漏做。
- 当前官方 trigger runner 直接消费的是 `alpha_trigger_candidate` 这一张 bounded detector 输入表，而不是在本轮内把全量 base-driven detector 重写完毕；这也是本轮刻意保持边界、避免把 full backfill 和 detector 重构混入同一卡的结果。
- 真实 pilot 暴露出顺序执行约束：`structure/filter` 和 `alpha trigger/formal signal` 不能并行写共享库；证据区已按正式顺序重跑并修正。

## 备注

- 当前命令行环境会优先命中旧仓 `mlq`，因此本轮所有实现验证和 pilot 命令都显式带 `PYTHONPATH=src`，保证读取的是本仓源码口径。
- `alpha_formal_signal_event` 本轮没有改 schema，只继续复用它对 `source_trigger_event_nk` 的既有正式合同；变化发生在它现在真正引用的是官方 `alpha_trigger_event.trigger_event_nk`，而不再是裸 detector 表。
