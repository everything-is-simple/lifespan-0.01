# 主线本地账本增量同步与断点续跑证据

证据编号：`40`
日期：`2026-04-13`

## 命令证据

1. `python -m py_compile src/mlq/data/data_mainline_incremental_sync.py src/mlq/data/runner.py src/mlq/data/__init__.py scripts/data/run_mainline_local_ledger_incremental_sync.py tests/unit/data/test_mainline_incremental_sync.py`
   - 结果：通过
2. 手工回放脚本
   - 场景一：官方 `structure` ledger 原地续跑，`CASE1 1 1 2026-04-08`
   - 场景二：legacy `structure.duckdb` 外部 source 两次推进同步，`CASE2 1 1 2026-04-10 2`
   - 场景三：显式 replay 覆盖 tail checkpoint，`CASE3 source_replayed 2026-04-01 2026-04-09`

## 实现证据

- `src/mlq/data/data_mainline_incremental_sync.py`
  - 新增 `40` 的正式 runner
  - 冻结 `run / checkpoint / dirty_queue / freshness_readout` 控制账本
  - 只按官方 ledger 的业务日期列计算 `latest_bar_dt`，避免被审计时间戳污染 freshness
- `scripts/data/run_mainline_local_ledger_incremental_sync.py`
  - 新增 `40` 的正式 CLI 入口
- `tests/unit/data/test_mainline_incremental_sync.py`
  - 覆盖原地续跑
  - 覆盖外部 source 推进后的复制同步
  - 覆盖显式 replay 对 tail checkpoint 的回放

## 环境说明

- 当前容器里 `pytest` 退出阶段会在清理临时目录时触发 Windows `PermissionError`，因此本卡运行证据先以 `py_compile + 手工回放脚本` 登记。
- 单测文件已补齐，后续在正式本地环境可直接按串行 `pytest` 口径回归。
