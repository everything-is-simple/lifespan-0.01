# 41-alpha-pas-five-trigger-canonical-detector 记录
更新时间 `2026-04-13`

## 实施内容

1. 新增官方 PAS detector 表族与 runner
   - `src/mlq/alpha/pas_shared.py`
   - `src/mlq/alpha/pas_detectors.py`
   - `src/mlq/alpha/pas_source.py`
   - `src/mlq/alpha/pas_materialization.py`
   - `src/mlq/alpha/pas_runner.py`
   - `scripts/alpha/run_alpha_pas_five_trigger_build.py`

2. 扩展 `alpha bootstrap`
   - 在 `src/mlq/alpha/bootstrap.py` 中新增：
     - `alpha_pas_trigger_run`
     - `alpha_pas_trigger_work_queue`
     - `alpha_pas_trigger_checkpoint`
     - `alpha_trigger_candidate`
     - `alpha_pas_trigger_run_candidate`

3. 保持 downstream contract 不变
   - `trigger_runner` 继续从 `alpha_trigger_candidate` 读最小六列
   - `family_runner` 不改正式 contract，只读取 candidate 扩展列
   - `formal_signal_runner` 保持既有输入合同

4. 更新入口文件
   - `AGENTS.md`
   - `README.md`
   - `pyproject.toml`
   - `scripts/README.md`

5. 新增单测
   - `tests/unit/alpha/test_pas_runner.py`

## 关键实现决策

1. `alpha_trigger_candidate` 继续保留为官方 trigger/family 的共享输入表
   - 本卡不重写 downstream 表族
   - 只补官方 producer

2. detector 输入严格限定为 canonical 主线
   - `filter_snapshot`
   - `structure_snapshot`
   - `market_base.stock_daily_adjusted(adjust_method='backward')`

3. queue/checkpoint 以 `filter_checkpoint(timeframe='D')` 作为 dirty scope 来源
   - 与当前主线 `structure -> filter -> alpha` 的续跑路径对齐

4. detector trace 以只读 JSON 方式沉淀
   - 不把 PAS 业务细节硬编码进 `trigger_event`
   - 由 `family` 在后续 ledger 中按需透传

## 偏差与修正

1. 初版 queue 实现直接复用 bounded run
   - 会重复插入同一 `run_id`
   - 已改为 `_materialize_scope_window(...)`，把 source/materialization 从 run row 插入中解耦

2. 初版测试夹具只触发了 `bof / tst`
   - 已重写 `pb / cpb / bpb` 的价格样本
   - 当前单测已覆盖五触发在官方 runner 内的落表
