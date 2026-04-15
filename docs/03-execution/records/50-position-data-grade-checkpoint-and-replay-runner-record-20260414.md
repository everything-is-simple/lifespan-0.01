# position data-grade checkpoint 与 replay runner 记录

记录编号：`50`
日期：`2026-04-14`

## 实施记录

1. 先冻结 `position` 的控制面账本，而不是改写业务自然键
   - 新增 `position_work_queue / position_checkpoint / position_run_snapshot`
   - `checkpoint_nk` 固定锚定到 `candidate_nk`
   - `queue_nk` 固定为 `candidate_nk + queue_reason`
2. 保留原有 bounded materialization 合同，只把 orchestration 升级成 data-grade runner
   - `scripts/position/run_position_formal_signal_materialization.py` 脚本名不变
   - 无窗口调用默认走 `checkpoint_queue`
   - 显式 `signal_start_date / signal_end_date / instrument` 仍保留 bounded replay 入口
3. 把 rematerialize 约束收敛为“单候选局部删除后重物化”
   - 只删除单个 `candidate_nk` 对应的 `candidate / risk_budget / capacity / sizing / entry / funding / exit`
   - 不允许退化为全表重跑
4. 把复用判定绑定到正式 source fingerprint
   - fingerprint 覆盖 `alpha formal signal`、`reference price` 与 `policy contract`
   - fingerprint 未变化且核心表族完整时直接判定 `reused`
   - fingerprint 变化则转入 `rematerialized`
5. 为治理文件长度把 runner 拆成 orchestration 与 helper
   - `runner.py` 只保留执行路径与摘要装配
   - `position_runner_shared.py / position_runner_support.py` 承担常量、source、queue、checkpoint、run audit 细节

## 边界

- `50` 不改动 `candidate_nk / entry_leg_nk / exit_plan_nk / exit_leg_nk` 的既有自然键合同；这些仍继承 `47-49`。
- `50` 不把 `position` 越界升级成 `portfolio_plan / trade` 消费者；下游继续等 `51-55`。
- `50` 不处理全仓历史超长文件债务；`python scripts/system/check_development_governance.py` 的全仓扫描仍被未触达的 `src/mlq/data/data_mainline_incremental_sync.py` 阻断，但本卡未新增 `position` 路径治理违规。
