# 主链 truthfulness 复核记录
记录编号：`26`
日期：`2026-04-11`

## 做了什么
1. 先按 `26` 号卡与其 design/spec 回读 `23/24/25` 的生效口径，静态复核了 `data -> malf -> structure -> filter -> alpha -> position -> portfolio_plan -> trade` 各 runner 的正式输入输出合同、sidecar 消费点和价格口径默认值。
2. 在静态复核中定位到一个真实偏差：`position` 底层 runner 默认 `adjust_method='none'`，但 CLI 入口脚本仍错误保留 `backward` 默认值，构成正式执行入口与正式合同不一致。
3. 将 `scripts/position/run_position_formal_signal_materialization.py` 的 CLI 默认值修正为 `none`，把偏差直接收口在本卡内，不再额外拆出修复卡。
4. 新增 `tests/unit/position/test_cli_entrypoint.py`，把 CLI 默认口径固化为显式回归测试。
5. 新增 `tests/unit/system/test_mainline_truthfulness_revalidation.py`，以 bounded 整链方式实际跑通 `structure -> filter -> alpha -> position -> portfolio_plan -> trade`，并在同一条测试中核对：
   - `structure / filter` 对 break/stats sidecar 的只读附加身份
   - `alpha` 对官方 `filter / structure snapshot` 的正式消费
   - `position -> trade` 对 `none` 价格口径的正式边界
6. 修正 `tests/unit/alpha/test_runner.py` 中一处过时样本价格口径，使其与正式 `position` 合同重新一致。
7. 串行执行新增测试、受影响模块单测和治理检查，避免 Windows 下两个 `pytest` 进程共享 `H:\Lifespan-temp\pytest-tmp` 时互相清理导致的临时目录冲突。
8. 回填 `26` 的 evidence / record / conclusion，并把执行索引切到最新结论锚点。

## 偏离项

- 本卡发现过一个真实入口偏差，但已在本卡内修复并补回归测试，因此最终裁决不再要求另开后置修复卡。
- 本卡没有趁机把 sidecar 继续扩到 `alpha / position` 的正式读数逻辑，也没有启动 `malf` canonical runner 替换工程；这些仍然不属于 `26` 的实现边界。

## 备注

- 静态合同复核表明：`structure / filter / alpha / position / portfolio_plan / trade` 当前仍通过正式上游账本对接，未发现绕过正式账本直接消费 bridge 私有中间过程的主链断裂。
- bounded 整链测试证明：`23/24/25` 引入的新 `malf` 口径没有破坏主链 truthfulness；本次唯一偏差是 `position` CLI 默认值漂移，而不是主链设计本身失真。
- 由于下一张卡尚未正式开启，当前治理锚点仍保留在 `26`；后续若要进入 `system` 正式实现，必须先另开新卡，不能继续借 `26` 施工。
