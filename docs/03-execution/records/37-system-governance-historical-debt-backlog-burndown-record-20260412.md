# system governance historical debt backlog burndown 记录

记录编号：`37`
日期：`2026-04-12`

## 做了什么

1. 以全仓治理扫描结果为基线，确认当前剩余历史债务清单已经落在 `scripts/system/development_governance_legacy_backlog.py`。
2. 新开 `37` 执行 bundle，并把当前待施工卡从 `100` 切换为 `37`。
3. 补写 `37` 对应的 design / spec / card / evidence / record / conclusion。
4. 把 2026-04-12 已完成的首批纠偏项登记为 `37` 的已解决项。
5. 修正 `new_execution_bundle.py` 的模板编号渲染和索引分栏标题错误。
6. 拆分 `src/mlq/system/runner.py`，把 child run 读取、snapshot 聚合、物化写回与共享归一化逻辑拆到独立 helper 模块，保留 `run_system_mainline_readout_build` 外部入口不变。
7. 通过 `py_compile`、文档门禁、执行索引检查、按路径治理检查和 `system` 串行单测，确认 `src/mlq/system/runner.py` 可从历史硬超长 backlog 移除。
8. 拆分 `src/mlq/trade/runner.py`，把共享结构、上游读取、执行计划构造、carry 继承与落表写回拆到独立 helper 模块，保留 `run_trade_runtime_build` 外部入口不变。
9. 通过 `py_compile` 与 `trade` 串行单测，确认 `src/mlq/trade/runner.py` 可从历史硬超长 backlog 移除。
10. 拆分 `src/mlq/alpha/trigger_runner.py`，把共享结构、source 读取与事件物化逻辑拆到独立 helper 模块，保留 `run_alpha_trigger_build` 外部入口和 queue/checkpoint 语义不变。
11. 通过 `py_compile`、按路径治理检查与 `alpha` 单测，确认 `src/mlq/alpha/trigger_runner.py` 可从历史硬超长 backlog 移除。
12. 拆分 `src/mlq/filter/runner.py`，把共享结构、上游读取与落表物化逻辑拆到独立 helper 模块，保留 `run_filter_snapshot_build` 外部入口和 queue/checkpoint 语义不变。
13. 通过 `py_compile`、按路径治理检查与 `filter` 单测，确认 `src/mlq/filter/runner.py` 可从历史硬超长 backlog 移除。
14. 拆分 `src/mlq/malf/mechanism_runner.py`，把共享结构、桥接输入读取与 sidecar 物化逻辑拆到独立 helper 模块，保留 `run_malf_mechanism_build` 外部入口不变。
15. 通过 `py_compile`、按路径治理检查与 `malf mechanism` 单测，确认 `src/mlq/malf/mechanism_runner.py` 可从历史硬超长 backlog 移除。
16. 拆分 `src/mlq/malf/canonical_runner.py`，把共享结构、上游行情读取与 canonical 物化逻辑拆到独立 helper 模块，保留 `run_malf_canonical_build` 外部入口不变。
17. 通过 `py_compile`、按路径治理检查与 `malf canonical` 单测，确认 `src/mlq/malf/canonical_runner.py` 可从历史硬超长 backlog 移除。
18. 拆分 `src/mlq/structure/runner.py`，把共享结构、上游读取、列解析与 sidecar 查询、脏队列/checkpoint 与落表物化逻辑拆到独立 helper 模块，保留 `run_structure_snapshot_build` 外部入口和 bounded 语义不变。
19. 通过 `py_compile` 与 `structure` 串行单测，确认 `src/mlq/structure/runner.py` 可从历史硬超长 backlog 移除。
20. 严格治理检查进一步暴露 `structure_source.py` 仍超过 1000 行硬上限，于是继续切出 `structure_query.py` 承接列解析与 sidecar 查询；修正一次漏导入后，重新通过 `alpha + structure` 串行联动单测。

## 偏离项

- `new_execution_bundle.py` 自动回填在实际执行中失效，原因是脚本使用的目录分栏标题和仓库现状不一致；本次已先修脚本，再手工补齐 `37` 索引与当前卡状态。
- 我一度并行启动两个 `pytest` 进程，导致它们争用 `H:\Lifespan-temp\pytest-tmp` 并相互清理临时目录；本卡后续测试一律按串行证据口径执行。
- `structure` 二次拆分时一度漏掉 `_normalize_optional_nullable_str` 导入，`alpha + structure` 联动回归立即暴露；已补齐导入并重跑通过。

## 备注

- 当前最新生效结论锚点仍保持为 `36`；`37` 只是新的治理清债施工卡，不代表新的生效业务结论。
- `100-105` 未取消，只是暂时后移到 `37` 之后。
- 当前 `37` 已开始清账，已完成的前七项是 `src/mlq/system/runner.py`、`src/mlq/trade/runner.py`、`src/mlq/alpha/trigger_runner.py`、`src/mlq/filter/runner.py`、`src/mlq/malf/mechanism_runner.py`、`src/mlq/malf/canonical_runner.py` 与 `src/mlq/structure/runner.py`。

## 记录结构图

```mermaid
flowchart LR
    STEP["施工步骤"] --> DEV["偏离项说明"]
    DEV --> NOTE["备注"]
    NOTE --> CON["结论引用"]
```
