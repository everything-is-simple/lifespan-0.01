# alpha 五表族共享合同与 family ledger bootstrap 记录

记录编号：`13`
日期：`2026-04-09`

## 做了什么？
1. 在 `src/mlq/alpha/bootstrap.py` 中补齐 `alpha_family_run / alpha_family_event / alpha_family_run_event` 三表 DDL，并把 family ledger 纳入 `alpha` 正式 bootstrap。
2. 新增 `src/mlq/alpha/family_runner.py`，提供 `run_alpha_family_build(...)`，支持：
   - 从官方 `alpha_trigger_event` 做 bounded 读取
   - 从 `alpha_trigger_candidate` 读取 bounded family candidate 输入
   - 以 `trigger_event_nk + family_code + family_contract_version` 相关语义稳定构造 `family_event_nk`
   - 在 `payload_json` 中固化 family 最小解释层与 trigger 上游指纹
   - 物化 `inserted / reused / rematerialized`
3. 新增 `scripts/alpha/run_alpha_family_build.py` 作为正式脚本入口，并同步刷新 `AGENTS.md`、`README.md`、`pyproject.toml` 的正式 runner 清单。
4. 新增 `tests/unit/alpha/test_family_runner.py`，覆盖：
   - `run / event / run_event` 三表落库
   - `alpha_trigger_event -> alpha_family_event` 桥接
   - `inserted / reused / rematerialized` 三种动作
5. 在 `H:\Lifespan-data` 上完成两轮 family pilot 与一次受控上游变更复跑，证明：
   - `bof / pb` 两个核心 family 已能形成最小正式解释层
   - family ledger 会随官方 trigger 上游变化而 rematerialize

## 偏离项
- 本轮没有一次性把 `bof / tst / pb / cpb / bpb` 五个 family 都拆成独立最终专表，而是只冻结共享合同和 family ledger 最小三表；这正是 13 号卡设计允许的 bootstrap 裁剪。
- 当前 `family_code` 先采用 `*_core` 的最小稳定编码，并允许未来由 candidate 输入显式覆盖；本轮不把全部 family-specific payload 一次性拆成正式列。
- 官方 pilot 只在 `bof / pb` 上证明合同成立，没有宣称五家族全部最终定型。

## 备注

- 当前 shell 默认会优先命中旧仓安装态 `mlq`，所以所有正式脚本命令都显式带 `PYTHONPATH=src`，保证命中的就是本仓源码。
- 本轮真实执行暴露了 DuckDB 文件锁限制：共享上游或共享目标库的 runner 不能并行推进，正式 readout 与 evidence 已按串行顺序收口。
- family ledger 当前仍是 `alpha` 内部解释层；`position / trade / system` 继续只允许消费 `alpha_formal_signal_event`，没有被本轮顺手改写。
