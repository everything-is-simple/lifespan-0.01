# alpha 五表族共享合同与 family ledger bootstrap 规格
日期：`2026-04-09`
状态：`生效中`

## 适用范围

本规格用于冻结 `alpha trigger ledger` 之上的五表族最小正式 family ledger 合同。
当前只覆盖：

1. 五表族共享 family contract
2. family ledger 最小正式表族
3. family ledger bounded runner 与脚本入口
4. 一到两个 family 的 bounded official pilot 合同

本规格不代表：

1. 五个 family 的完整专表都已全部正式齐备
2. `position / trade / system` 已可直接消费 family ledger
3. full-history family backfill 已在本轮完成

## 正式输入

family ledger 的正式输入固定分成三类：

1. 官方 `alpha_trigger_event`
   - 至少提供 `trigger_event_nk / instrument / signal_date / asof_date / trigger_family / trigger_type / pattern_code`
2. detector 或 family-specific bounded 输入
   - 只允许作为 `alpha` 内部构造 family payload 的实现来源
   - 不允许被下游直接视作正式对接合同
3. runner 自身 run 元数据
   - 包括 `run_id / family_contract_version / source table identity / bounded window`

硬约束：

1. 不允许绕开 `alpha_trigger_event` 直接以 detector sidecar 充当 family 正式主键来源
2. 不允许把 `H:\Lifespan-temp` 里的中间结果当正式 family ledger
3. 不允许让下游模块绕过 `alpha_formal_signal_event` 直接消费 family ledger 充当交易信号

## 正式输出

正式落点固定为 `H:\Lifespan-data\alpha\alpha.duckdb`。

当前 `v1` 最小正式表族固定为：

1. `alpha_family_run`
2. `alpha_family_event`
3. `alpha_family_run_event`

## 五表族共享合同

本轮 family ledger 至少要能容纳下面五个 family：

1. `bof`
2. `tst`
3. `pb`
4. `cpb`
5. `bpb`

共享最小字段组固定为：

1. `family_event_nk`
2. `trigger_event_nk`
3. `instrument`
4. `signal_date`
5. `asof_date`
6. `trigger_family`
7. `trigger_type`
8. `pattern_code`
9. `family_code`
10. `family_contract_version`
11. `payload_json`
12. `first_seen_run_id`
13. `last_materialized_run_id`

说明：

1. `trigger_family` 保持与 trigger ledger 对齐
2. `family_code` 用于表达 family-specific 最小细分语义
3. `payload_json` 先承载最小 family-specific payload，不强制本轮把全部 payload 拆成稳定列

## 1. `alpha_family_run`

用途：

1. 记录一次 bounded family ledger materialization run
2. 固定本次运行的 family 范围、版本、来源与摘要

最小字段：

1. `run_id`
2. `producer_name`
3. `producer_version`
4. `run_status`
5. `family_scope`
6. `signal_start_date`
7. `signal_end_date`
8. `bounded_instrument_count`
9. `materialized_family_event_count`
10. `source_trigger_table`
11. `family_contract_version`
12. `started_at`
13. `completed_at`
14. `summary_json`

状态枚举：

1. `pending`
2. `running`
3. `completed`
4. `failed`

规则：

1. `run_id` 只做审计，不做 family 事实主语义
2. `family_scope` 必须说明本次覆盖了哪些 family
3. `summary_json` 必须能回答本次的 `inserted / reused / rematerialized` 统计

## 2. `alpha_family_event`

用途：

1. 保存五表族最小正式 family 事实层
2. 成为 `alpha_trigger_event` 之上的官方 family-specific 解释层

最小字段：

1. `family_event_nk`
2. `trigger_event_nk`
3. `instrument`
4. `signal_date`
5. `asof_date`
6. `trigger_family`
7. `trigger_type`
8. `pattern_code`
9. `family_code`
10. `family_contract_version`
11. `payload_json`
12. `first_seen_run_id`
13. `last_materialized_run_id`
14. `created_at`
15. `updated_at`

`family_event_nk` 规则：

`v1` 固定由下面语义字段拼出稳定自然键：

1. `trigger_event_nk`
2. `trigger_family`
3. `trigger_type`
4. `pattern_code`
5. `family_code`
6. `family_contract_version`

规则：

1. 同一 family 事实不得因为 `run_id` 变化而生成新主键
2. `payload_json` 允许在同一自然键下随合同升级而 rematerialize
3. 本轮优先冻结最小 family 事实层，不强制一次性拆出全部 family-specific payload 列

## 3. `alpha_family_run_event`

用途：

1. 桥接某次 `run` 与本次触达的 family 事实
2. 支持 resume、审计、bounded readout 与 selective rebuild

最小字段：

1. `run_id`
2. `family_event_nk`
3. `trigger_event_nk`
4. `materialization_action`
5. `family_code`
6. `recorded_at`

动作枚举：

1. `inserted`
2. `reused`
3. `rematerialized`

规则：

1. `run_event` 是桥接层，不是新的 family 主事实层
2. 同一 `run_id + family_event_nk` 不得重复写入多行
3. 该表必须支持每次 bounded pilot 的 per-action 统计

## 与 trigger ledger / formal signal 的对接规则

固定分层为：

1. `alpha_trigger_event`
   - 回答“触发发生过什么”
2. `alpha_family_event`
   - 回答“该 family 的最小正式解释与 payload 是什么”
3. `alpha_formal_signal_event`
   - 回答“当前是否成为下游可消费正式信号”

禁止：

1. 让 `position / trade / system` 直接把 `alpha_family_event` 当交易主信号
2. 让 family runner 反向重写 `alpha_trigger_event`
3. 把 `alpha_family_event` 退化成 research-only 临时表

## 增量与 selective rebuild 规则

1. runner 必须支持 bounded window 执行
2. runner 必须支持按 `family` 与 `instrument` 裁切
3. 同一 `family_event_nk` 重复命中时优先记为 `reused`
4. 当 family 合同或 payload 生成规则变化时，必须显式记为 `rematerialized`
5. 不允许为了 bounded pilot 方便而清空正式 family ledger 后重写

## Family Runner 合同

### Python 入口

正式 Python 入口固定命名为：

`run_alpha_family_build(...)`

### 脚本入口

正式脚本入口固定命名为：

`scripts/alpha/run_alpha_family_build.py`

### 最小参数

1. `run_id`
2. `family_scope`
3. `signal_start_date`
4. `signal_end_date`
5. `instrument` 或 bounded instrument 列表
6. `limit`
7. `batch_size`
8. `source_trigger_table`
9. `source_candidate_table`
10. `summary_path`

### 最小职责

1. 从官方 `alpha_trigger_event` 读取 bounded 样本
2. 构造 `alpha_family_event`
3. 写入 `alpha_family_run`
4. 写入 `alpha_family_run_event`
5. 输出 summary JSON

### 明确禁止

1. 自动调用 `alpha formal signal` runner
2. 自动写 `position / trade / system`
3. 默认把所有 family-specific payload 都展开成最终列族
4. 继续依赖 temp-only sidecar 充当正式账本

## Bounded Evidence 要求

本卡完成时至少要留下：

1. 单元测试
   - 覆盖 `run / event / run_event`
   - 覆盖 `inserted / reused / rematerialized`
2. 一次真实写入 `H:\Lifespan-data\alpha\alpha.duckdb` 的 bounded pilot
3. 一次正式库 readout
   - 至少给出 `run_id`
   - 至少给出 `family_event_count`
   - 至少给出 per-action 统计
4. 一次 `alpha_trigger_event -> alpha_family_event` 对接验证

## 当前明确不做

1. 五个 family 的全部最终专表
2. `position / trade / system` 改读 family ledger
3. full-market 全历史 family backfill
4. 回头重做 `position`

## 一句话收口

`13` 的最小正式合同不是把五表族全部做完，而是先把共享 trigger 事实继续推进成可复用、可复物化、可审计的 family ledger 层，并先在一到两个 family 上证明这套合同站得住。
