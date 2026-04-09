# alpha trigger ledger 与五表族最小物化规格
日期：`2026-04-09`
状态：`生效中`

## 适用范围

本规格用于冻结 `alpha` 模块在 `formal signal` 之上的上一层最小正式中间账本。
当前只覆盖：

1. `alpha_trigger_run`
2. `alpha_trigger_event`
3. `alpha_trigger_run_event`
4. bounded materialization runner
5. 写入 `H:\Lifespan-data` 的正式 pilot 合同

本规格不代表：

1. `alpha` 内部全部五家族细节表已经正式落齐
2. `trade / system` 已经可以直接消费这层 ledger
3. full-history backfill 已在本轮完成

## 正式输入

`alpha trigger ledger` 的正式输入固定分成三类：

1. `market_base`
   - 提供检测所需的正式价格事实基础
2. 官方 `filter_snapshot + structure_snapshot`
   - 提供 `alpha` 需要复用的官方准入与结构上下文
3. `alpha` 自身 detector bounded 输入
   - 只允许作为 `alpha` 内部生产触发事实的实现来源
   - 不允许下游把 detector 中间态当作正式对接契约

硬约束：

1. 不允许把 `run_id` 当成 trigger 历史主语义
2. 不允许把 `H:\Lifespan-temp` 里的 smoke 结果当成正式账本
3. 不允许让 `position / trade / system` 直接消费未冻结的内部 sidecar

## 正式输出

正式落点固定为模块级历史账本 `H:\Lifespan-data\alpha\alpha.duckdb`。

当前 `v1` 最小正式表族固定为：

1. `alpha_trigger_run`
2. `alpha_trigger_event`
3. `alpha_trigger_run_event`

## 五家族最小共享合同

本轮不强制五家族各自拆成正式独立专表，但共享正式合同必须已经容纳：

1. `bof`
2. `tst`
3. `pb`
4. `cpb`
5. `bpb`

因此共享事实层至少要稳定保存：

1. `trigger_family`
2. `trigger_type`
3. `pattern_code`
4. `instrument`
5. `signal_date`
6. `asof_date`

是否在本轮把五种 `trigger_type` 全部都接满，允许在执行卡里按 detector 稳定度裁剪；
但正式 schema 不得把五家族排除在合同之外。

## 1. `alpha_trigger_run`

用途：

1. 记录一次 bounded trigger materialization run
2. 固定本次运行的输入范围、版本、来源与摘要

最小字段：

1. `run_id`
2. `producer_name`
3. `producer_version`
4. `run_status`
5. `signal_start_date`
6. `signal_end_date`
7. `bounded_instrument_count`
8. `detected_trigger_count`
9. `source_filter_table`
10. `source_structure_table`
11. `trigger_contract_version`
12. `started_at`
13. `completed_at`
14. `summary_json`
15. `notes`

状态枚举：

1. `pending`
2. `running`
3. `completed`
4. `failed`

规则：

1. `run_id` 只做审计，不做事件主键
2. `summary_json` 必须能回答本次写入的 inserted / reused / rematerialized 统计
3. 非 `completed` run 不得被描述成正式验收证据

## 2. `alpha_trigger_event`

用途：

1. 保存 `alpha` 内部最小正式 trigger 发生事实
2. 成为 `alpha_formal_signal_event.source_trigger_event_nk` 的官方上游

最小字段：

1. `trigger_event_nk`
2. `instrument`
3. `signal_date`
4. `asof_date`
5. `trigger_family`
6. `trigger_type`
7. `pattern_code`
8. `trigger_contract_version`
9. `first_seen_run_id`
10. `last_materialized_run_id`
11. `created_at`
12. `updated_at`

自然键规则：

`v1` 固定由下面语义字段稳定拼出：

1. `instrument`
2. `signal_date`
3. `asof_date`
4. `trigger_family`
5. `trigger_type`
6. `pattern_code`
7. `trigger_contract_version`

规则：

1. 同一事实不得因为 `run_id` 变化而生成新主键
2. 初次出现记 `first_seen_run_id`
3. 后续复物化只更新 `last_materialized_run_id`
4. 本轮优先冻结共同事实层，不强制一次性补齐全部 family-specific payload

## 3. `alpha_trigger_run_event`

用途：

1. 桥接某次 `run` 与本次触达的 trigger 事实
2. 支撑 resume、审计、bounded readout 和 selective rebuild

最小字段：

1. `run_id`
2. `trigger_event_nk`
3. `materialization_action`
4. `trigger_type`
5. `recorded_at`

动作枚举：

1. `inserted`
2. `reused`
3. `rematerialized`

规则：

1. `run_event` 是桥接层，不是新的主事实层
2. 同一 `run_id + trigger_event_nk` 不得重复写入多行
3. 该表必须支撑每次 bounded pilot 的动作统计

## 与 `formal signal` 的对接规则

`alpha_formal_signal_event` 继续作为对下游的官方消费层。
当前固定关系为：

1. `alpha_trigger_event` 回答 trigger 是否发生
2. `alpha_formal_signal_event` 在此基础上叠加官方 `filter / structure` 上下文，回答是否 admitted / blocked / deferred
3. `position` 当前仍只允许消费 `alpha_formal_signal_event`

禁止：

1. 让 `position` 直接把 `alpha_trigger_event` 当正式交易信号
2. 让 `formal signal` 重新退化成 research-only 临时表

## 正式 pilot 合同

本轮必须至少完成一次 bounded official pilot，且满足：

1. 数据真实写入 `H:\Lifespan-data\alpha\alpha.duckdb`
2. pilot 可以是 bounded date window、bounded instrument slice，或二者组合
3. summary、命令证据、临时导出写入 `H:\Lifespan-temp`
4. pilot 完成后能够对正式库做 readout，证明事实已沉淀

推荐但不强制：

1. 优先沿袭旧仓已验证的 `2010` pilot 思路
2. 若当前数据准备不足，可先缩成更小窗口，但不得退回 temp-only 口径

## 重跑与 selective rebuild 规则

1. 重复运行命中同一 `trigger_event_nk` 时优先 `reused`
2. 当 detector 合同或官方上下文变化导致事实需要更新时，必须显式记账 `rematerialized`
3. 不允许为了 bounded pilot 方便而清空正式历史账本后重写
4. selective rebuild 应按 `instrument / signal_date window / trigger_type` 裁切，而不是按整库删除

## Bounded Evidence 要求

本卡完成时至少要留下：

1. 单元测试
   - 覆盖 `run / event / run_event`
   - 覆盖 `inserted / reused / rematerialized`
2. 一次真实写入正式数据根的 bounded pilot 命令
3. 一次正式库 readout
   - 至少给出 `run_id`
   - 至少给出 `trigger_event_count`
   - 至少给出 per-action 统计
4. 一次 `alpha_trigger_event -> alpha_formal_signal_event` 对接验证

## 当前明确不做

1. 五家族全部细节专表一次性补齐
2. `position / trade / system` 正式改读 `trigger ledger`
3. full-market 全历史一次性回填
4. 为了开卡而回头改写 `position`

## 一句话收口

`12` 的最小正式合同不是“多一张 alpha 临时表”，而是把 `trigger ledger` 变成写入正式数据根、可复用、可复物化、可被 `formal signal` 稳定引用的历史账本层。
