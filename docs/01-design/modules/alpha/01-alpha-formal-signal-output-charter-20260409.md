# alpha formal signal 正式出口设计章程
日期：`2026-04-09`
状态：`生效中`

## 问题

`09-position-formal-signal-runner-and-bounded-validation` 已经把 `position` 消费侧 runner 建起来了，
但当前新仓真正缺的不是更多 `position` 内部表，而是 `alpha` 自己还没有正式的 `formal signal producer` 与正式账本出口。

如果这里继续停留在“消费侧先兼容一张合同表”，会留下三个长期问题：

1. `M2 alpha-position 正式桥接成立` 仍然无法收口，因为上游没有新仓官方 producer。
2. `position` 只能证明“我会读合同”，不能证明“我已经接到新仓真实上游”。
3. 后续 `trade / system` 也拿不到一个可追溯、可续跑、可复算的 `alpha` 正式输出层。

## 设计输入

本章程建立在下面这些已冻结来源之上：

1. `docs/01-design/modules/alpha/00-alpha-module-lessons-20260409.md`
2. `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
3. `docs/02-spec/modules/position/02-alpha-to-position-formal-signal-bridge-spec-20260409.md`
4. `docs/02-spec/modules/position/03-position-formal-signal-runner-spec-20260409.md`
5. `docs/03-execution/09-position-formal-signal-runner-and-bounded-validation-conclusion-20260409.md`
6. `G:\\MarketLifespan-Quant\\docs\\01-design\\modules\\alpha\\05-pas-full-market-five-trigger-ledger-backfill-reset-20260408.md`
7. `G:\\MarketLifespan-Quant\\docs\\01-design\\modules\\alpha\\06-pas-code-ledger-reset-and-2010-pilot-20260408.md`
8. `G:\\MarketLifespan-Quant\\docs\\03-execution\\310-pas-formal-signal-reconnect-to-position-bootstrap-conclusion-20260409.md`
9. `G:\\EmotionQuant-gamma\\normandy\\01-full-design\\01-alpha-first-mainline-charter-20260312.md`

## 设计目标

本轮只冻结三件正式事实：

1. `alpha formal signal` 是 `alpha` 给下游的正式事实层，不再让 `position` 直接读 `alpha` 内部临时过程。
2. `alpha` 在新仓的最小正式出口先收敛为三张表：
   - `alpha_formal_signal_run`
   - `alpha_formal_signal_event`
   - `alpha_formal_signal_run_event`
3. `alpha` 需要一个最小 producer runner，把官方上游事实按 bounded 方式物化为正式 `formal signal`，并留下 run 审计与 bounded evidence。

## 裁决一：`trigger ledger` 与 `formal signal` 必须继续彻底分层

新仓沿袭老仓已验证结论：

1. `trigger ledger` 回答“它什么时候发生了”。
2. `formal signal` 回答“在当前正式准入口径下，它是否成立为可被下游消费的信号”。
3. `position` 只能消费 `formal signal`，不能直接消费 `trigger ledger`、detector payload、research sidecar 或 `alpha` 内部候选表。

因此，`alpha formal signal` 在新仓中的身份固定为：

`alpha` 对下游 `position / portfolio_plan / trade / system` 的官方冻结输出层。

## 裁决二：本轮先冻结正式出口，不在同一张卡里把 `alpha` 全家表族一次做完

`alpha` 后续当然还会继续补更完整的内部表族，但当前最自然的下一锤不是把 `PAS` 五表族一次性全落完，而是先把下游真的在消费的出口层收口。

所以本轮明确只先冻结：

1. `alpha_formal_signal_event`
   - 正式历史事实层
2. `alpha_formal_signal_run`
   - run 级审计层
3. `alpha_formal_signal_run_event`
   - run 到事实的桥接层

本轮不同时宣称下面这些内容已经正式完成：

1. `alpha` 全部内部家族表全部落库
2. `structure / filter` 已经完成正式表合同
3. `alpha-position` 全链路历史主线已经全窗打通

## 裁决三：`alpha_formal_signal_event` 必须优先对齐 `position` 已冻结消费合同

`position` 这边已经有正式 bridge contract 与 runner，所以 `alpha` 新出口不能再自说自话。

`alpha_formal_signal_event` 的最小字段组必须优先稳定对齐当前消费侧合同，至少覆盖：

1. `signal_nk`
2. `instrument`
3. `signal_date`
4. `asof_date`
5. `trigger_family`
6. `trigger_type`
7. `pattern_code`
8. `formal_signal_status`
9. `trigger_admissible`
10. `malf_context_4`
11. `lifecycle_rank_high`
12. `lifecycle_rank_total`
13. `source_trigger_event_nk`
14. `signal_contract_version`

换句话说，`position` 之前先围绕消费侧冻结出的最小字段组，现在反过来成为 `alpha` 正式出口的最小硬合同。

## 裁决四：`run_id` 只承担审计职责，历史主语义由 `signal_nk` 与事件事实承担

本轮继续服从历史账本原则：

1. `alpha_formal_signal_event` 是历史事实层，不允许把 `run_id` 当主语义。
2. `alpha_formal_signal_run` 只记录一次 producer 运行的范围、来源、版本与摘要。
3. `alpha_formal_signal_run_event` 只承担“本次 run 触达了哪些事实”的桥接职责。

因此正式口径固定为：

1. 事实按 `signal_nk` 累积
2. 审计按 `run_id` 追溯
3. 同一事实允许被后续 run 复物化，但不得把 run 语义反向顶替事实语义

## 裁决五：`alpha` 默认优先消费官方 `filter / structure snapshot`

`11` 已经冻结 `structure_snapshot / filter_snapshot` 的最小官方合同，因此当前正式口径进一步固定为：

1. `alpha` 默认从官方 `alpha trigger + filter_snapshot + structure_snapshot` 做 bounded 读取。
2. `filter` 负责 pre-trigger 准入，`structure` 负责结构事实，`alpha` 不再默认回读旧 `malf` 兼容准入字段。
3. `fallback_context_table` 默认关闭；旧 `pas_context_snapshot` 一类输入只允许显式兼容启用，不再是默认上游。

## 裁决六：最小 producer runner 只解决“正式落库”，不顺手吞并下游桥接

本轮 producer runner 的职责固定为：

1. bounded 读取 `alpha` 官方上游事实
2. 物化 `alpha_formal_signal_event`
3. 记录 `alpha_formal_signal_run / alpha_formal_signal_run_event`
4. 产出 bounded summary 与 evidence

本轮 producer runner 不负责：

1. 自动调用 `position` runner
2. 自动写 `trade / system`
3. 宣称已经完成 full-history backfill
4. 继续依赖合同兼容表充当长期上游

## 模块边界

### 范围内

1. `alpha` 正式 `formal signal` 出口身份
2. `alpha_formal_signal_run / alpha_formal_signal_event / alpha_formal_signal_run_event` 三表合同
3. 最小 producer runner 的职责、输入、输出与 bounded evidence 边界

### 范围外

1. `structure / filter` 全部正式表族
2. `position` 新增更多 funding/exit family 表
3. `trade / system` 正式开工
4. `alpha` 全历史 backfill 与全市场重算节奏

## 一句话收口

`alpha` 在新仓的下一步不是继续让下游围着兼容合同打转，而是先把 `formal signal` 正式出口单独收口成历史事实层、run 审计层和最小 producer runner；这样 `position` 才能真正接上新仓官方上游。
