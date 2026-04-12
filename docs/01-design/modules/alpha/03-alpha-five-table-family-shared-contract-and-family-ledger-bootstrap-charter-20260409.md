# alpha 五表族共享合同与 family ledger bootstrap 设计章程
日期：`2026-04-09`
状态：`生效中`

## 问题

`12` 已经把 `alpha trigger ledger` 的共享最小正式账本层立住了，也证明了 `inserted / reused / rematerialized` 能在 `H:\Lifespan-data` 里成为正式运行能力。

但当前 `alpha` 里仍然空着的一层，是 `bof / tst / pb / cpb / bpb` 五表族自己的 family-specific 正式沉淀层。

如果这里继续停在“只有共享 trigger 事实，没有 family ledger”的状态，会留下三个长期问题：

1. `alpha` 虽然已经能回答“触发发生过”，但还不能稳定回答“这个 family 自己的最小 payload 是什么”。
2. `formal signal` 虽然已与官方 trigger 事实分层，但五表族解释层仍偏薄，后续 `trade / system` 看不到更细、可审计的 family 侧语义。
3. 主线会再次被误导到回头扩 `position`，或过早跳到 `portfolio_plan / trade / system`，而不是继续把 `alpha` 遗产里最值得保住的部分正式账本化。

## 设计输入

本章程建立在下面这些已冻结来源之上：

1. `docs/01-design/modules/alpha/00-alpha-module-lessons-20260409.md`
2. `docs/01-design/modules/alpha/01-alpha-formal-signal-output-charter-20260409.md`
3. `docs/01-design/modules/alpha/02-alpha-trigger-ledger-and-five-table-family-minimal-materialization-charter-20260409.md`
4. `docs/02-spec/Ω-system-delivery-roadmap-20260409.md`
5. `docs/02-spec/modules/alpha/01-alpha-formal-signal-output-and-producer-spec-20260409.md`
6. `docs/02-spec/modules/alpha/02-alpha-trigger-ledger-and-five-table-family-minimal-materialization-spec-20260409.md`
7. `docs/03-execution/12-alpha-trigger-ledger-and-five-table-family-minimal-materialization-conclusion-20260409.md`
8. `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\04-pas-five-trigger-ledger-and-incremental-materialization-reset-20260408.md`
9. `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\05-pas-full-market-five-trigger-ledger-backfill-reset-20260408.md`
10. `G:\MarketLifespan-Quant\docs\01-design\modules\alpha\06-pas-code-ledger-reset-and-2010-pilot-20260408.md`
11. `G:\EmotionQuant-gamma\gene\03-execution\09-phase-g6-bof-pb-cpb-conditioning-card-20260316.md`
12. `G:\EmotionQuant-gamma\gene\03-execution\16-phase-gx5-two-b-window-semantics-refactor-card-20260317.md`
13. `G:\EmotionQuant-gamma\gene\03-execution\17-phase-gx6-123-three-condition-refactor-card-20260317.md`

## 设计目标

本轮只冻结一件事：

把 `alpha trigger ledger` 之上的 family-specific 最小正式账本层往前推进一步，让五表族从“共享 trigger 事实”继续过渡到“共享合同 + family ledger bootstrap”，并先在一到两个最核心 family 上证明 schema、rerun 与 bounded materialization 可以成立。

## 裁决一：下一锤继续留在 `alpha`，不回头扩 `position`，也不提前跳下游

当前最自然的下一张正式主线卡不是：

1. 回头再扩 `position`
2. 直接开 `portfolio_plan`
3. 直接开 `trade / system`

而是继续把 `alpha` 内部正式中间账本补厚。

原因是：

1. `position` 当前已经能稳定消费官方 `alpha formal signal`
2. `portfolio_plan / trade / system` 还没有正式 runner 与账本入口
3. 真正还薄的，是 `alpha` 五表族自己的 family-specific 正式解释层

## 裁决二：本轮先冻结“共享合同到 family ledger”的桥接层，不宣称五表族已全部最终定型

旧仓里的 `bof / tst / pb / cpb / bpb` 是 `alpha` 最值得继承的已验证遗产，但新仓当前最稳的推进方式不是一次性把五个 family 的完整专表全部做满，而是先把它们共享、稳定、最常复用的一层 family ledger 合同冻住。

本轮先冻结：

1. family ledger 的共享自然键规则
2. family ledger 与 `alpha_trigger_event` 的官方关联
3. family payload 最小公共字段组
4. 一到两个 family 的最小正式落库示范

本轮不宣称：

1. 五个 family 的全部 detector 已全部重构完成
2. 五个 family 都已拥有完全独立、最终形态的专表族
3. `alpha` 内部全部 trace / observation / sidecar 都已经正式化

## 裁决三：family ledger 必须继续站在官方 `trigger ledger` 之上，而不是绕过共享事实层

新仓继续服从已经收口的分层：

1. `alpha_trigger_event` 回答“触发是否发生”
2. family ledger 回答“这个 family 对该触发的最小正式解释与 payload 是什么”
3. `alpha_formal_signal_event` 继续回答“是否成为可被下游消费的正式信号”

因此：

1. family ledger 必须官方引用 `alpha_trigger_event.trigger_event_nk`
2. 不允许直接让 family ledger 重新替代共享 trigger 事实层
3. 下游仍不得绕过 `formal signal` 直接把 family ledger 当交易主语义

## 裁决四：本轮允许先落一到两个 family，先证明 family ledger 合同成立

五表族方向已经明确，但当前最合理的施工方式是：

1. 先把共享合同冻结
2. 先挑一到两个最核心、历史上验证较充分的 family 做最小正式 ledger bootstrap
3. 在 bounded pilot 中证明 family ledger 也能支持 `inserted / reused / rematerialized`

是否一次性覆盖全部五个 family，不在这张卡里写死。

## 裁决五：family ledger 仍必须写入 `H:\Lifespan-data`，不回退到 temp-only

本轮关键目标不是“多一层 smoke 表”，而是：

1. family ledger 必须作为 `alpha.duckdb` 中的新正式账本层落入 `H:\Lifespan-data`
2. `H:\Lifespan-temp` 只保存 summary、临时导出与调试痕迹
3. bounded pilot 完成后，正式库应能 readout 到 family 级落表事实与按动作分布的审计记录

## 裁决六：本轮优先解决 family ledger bootstrap，不顺手吞并下游职责

本轮范围固定为：

1. 五表族共享 contract
2. family ledger 最小表族
3. 一到两个 family 的 bounded bootstrap
4. rerun / reuse / rematerialize 验证

本轮不负责：

1. 回头改写 `position`
2. 正式开工 `portfolio_plan / trade / system`
3. full-market 全历史 family backfill
4. 把所有 family-specific payload 一次性做成最终形态

## 模块边界

### 范围内

1. 五表族共享合同与自然键
2. family ledger 最小正式表族
3. family ledger 与 `alpha_trigger_event` 的对接关系
4. bounded runner / pilot / rerun 审计

### 范围外

1. `position` 新增 family 读口
2. `portfolio_plan / trade / system` 正式 runner
3. 五表族所有专表一次性补齐
4. full-history 一次性正式回填

## 一句话收口

`13` 号卡要做的不是改写下游，也不是把五表族一口气做完，而是先把 `alpha` 已经共享下来的 trigger 事实继续推进成“共享合同 + family ledger bootstrap”的最小正式历史账本层。

## 流程图

```mermaid
flowchart LR
    TRG[alpha_trigger_event] --> FAM[family ledger bof/tst/pb/cpb/bpb]
    FAM --> BOOT[family ledger bootstrap bounded pilot]
    BOOT --> SIG[alpha_formal_signal_event]
    SIG --> POS[position downstream]
