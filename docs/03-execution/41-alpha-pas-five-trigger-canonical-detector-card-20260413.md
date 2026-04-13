# 41-alpha PAS 五触发 canonical detector 施工卡
日期：`2026-04-13`
状态：`施工中`

## 需求

- 问题：
  当前主线已经完成 `data -> malf -> structure -> filter` 的 canonical 收口，也已经具备 `alpha_trigger_event -> alpha_family_event -> alpha_formal_signal_event` 的最小账本骨架，但官方 `PAS detector` 仍然缺席。`alpha_trigger_candidate` 目前没有 canonical 生产者，五触发只能靠测试或手工注入，无法证明它们来自新版 `malf` 清洗后的 `structure / filter`。
- 为什么现在做：
  `100` 要冻结 trade 使用的正式 signal anchor，而 anchor 的上游必须先有可信的 PAS 五触发生产者；如果继续跳过 detector，后续 `100-105` 会建立在半人工、半占位的 alpha 输入之上。
- 目标：
  在进入 `100` 之前，先把 `bof / tst / pb / cpb / bpb` 五触发按当前主线语义正式接回 `alpha`，使 `alpha_trigger_candidate` 成为官方 detector 输出，再继续沿用现有 `trigger / family / formal signal` 链路。
- 目标结果：
  官方主线能够从 canonical `filter / structure / market_base(backward)` 直接生成五触发候选，并把这些候选稳定送入 `trigger / family / formal signal` 三段账本。
- 成功标准：
  五触发能够在官方 `filter / structure / market_base(backward)` 上稳定生成候选，且 `trigger / family / formal signal` 不再依赖测试或手工注入候选。

## 设计输入

- 设计文档：
  - `docs/01-design/modules/alpha/04-alpha-pas-five-trigger-canonical-detector-charter-20260413.md`
- 规格文档：
  - `docs/02-spec/modules/alpha/04-alpha-pas-five-trigger-canonical-detector-spec-20260413.md`
- 现行结论：
  - `docs/03-execution/12-alpha-trigger-ledger-and-five-table-family-minimal-materialization-conclusion-20260409.md`
  - `docs/03-execution/13-alpha-five-table-family-shared-contract-and-family-ledger-bootstrap-conclusion-20260409.md`
  - `docs/03-execution/38-structure-filter-mainline-legacy-malf-semantic-purge-conclusion-20260413.md`
  - `docs/03-execution/40-mainline-local-ledger-incremental-sync-and-resume-conclusion-20260413.md`
- 参考实现：
  - `G:\。backups\MarketLifespan-Quant\src\mlq\alpha\pas\detectors_breakout.py`
  - `G:\。backups\MarketLifespan-Quant\src\mlq\alpha\pas\detectors_cpb.py`
  - `G:\。backups\EmotionQuant-gamma\src\backtest\pas_ablation.py`

## 任务分解

1. 为 `alpha PAS detector` 冻结官方表族、bootstrap 与 CLI 入口。
2. 重写五触发 detector，只读取官方 `filter_snapshot`、`structure_snapshot` 与 `market_base.stock_daily_adjusted(adjust_method='backward')`。
3. 让 detector 输出官方 `alpha_trigger_candidate`，同时具备 `run / work_queue / checkpoint / run_candidate` 审计链。
4. 对接现有 `run_alpha_trigger_build`，确保正式主线不再依赖手工候选输入。
5. 升级 `family_runner` 对 detector 扩展列的消费，开始形成 family-specific 解释层。
6. 补足五触发正反样本、queue/checkpoint 续跑与 `candidate -> trigger -> family -> formal signal` 贯通测试。
7. 回填 evidence / record / conclusion，并同步索引与入口文件。

## 实施范围

1. 新增 `alpha PAS detector` 官方表族与 bootstrap。
2. 新增 `scripts/alpha/run_alpha_pas_five_trigger_build.py`。
3. 新增 `src/mlq/alpha` 下的 detector runner / source / materialization / shared helper。
4. 升级 `trigger / family / formal signal` 对官方 detector 输出的消费。

## 非范围

1. `trade` 的 `signal_low / last_higher_low` 正式锚点。
2. `position` sizing 语义重写。
3. `101-105` 的执行与 system 收口。

## 历史账本约束

- 实体锚点：`asset_type + code`
- 业务自然键：
  `instrument + signal_date + asof_date + trigger_type + pattern_code + detector_contract_version`
- 批量建仓：
  支持按 `filter_snapshot` 与 `market_base(backward)` 一次性 bounded 回放全历史
- 增量更新：
  支持按 `filter_checkpoint` 每日增量推进
- 断点续跑：
  支持 `checkpoint / dirty queue / replay`
- 审计账本：
  `alpha_pas_trigger_run / alpha_pas_trigger_work_queue / alpha_pas_trigger_checkpoint / alpha_trigger_candidate / alpha_pas_trigger_run_candidate`

## 验收标准

1. 五触发可从官方主线上正式生成候选。
2. `trigger / family / formal signal` 不再依赖测试手工注入候选。
3. queue/checkpoint 续跑通过。
4. `python scripts/system/check_doc_first_gating_governance.py` 通过。
5. `python scripts/system/check_development_governance.py` 通过。
6. 相关单测通过并形成可追溯证据。
