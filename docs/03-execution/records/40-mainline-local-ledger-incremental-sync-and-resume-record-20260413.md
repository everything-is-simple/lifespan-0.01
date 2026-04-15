# 主线本地账本增量同步与断点续跑记录

记录编号：`40`
日期：`2026-04-13`

## 实施记录

1. 新增 `40` 独立 runner
   - 在 `data` 模块内部新增控制账本，不改 `39` 已冻结的 `10` 个官方 ledger 名称
   - 控制账本包含 `run / checkpoint / dirty_queue / freshness_readout`
2. 冻结增量同步语义
   - 默认 source 未显式指定时，按官方 target 原地观察并补齐 checkpoint
   - 显式外部 source 时，按文件复制同步到官方 target，再补齐 schema
   - replay 只覆盖 `tail_start_bar_dt / tail_confirm_until_dt`，不把 run 元数据误写成业务真值
3. 冻结 freshness audit 口径
   - 每个官方 ledger 只读取正式业务表与业务日期列
   - 不使用 `run.completed_at`、`updated_at` 之类的审计时间戳判断数据新鲜度
4. 新增正式 CLI 与回归测试
   - CLI 接受 `source-ledger / source-latest-bar-date / replay-start-date / replay-confirm-until-date`
   - 单测覆盖原地续跑、外部复制同步和显式 replay

## 边界

- `40` 只负责官方 ledger 的标准化续跑治理，不负责各模块内部业务事实重算。
- `100-105` 现在可以恢复开工，但不得反向改写 `40` 已冻结的 data-grade checkpoint / freshness 语义。
