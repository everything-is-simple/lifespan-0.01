# 主线本地账本标准化 bootstrap 记录

记录编号：`39`
日期：`2026-04-13`

## 实施记录

1. 复核五根目录与官方 DB 路径契约
   - 确认官方主线 DB 继续冻结为 `raw_market / market_base / malf / structure / filter / alpha / position / portfolio_plan / trade_runtime / system`
2. 新增标准化 bootstrap runner
   - 用统一 registry 绑定 `ledger_name -> official target path -> bootstrap function`
   - 迁移只接受显式 `source_ledger_paths`，避免靠猜目录误迁库
3. 增加迁移审计摘要
   - 迁移后输出 JSON / Markdown 报告
   - 记录 inventory、source、action、table_count、row_count 摘要
4. 完成一条正式迁移演练
   - 以 `structure` 账本为样本，从 legacy workspace 复制到 official workspace
   - 验证 `structure_run` 事实成功进入官方标准库

## 边界

- 本卡只收口一次性标准化 bootstrap，不触碰每日增量与断点续跑自动化
- `40` 将继续补齐 `checkpoint / dirty queue / replay / freshness audit`
- 迁移策略当前默认走显式源路径，不做隐式自动发现
