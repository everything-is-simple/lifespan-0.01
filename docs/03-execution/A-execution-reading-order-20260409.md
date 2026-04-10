# 执行阅读顺序

日期：`2026-04-09`
状态：`生效中`

当前默认顺序：
1. 先读 `00-conclusion-catalog-20260409.md`
2. 再读 `B-card-catalog-20260409.md`
3. 再读 `C-system-completion-ledger-20260409.md`
4. 先看最新已生效结论 `17-raw-base-strong-checkpoint-and-dirty-materialization-conclusion-20260410.md`
5. 如果要理解第二阶段日更方向，先看 `18-daily-raw-base-fq-incremental-update-source-selection-conclusion-20260410.md`
6. 如果要继续正式施工，打开当前实现卡 `19-tdxquant-daily-raw-source-ledger-bridge-card-20260410.md`

当前活动主线：
1. `data -> raw_market -> market_base -> malf -> structure` 最小官方前半段主线已成立。
2. `market_base` 已冻结 `none / backward / forward` 三套价格，并正式分开“信号后复权 / 执行不复权”口径。
3. 当前最新已生效结论锚点已推进到 `17`，执行区最新正式口径已包含 `raw/base` 的强断点、dirty queue、run/file ledger 与库级约束。
4. 当前待施工卡已切到 `19`；卡 `18` 负责方案选型，卡 `19` 负责把 `TdxQuant` 日更原始事实正式桥接进现有 `raw/base` 账本机制。在新结论生效前，正式入口仍保持卡 `17` 口径。
