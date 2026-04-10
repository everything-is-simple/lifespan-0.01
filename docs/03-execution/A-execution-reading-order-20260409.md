# 执行阅读顺序

日期：`2026-04-09`
状态：`生效中`

当前默认顺序：
1. 先读 `00-conclusion-catalog-20260409.md`
2. 再读 `B-card-catalog-20260409.md`
3. 再读 `C-system-completion-ledger-20260409.md`
4. 先看最新已生效结论 `19-tdxquant-daily-raw-source-ledger-bridge-conclusion-20260410.md`
5. 如果要理解第二阶段日更方向，再看 `18-daily-raw-base-fq-incremental-update-source-selection-conclusion-20260410.md`
6. 如果要继续正式施工，打开当前待施工卡 `19-tdxquant-daily-raw-source-ledger-bridge-card-20260410.md`；在下一张卡生成前，它仍作为当前执行锚点保留

当前活动主线：
1. `data -> raw_market -> market_base -> malf -> structure` 最小官方前半段主线已成立。
2. `market_base` 已冻结 `none / backward / forward` 三套价格，并正式分开“信号后复权 / 执行不复权”口径。
3. 当前最新已生效结论锚点已推进到 `19`，执行区最新正式口径已包含 `TdxQuant(none)` 的 `run/request/checkpoint` 桥接、checkpoint replay 与 `none dirty_queue` 联动。
4. 当前主线卡已收口；但在下一张卡生成前，当前待施工卡仍暂留 `19` 作为执行锚点。后续 `data` 侧若继续推进，重点会转向 `TQ raw + txt fallback` 并存治理与仓内复权物化后续卡。
