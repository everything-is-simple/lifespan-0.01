# raw/base 每日复权增量更新方案选型 记录

记录编号：`18`
日期：`2026-04-10`

## 做了什么

1. 已为卡 `18` 建立 execution 四件套。
2. 已补 `03` 号 design/spec，冻结“每日复权增量更新方案选型”的正式问题定义与比较标准。
3. 已把当前待施工卡切换到 `18`，但当前最新已生效结论锚点仍保持在 `17`。
4. 已完成切片 1 的第一轮输入盘点：
   - 确认本机 `H:\new_tdx64\vipdoc`、北交所 `.day` 文件与 `T0002\blocknew` 存在
   - 确认当前 Python 环境已有 `mootdx` 与 `tdxpy`
   - 确认官方文档已公开 `TdxQuant` 的终端依赖、复权枚举、`.BJ` 与 ETF 支持
5. 已补一轮最小真实 probe：
   - 候选 A：直接解包 `bj430017.day` 成功，证明 `.day` 技术上可直接进入本地解析路径
   - 候选 B：`tqcenter.py` import 成功，但 `tq.get_market_data(...)` 在真实调用时返回 `TQ数据接口初始化失败`
6. 已进一步把候选 B 的失败点缩小：
   - 默认失败的直接原因是未先调用 `tq.initialize(path)`
   - 显式 `initialize(path='H:\lifespan-0.01')` 后，接口可进入初始化成功状态
   - 但初始化成功后的 `get_market_data / get_stock_info / get_stock_list` 结果仍出现空返回、字段忽略和参数语义异常，说明官方路线还不能直接视为“已可用于正式账本”
7. 已补第二轮真实 probe，把候选 B 的路径约束跑透：
   - 本机存在正在运行的 `G:\new_tdx64\TdxW.exe`
   - `tqcenter.py` 的 `initialize(...)` 会把 `path` 直接送进 DLL 做连接标识
   - 直接传目录字符串会出现“初始化成功但数据为空”的假阳性
   - 改为 `G:\new_tdx64\PYPlugins\user\*.py` 这类唯一策略路径后，`get_market_data(...)`、`get_stock_info(...)` 才稳定返回真实结果
8. 已补第二轮覆盖核验：
   - 候选 B 在正确初始化后，已真实读到 `000001.SZ`、`510300.SH`、`920021.BJ` 的 OHLCV 与基础信息
   - 这说明官方路线的市场覆盖在样本上确实触达沪深、北交所与 ETF
   - 但 `get_stock_list(market='0'/'1'/'2')` 语义仍异常，当前只有 `market='5'` 能返回看似完整的全市场列表
9. 已补候选 A 与候选 B 的新鲜度对照：
   - `vipdoc` 样本 `.day` 的最后记录统一停在 `2026-04-03`
   - 同期 `TdxQuant` 与正式 `raw_market` 已能读到 `2026-04-10`
   - 这说明候选 A 当前更像“可直读的本地文件源”，还不能直接证明自己满足“每日联动更新”目标
10. 已补复权口径对照：
   - `920021.BJ` 在 `2025-09-30` 的正式 `raw_market.backward.close = 10.64`
   - 同日 `raw_market.none.close = 6.84`
   - `TdxQuant(dividend_type='back').Close = 6.84`
   - 这说明官方“自带复权”在当前样本上尚未证明能直接替代现有 `raw backward` 口径
11. 已补账本实物限制登记：
   - 当前正式 `raw/base` 里尚无 `510300.SH`，说明现有正式入口还未覆盖 ETF
   - 当前 `market_base.stock_daily_adjusted` 实物里 `backward` 仅有 `1000` 行，无法直接拿它当作候选 B 的后复权真值面
12. 已补“已知除权样本集”的专项 probe：
   - 抽取 `000001.SZ / 600519.SH / 920021.BJ / 000538.SZ / 300033.SZ`
   - 用正式 `raw_market` 的 `none / forward / backward` 做逐日真值面
   - 用 `TdxQuant` 的 `none / front / back` 做同窗逐日对齐
13. 已确认 `front/back` 当前都不能直接映射到账本复权列：
   - `300033.SZ` 在 `2026-04-01 ~ 2026-04-09` 的正式 `raw_forward` 与 `raw_none` 差距很大
   - 但 `TdxQuant none/front/back` 三列全部等于 `raw_none`
   - `920021.BJ` 在 `2025-09-30` 和 `2026-04-07 ~ 2026-04-10` 也表现为 `TdxQuant none/front/back` 全部等于 `raw_none`
14. 已确认官方 `back` 结果存在窗口依赖：
   - `000001.SZ` 与 `600519.SH` 在 `count=5` 时，`TdxQuant(back) == TdxQuant(none)`
   - 改成 `count=120` 且显式 `end_time='20260410150000'` 后，`TdxQuant(back)` 会整体上移
   - 但上移后的价格仍远离正式 `raw_backward`
   - 这说明当前官方 `back` 至少还不是“同一结束日下与窗口长度无关”的稳定复权事实
15. 已补 `ForwardFactor` 探针：
   - `TdxQuant(none)` 的 `ForwardFactor` 为 `1.0`
   - `TdxQuant(front/back)` 的 `ForwardFactor` 为 `0.0`
   - 当前还不足以解释 `front/back` 与正式账本价格之间的差异

## 偏离项

- `new_execution_bundle.py` 在回填索引时因旧目录标题为“当前结论文件”而不是“当前正式结论”中断。
- 四件套文件已正常生成；索引改由手动回填，不影响卡 `18` 的正式开卡结果。
- `vipdoc` 抽样文件最后写入时间停留在 `2025-09-30`，相对当前日期 `2026-04-10` 明显偏旧。
  - 后续如果候选 A 要进入正式比较，必须单独验证本机通达信数据刷新机制是否可信。
- 候选 B 当前的主要阻塞点不是接口签名，而是运行时初始化。
  - 后续若继续 probe，必须先确认“终端是否已启动、客户端数据是否已就绪、Python 侧是否需要额外初始化步骤”。
- 候选 B 还表现出显式 run/path 约束。
  - 同一路径并发初始化会出现 `返回ID小于0`，这对后续批量 runner 的并发和断点设计是正式风险点。
- 本轮又发现一项对照限制：
  - 当前 `market_base` 实物里的 `backward` 数据量明显异常偏少。
  - 因此卡 `18` 现阶段不能把“`TdxQuant(back)` 不等于 `market_base(backward)`”直接写成官方路线错误，而应先按 `raw_market.backward` 做真值比对。
- 本轮还发现一项新的官方 quirk：
  - `TdxQuant(back)` 对同一标的、同一结束日期的返回结果会受 `count` 长度影响。
  - 这对“断点续跑后同窗重放应得到相同结果”的正式账本要求是直接风险。

## 备注

- 当前 `txt -> raw_market -> market_base` 正式入口继续保留。
- 卡 `18` 当前只做选型研究，不直接替换卡 `17` 已生效口径。
- 当前仓库还没有 `TdxQuant / mootdx / vipdoc` 的正式适配层；后续若要继续，只能按卡 `18` 的 bounded probe 逐步推进。
- 当前阶段性倾向已经收敛为：
  - 候选 A 更像“本地文件回放 / 审计 / 离线兜底”路线
  - 候选 B 更像“可能具备每日新鲜度的官方在线/终端联动路线”
  - 但两者都还没有完成可直接进入正式主链的最终裁决条件
- 当前卡 `18` 的复权子问题已经进一步收敛为：
  - 若后续要采用候选 B 做每日主源头，则复权更可能需要走“官方日更原始事实 + 仓内可审计复权物化”
  - 而不是直接把 `TdxQuant(front/back)` 当作正式 `market_base` 价格
