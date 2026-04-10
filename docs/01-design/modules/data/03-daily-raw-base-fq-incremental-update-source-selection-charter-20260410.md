# data 模块 raw/base 每日复权增量更新方案选型章程

日期：`2026-04-10`
状态：`生效中`

## 问题

卡 `17` 已把当前正式 `raw/base` 口径冻结为：

`文本导出 -> raw_market -> market_base`

并补齐了：

1. 批量更新
2. 强断点
3. 断点续跑
4. 脏标的增量物化

但这套口径仍有一个现实成本：

1. 日常更新依赖人工或半人工导出文本文件。
2. `txt` 文件本身承载的信息有限，日更链路不够“无感”。
3. 如果未来希望把 `raw/base` 升级为“收盘后自动联动更新”，当前正式源头仍偏重首建库和大批量重建。

因此需要专门开一张选型卡，研究“第二阶段每日联动更新”到底该落在哪一种正式源头之上。

## 设计输入

1. `docs/01-design/modules/data/01-tdx-offline-raw-and-market-base-bridge-charter-20260410.md`
2. `docs/01-design/modules/data/02-raw-base-strong-checkpoint-and-dirty-materialization-charter-20260410.md`
3. `docs/02-spec/modules/data/01-tdx-offline-raw-and-market-base-bridge-spec-20260410.md`
4. `docs/02-spec/modules/data/02-raw-base-strong-checkpoint-and-dirty-materialization-spec-20260410.md`
5. `docs/03-execution/17-raw-base-strong-checkpoint-and-dirty-materialization-conclusion-20260410.md`
6. 用户提供的社区方案参考：
   - `https://xueqiu.com/1834496434/381550637`
   - `https://zhuanlan.zhihu.com/p/1999045057962075099`
   - `https://www.zhihu.com/question/40287660/answer/1997621496328259314`
7. 官方方案参考：
   - `https://help.tdx.com.cn/quant/docs/markdown/mindoc-1cfsjkbf8f3is/`
   - `https://help.tdx.com.cn/quant/docs/markdown/mindoc-1ctuhthaq5qmg/`
   - `https://help.tdx.com.cn/quant/docs/markdown/Dict.html`

## 裁决

### 裁决一：卡 `18` 是正式方案选型卡，不是直接替换卡 `17`

在卡 `18` 收口之前，当前正式有效的 `raw/base` 路径仍然是：

`离线 txt -> raw_market -> market_base`

不能因为开始研究新方案，就把卡 `17` 已生效的正式入口降级为“临时方案”。

### 裁决二：本轮必须并列研究两种候选源头

卡 `18` 的研究对象冻结为两类：

1. 候选 A：`vipdoc/*.day` 本地二进制直读，必要时辅以 `mootdx` 补缺
2. 候选 B：通达信官方 `TdxQuant / tqcenter` 数据接口

不允许只因为某一路线实现更快，就跳过另一条路线的正式比较。

### 裁决三：方案优先级按“正式账本适配度”排序，而不是按单次速度排序

本仓库要解决的不是一次性把数据读出来，而是要把数据沉淀成长期可续跑、可复算、可审计的历史账本。

因此本轮优先级固定为：

1. 复权口径可控且可审计
2. 每日增量可以稳定断点续跑
3. 能与 `raw_market / market_base` 账本契约对接
4. 市场覆盖完整
5. 自动化成本低
6. 吞吐和速度足够高

### 裁决四：如果依赖第三方非官方补丁，必须先降级为“候选风险”而不是“正式方案”

社区方案里提到对 `mootdx` 的 `site-packages` 原地魔改。

这类做法在本仓库内不能直接成为正式口径；如果候选 A 需要依赖类似处理，后续必须改写成：

1. 仓库内可版本化的兼容层
2. 可验证、可回放、可重建的环境步骤

否则最多只能作为实验通路，不能直接进入正式主链。

### 裁决五：官方 `TdxQuant` 的“自带复权”必须接受账本约束复核

官方资料已经明确：

1. 运行前需先启动支持 TQ 的通达信终端
2. 提供 `none / front / back` 复权类型
3. 支持 `.BJ`、ETF 等市场和品种

但这些能力是否适合作为本仓库 `raw/base` 的正式源头，仍要额外复核：

1. 是否能形成可追踪的 daily ingest ledger
2. 是否能稳定落到 `raw_market` 历史账本，而不是只能拿临时 DataFrame
3. 是否支持断点续跑与 bounded replay

### 裁决六：卡 `18` 的目标是“选源头 + 定合同”，不是顺手重写整个 `data` 子系统

本轮的正式产出应先回答：

1. 哪条路线更适合作为 `raw/base` 第二阶段每日联动更新源头
2. 另一条路线为什么不适合作为主路径，或只适合作为 sidecar / fallback
3. 如果要落地，需要新增哪些 `raw` 侧 source ledger、指纹、调度和失败恢复合同

不在这张卡里顺手推进 `malf`、下游模块或 corporate action 总账。

## 评估维度

1. `复权真实性`：能否稳定支持 `none / backward / forward` 或提供足够信息复算
2. `日更自动化`：能否在收盘后自动运行，不依赖手工导出
3. `断点续跑`：中断后能否只补未完成范围
4. `增量粒度`：能否只处理新增交易日或新增标的
5. `市场覆盖`：A 股、北交所、ETF 是否都可进入正式合同
6. `运行依赖`：是否依赖 GUI、在线节点、第三方补丁、特定本机状态
7. `账本适配`：能否自然沉淀为 `raw_market` 与 `market_base` 的 run ledger
8. `可运维性`：安装、升级、复现、审计成本是否可接受

## 模块边界

### 范围内

1. `raw/base` 第二阶段每日联动更新源头选型
2. 候选 A / 候选 B 的正式比较标准
3. 候选方案的 bounded 研究与风险登记
4. 对现有 `raw/base` 正式账本合同的影响评估

### 范围外

1. 直接替换卡 `17` 已生效入口
2. `malf`、`structure`、`filter` 等下游改造
3. 企业行为总账的完整设计
4. 分钟级、tick 级正式历史账本

## 一句话收口

卡 `18` 的任务不是追求“更快读数据”，而是用正式账本标准比较 `.day + 补源` 与 `TdxQuant` 两条路线，选出更适合作为 `raw/base` 第二阶段每日复权增量更新源头的正式方案。
