# malf 模块多级别极值推进账本设计宪章

日期：`2026-04-11`
状态：`已被 03 收缩替代`

> 本章保留为 `2026-04-11` 首轮扩展版历史记录。
> 当前 `malf` 正式核心请改读 `03-malf-pure-semantic-structure-ledger-charter-20260411.md`。

## 问题

当前仓内 `malf` 已经有一版最小桥接实现，但它仍然主要回答：

1. 当前更像 `BULL_MAINSTREAM / BEAR_MAINSTREAM / RANGE_BALANCED / RECOVERY_MAINSTREAM` 的哪一种
2. 当前是否出现 `new_high / new_low / failed_extreme` 一类候选事实

这版桥接足以支撑 `structure` 起跑，但它不是 `malf` 的核心定义。

`malf` 的核心不是市场语义标签，而是：

`只从 price bar 出发，对月/周/日各自的 HH / HL / LL / LH、结构破坏、极值推进、同级别历史分布与可执行动作接口进行正式记账。`

## 设计输入

1. `docs/01-design/modules/malf/00-malf-module-lessons-20260409.md`
2. `docs/01-design/modules/malf/01-market-base-to-malf-minimal-snapshot-bridge-charter-20260410.md`
3. `G:\《股市浮沉二十载》\2012.(Japan)【立花义正】\你也能成为股票操作高手（立花义正）PDF转图片版\你也能成为股票操作高手（立花义正）B0288.jpg`
4. `G:\《股市浮沉二十载》\2012.(Japan)【立花义正】\你也能成为股票操作高手（立花义正）PDF转图片版\你也能成为股票操作高手（立花义正）B0294.jpg`
5. `G:\《股市浮沉二十载》\2012.(Japan)【立花义正】\丽花义正-交易谱（1975.01-1976.12）\你也能成为股票操作高手立花义正-Pioneer交易记录19751976.xlsx`
6. `G:\《股市浮沉二十载》\2011.专业投机原理\专业投机原理_ocr_results\专业投机原理_20260321_105102.md`
7. `G:\《股市浮沉二十载》\2011.专业投机原理\专业投机原理_ocr_results\page_160_img-54.jpeg.png`
8. `G:\《股市浮沉二十载》\2011.专业投机原理\专业投机原理_ocr_results\page_345_img-57.jpeg.png`

## 裁决

### 裁决一：`malf` 的正式定义回到结构账本，而不是标签系统

`malf` 对外的正式身份冻结为：

`多级别极值推进账本 + 执行账本接口`

它首先回答结构，再回答概率，最后才回答动作。

### 裁决二：同级别统计是核心，不得跨生命周期混算

月、周、日分别是三套独立世界。

1. 月线统计月线的推进、回摆、反转
2. 周线统计周线的推进、回摆、反转
3. 日线统计日线的推进、回摆、反转

不得把日线样本混入周线分布，也不得把周线样本混入月线分布。

一句话冻结：

`蚂蚁的一生与古树的一生不可共用同一把尺。`

### 裁决三：硬规则只来自结构，软信息才来自统计

硬规则只由下列事实驱动：

1. `HH / HL / LL / LH`
2. 最后一个有效 `HL` 是否被击破
3. 最后一个有效 `LH` 是否被上破
4. 当前波内部是否继续创出新极值

软信息只用于调节经验概率，不用于替代结构判断：

1. `三顺二逆`
2. 波段幅度分位
3. 波段持续期分位
4. 当前推进位于同级别分布的第几四分位

### 裁决四：`HH(n+m)` 是正式一等公民

一旦突破上一波涨势或跌势的旧纪录，必须从破纪录的那根 bar 重新建立波段账本。

之后每再创一次同方向新极值：

1. 上升波 `hh_count += 1`
2. 下降波 `ll_count += 1`

这不是“最近 N 日新高次数”，而是“当前这波内部的极值推进累计次数”。

### 裁决五：`malf` 分为三层

`malf` 正式冻结为三层：

1. `structure ledger`
2. `same-level stats ledger`
3. `execution interface`

其中：

1. `structure ledger` 负责事实
2. `same-level stats ledger` 负责分布
3. `execution interface` 负责把结构事实翻译为测试单、母单、加码、减码、锁单、平仓、休息等动作约束

### 裁决六：现有 bridge v1 不废弃，但降级为兼容层

当前仓内既有：

1. `pas_context_snapshot`
2. `structure_candidate_snapshot`

它们继续有效，但正式身份调整为：

`canonical malf ledger` 的兼容输出与下游过渡视图

新系统不再把它们视为 `malf` 的终局定义。

### 裁决七：月周日联动是约束关系，不是替代关系

月线不替周线做判断，周线不替日线做判断。

联动只体现为：

1. 高级别定义背景
2. 中级别定义当前主波段
3. 低级别定义执行时机

低级别可以逆高级别，但必须单独记账，不得偷换为高级别已经反转。

## 模块边界

### 范围内

1. 月/周/日三个级别的 `pivot / wave / state / extreme_progress`
2. 同级别历史分布统计
3. 对 `structure / filter / alpha` 的正式消费接口
4. 与现有 `pas_context_snapshot / structure_candidate_snapshot` 的兼容映射

### 范围外

1. 具体仓位大小
2. 资金曲线管理
3. 订单路由与成交回报
4. 执行层未复权定价

## 一句话收口

`malf` 的正式核心不是“市场语义分类”，而是“把每个级别自己的生命过程用 HH/HL/LL/LH、break、极值累计和同级别统计完整记成账本，并向执行层提供动作接口”。

## 流程图

```mermaid
flowchart LR
    MB[market_base backward] --> MALF[malf D/W/M 独立计算]
    MALF --> PL[pivot_ledger]
    MALF --> WL[wave_ledger]
    MALF --> EL[extreme_progress_ledger]
    MALF --> SS[state_snapshot]
    MALF --> SLS[same_level_stats]
