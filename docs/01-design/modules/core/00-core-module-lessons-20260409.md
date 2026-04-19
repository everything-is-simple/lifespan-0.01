# core 模块经验冻结

日期：`2026-04-09`
状态：`生效中`

## 当前职责

- 统一五根目录路径、正式数据库路径和环境覆盖入口
- 提供 ownership、checkpoint、resume、incremental 等底座能力
- 为其他模块提供正式系统治理边界

## 必守边界

1. 所有正式路径必须经过 `default_settings()` 与冻结 dataclass 解析，不允许脚本散写路径。
2. ownership manifest 必须机器可读，不能只靠口头约定谁能读谁的表。
3. checkpoint 必须带 fingerprint；参数变了就不能直接续跑旧产物。

## 已验证坑点

1. 路径写死会在换机、换根目录、pytest 重定向时整体崩掉。
2. 没有 fingerprint 的 checkpoint 会把新参数结果写进旧批次语义里。
3. owner / reader 边界不清时，下游会直接读上游内部表，最后把模块边界读穿。

## 新系统施工前提

1. 新 runner 先接 resumable 基础设施，再谈批量和增量。
2. 新正式表先登记 ownership，再开放下游消费。
3. `Validated` 必须被当成正式根目录，而不是附带备份区。

## 来源

1. 老系统总表 `battle-tested-lessons-all-modules-and-mainline-bridging-20260408.md`
2. 老系统 `core` 章程与合同集中化文档

## 流程图

```mermaid
flowchart LR
    CFG[default_settings 冻结路径] --> OWN[ownership manifest]
    OWN --> CP[checkpoint + fingerprint]
    CP --> RESUME[断点续跑]
    RESUME --> DS[各模块 runner]
```
