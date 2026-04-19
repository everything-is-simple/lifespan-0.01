# mainline real data smoke regression 结论

结论编号：`104`
日期：`2026-04-11`
状态：`草稿`

## 预设裁决

- 接受：
  当真实 bounded smoke 明确承接 `85` 之后的新官方 upstream，并验证 `alpha -> position -> trade -> system` 四层都只使用正式账本时接受。
- 拒绝：
  如果 smoke 仍沿用旧 upstream 假设，或 `position` 桥接、`trade` 只读消费、`system` 正式 readout 任何一层无法通过，则拒绝。

## 预设原因

1. `104` 是新框架下第一次端到端验收卡，不只是“链条跑通”的最小烟雾测试。
2. 如果不在真实 smoke 中验证 `position` 桥接和 `trade` 正式推进，`105` 的 orchestration acceptance 会建立在未证实的前提上。
3. `85` 已经冻结 upstream 主权边界，`104` 必须验证下游是否真的承接了那条官方链。

## 预设影响

1. `105` 可以把 `104` 结果作为正式 orchestration acceptance 输入，而不是概念性前提。
2. `system` 将首次在真实 bounded 主线中验证其 readout 是否只读消费正式账本。
3. `100-105` 卡组的分层语义会在一次端到端运行里被整体验收。

## 结论结构图

```mermaid
flowchart LR
    U["85 后官方 upstream"] --> A["alpha -> position -> trade -> system smoke"]
    A --> V["position 桥接 / trade 只读 / system readout"]
    V --> N["放行 105"]
```


