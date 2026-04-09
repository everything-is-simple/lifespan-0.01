# 历史账本共享契约与pytest路径修正 结论

结论编号：`02`
日期：`2026-04-09`
状态：`生效中`

## 裁决

- 接受：`pytest` 临时目录定位已固定到 `H:\Lifespan-temp`，历史账本共享契约已进入正式 design/spec。
- 拒绝：无。

## 原因

1. `pyproject.toml` 已不再依赖相对路径推导 `pytest` 临时目录。
2. 从仓库根目录和 `tests/` 子目录运行 `pytest` 都能通过，并把临时产物写到 `H:\Lifespan-temp`。
3. “历史账本共享契约”已经明确自然键优先、审计字段分离、账本与快照分层、五根目录来源优先级和 `trade -> trade_runtime` 命名映射。

## 影响

1. 后续 `position / alpha / portfolio_plan / trade` 新增正式表时，有了统一的共享账本门槛。
2. `pytest` 临时目录不再因为运行目录不同而漂移。
3. 下一步可以在这份共享契约之上实现真正的 `doc-first gating` 检查器。
