# downstream truthfulness revalidation after malf canonicalization 记录

记录编号：`32`
日期：`2026-04-11`
状态：`已补记录`

## 做了什么?

1. 跑了 `doc-first gating` 检查和 canonical downstream 的 bounded regression。
2. 复核 canonical `malf v2 -> structure -> filter -> alpha` 已经是默认主链，bridge-v1 只保留兼容回退。
3. 把 `32` 的卡面、证据、记录和结论补齐，并同步把执行总账的下一卡指针推进到 `100`。

## 偏离项

- 无功能偏离。
- `pytest` 仍然报告 `cache_dir` 是未知配置项，但不影响结果。

## 备注

- 本次不新增代码实现，只做正式 revalidation 收口。
