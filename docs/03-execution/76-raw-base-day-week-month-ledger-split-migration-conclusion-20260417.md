# raw/base 日周月分库迁移结论

结论编号：`76`
日期：`2026-04-17`
状态：`草稿`

## 裁决

- 接受：
  暂未接受，待 `76` 的六库迁移、真实库重建和旧 day 库周月清理全部完成后再正式裁决。
- 拒绝：
  拒绝继续把 `75` 的单库周月 txt 回补路径当作长期官方执行方案。

## 原因

- 真实官方库已经证明旧单库方案在 stock 规模下无法稳定收口：`stock week raw` 半成品、`stock week base` 和 `stock month raw/base` 均未完成。
- 现有离线源只有 day txt，没有任何 week/month txt；旧方案继续跑下去，只会反复从 txt 重扫周月并与 day 官方库争锁。

## 影响

- 当前正式待施工卡已切到 `76`，`80-86` 暂缓，等待 `data` 层六库分时框迁移完成后再恢复。
- 后续 week/month 的官方来源被重定义为“从 day 官方库派生”，不再是“从 txt 回退聚合写入同一物理库”。

## 结论结构图

```mermaid
flowchart TD
    A[盘点真实官方库] --> B[确认旧单库周月路径失效]
    B --> C[76 迁移卡成为当前施工位]
    C --> D[待六库迁移完成后正式裁决]
```
## 2026-04-17 第一刀实现状态

- 已完成：
  - 六库路径契约已正式落到代码
  - day 兼容别名仍保留
  - raw/base 的 timeframe-aware bootstrap 入口已落地
  - `week/month` 独立库已经可以被正式 bootstrap 出来
- 尚未完成：
  - runner 仍主要落在旧 day 官方库
  - `day raw -> week/month raw` 派生链路尚未实现
  - 真实 `stock week/month` rebuild 与旧 day 库 purge 还未开始

## 当前裁决补充

- `76` 现在不再只是设计草稿，而是已经进入“代码第一刀落地、迁移未完成”的进行中状态。
- 下一刀应直接进入 runner 路由改造，而不是继续在旧单库方案上补跑 `stock week/month`。

## 本次验证

```text
pytest tests/unit/core/test_paths.py tests/unit/data/test_timeframe_ledger_bootstrap.py tests/unit/data/test_raw_ingest_runner.py -q
python scripts/system/check_doc_first_gating_governance.py
python scripts/system/check_development_governance.py
```

- 结果：`14 passed`，两项治理检查通过。
