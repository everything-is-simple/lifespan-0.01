# raw/base 日周月分库迁移尾收口 记录

记录编号：`77`
日期：`2026-04-18`

## 做了什么

1. 先盘点六个官方库，冻结 `stock/index/block × day/week/month × raw/base` 的完成度矩阵，确认 `77` 切片 1 范围正确
2. 用现有正式 runner 完成 `index/block week/month raw` 迁移到 `raw_market_week/month.duckdb`
3. 用现有正式 runner 完成 `index/block week/month base` 迁移到 `market_base_week/month.duckdb`
4. 重新核验六库 row_count / code_count / date_range，并确认 `index/block week/month` pending scope 清零
5. 新增 `src/mlq/data/data_timeframe_split_cleanup.py` 与 `scripts/data/run_timeframe_split_tail_cleanup.py`，把旧 day 库 `week/month` 价格表与 timeframe audit/dirty 尾巴 purge 成可复现动作
6. 在真实库执行 day tail cleanup，并确认 day raw/base 只剩 `day`
7. 发现并修正 day bootstrap 会在 purge 后重建空 `week/month` 表的问题，补了对应单测和 bootstrap day-only table selection

## 偏离项

- cleanup 第一版直接对带唯一索引的 registry 表做 delete 时触发 DuckDB 索引删除异常，随后改成“临时卸索引再删再建”的 helper 处理路径
- purge 后又发现 `bootstrap_raw_market_ledger()` 仍会把 day raw 的空 `week/month` 表补回来，因此把 `bootstrap.py` 的 day table set 收窄到规范要求的 day-only 表族

## 备注

- `77` 不是推翻 `76`，而是承接 `76` 收尾，把“stock 已迁移”推进到“六库全部完成且旧 day 库周月已清”
- 本次收口后，`78-84` 可恢复为当前正式主线
- 真实执行 summary 已落到 `H:\Lifespan-temp\77-split-tail\`

## 记录结构图

```mermaid
flowchart LR
    A["六库复盘"] --> B["迁移 index/block week/month raw/base"]
    B --> C["purge 旧 day tail"]
    C --> D["修 bootstrap 防止空表回生"]
    D --> E["77 收口并恢复 78-84"]
```
