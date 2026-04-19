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

- 当前正式待施工卡已切到 `76`，`78-84` 暂缓，等待 `data` 层六库分时框迁移完成后再恢复。
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

## 2026-04-17 第二刀补充裁决

- `76` 已从“只冻结路径契约”推进到“runner 已开始按 timeframe 写新库”的状态。
- 当前已成立的正式口径：
  - `day raw/base` 继续写旧 day 官方库
  - `week/month raw/base` 已开始写新 `week/month` 官方库
- 当前尚未成立的正式口径：
  - `week/month raw` 还不是从 `day raw` 正式派生，仍保留 day txt fallback
  - 真实官方库的 `stock week/month` rebuild 与旧 day 库 purge 还未执行

## 第二刀验证

```text
pytest tests/unit/core/test_paths.py tests/unit/data/test_timeframe_ledger_bootstrap.py tests/unit/data/test_raw_ingest_runner.py -q
python scripts/system/check_doc_first_gating_governance.py
python scripts/system/check_development_governance.py
```

- 结果：`16 passed`，两项治理检查通过。
## 2026-04-17 第三刀补充裁决

- `76` 已进一步推进到“`week/month raw` 的官方来源从 `day raw` 账本派生”的状态。
- 当前已经成立的正式口径：
  - `day raw` 仍是唯一读取 `H:\tdx_offline_Data` 日线 txt 的官方 raw 入口。
  - `week/month raw` 只允许读取 `raw_market_day.duckdb` 中对应 `asset + adjust_method` 的日线真值，再派生写入 `raw_market_week/month.duckdb`。
  - `week/month raw` 的 `resume / pending-only` 语义也已经改成基于 `day raw` 官方库盘点，而不是基于 `week/month txt` 目录盘点。
- 当前不再成立的旧口径：
  - `week/month raw` 从 `stock-week` / `stock-month` 直接源读取。
  - `week/month raw` 缺直接源时回退到 `day txt` 重扫聚合。
- 当前仍未完成：
  - 真实官方库上的 `stock week/month` rebuild。
  - rebuild 后的 parity 校验。
  - 旧 `day` 官方库里遗留周月表和数据的 purge。

## 第三刀验证

```text
pytest tests/unit/core/test_paths.py tests/unit/data/test_timeframe_ledger_bootstrap.py tests/unit/data/test_raw_ingest_runner.py tests/unit/data/test_market_base_timeframe_runner.py -q
python scripts/system/check_doc_first_gating_governance.py
python scripts/system/check_development_governance.py
```

## 2026-04-18 绗洓鍒€琛ュ厖瑁佸喅

- `76` 鐜板湪宸叉帹杩涘埌鈥滅湡瀹炲畼鏂瑰簱 stock day/week/month raw/base 鏁版嵁鏈韩鍏ㄩ儴瀵归綈锛宒irty / audit 灏惧反涔熷凡鏀跺彛鈥濈殑鐘舵€併€?
- 鎵€鏈?stock backward 瀹樻柟琛ㄧ幇鍦ㄥ凡缁忓叏閮ㄥ榻愶細
  - `raw day` = `5501 code / 16,348,113 rows`
  - `raw week` = `5501 code / 3,453,967 rows`
  - `raw month` = `5501 code / 826,336 rows`
  - `base day` = `5501 code / 16,348,113 rows`
  - `base week` = `5501 code / 3,453,967 rows`
  - `base month` = `5501 code / 826,336 rows`
- `market_base_month.duckdb` 鐨?`base_dirty_instrument(timeframe='month', adjust_method='backward')` 宸茬敱 `5491 consumed + 10 pending` 鏀跺埌 `5501 consumed`銆?
- `base-stock-month-split-resume2-20260417c-b0001` 浠ュ強 2026-04-18 涓轰簡娓呯悊灏惧反鑰岀敓鎴愮殑涓ゆ潯 `cleanup/debug` run audit 宸插叏閮ㄨˉ鎴?`failed`锛屼笉鍐嶇暀涓?`running` 僵灏俱€?
- `76` 褰撳墠浠嶆湭姝ｅ紡鎺ュ彈锛屼絾鈥滃叚搴撳垎鏃舵 + day raw 娲剧敓 week/month raw + stock week/month 鐪熷疄瀹樻柟搴撻噸寤?+ parity + dirty/audit 鏀跺彛鈥濊繖鏉¤縼绉婚摼璺凡缁忔墦閫氥€?

## 绗洓鍒€楠岃瘉

```text
python -m pytest tests/unit/data/test_market_base_runner.py tests/unit/data/test_market_base_timeframe_runner.py tests/unit/data/test_market_base_dirty_consumption.py -q
python scripts/system/check_doc_first_gating_governance.py
python scripts/system/check_development_governance.py
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
duckdb 鏌ヨ H:\Lifespan-data\raw\raw_market*.duckdb / H:\Lifespan-data\base\market_base*.duckdb 鐨?stock backward day/week/month row_count code_count dirty_count
```

- 结果：`19 passed`，两项治理检查通过。
