# structure filter 主线旧版 malf 语义清理记录

记录编号：`38`
日期：`2026-04-13`

## 实施记录

1. 盘点 `structure / filter` 主线代码
   - 确认 `runner` 默认口径虽已偏 canonical，但 `structure_source.py` 仍保留 bridge-era 读取分支与旧语义映射
2. 固化主线 contract
   - 在 `structure` 与 `filter` runner 入口加显式校验
   - 非 canonical / official 表名与非 `D` 时间框架直接拒绝
3. 清理残留 bridge path
   - 删除 `structure_source.py` 中主线已不允许触达的 legacy loader / mapper
   - `read_only_context` 也只允许 canonical 上游
4. 重建测试基座
   - `structure / filter` 单测改为 canonical-only fixture
   - `alpha / system` 主线上游 fixture 一并切换，防止回归链路仍从旧表取数
5. 回归验证
   - 运行 `structure / filter / alpha / system` 相关单测
   - 运行 doc-first 与 development governance 检查

## 风险与边界

- 本卡只处理 `structure / filter` 主线旧版语义清理，不触碰 `malf snapshot bridge v1` 的兼容职责
- `tests/unit/malf/*` 中针对 bridge v1 兼容回退的测试仍保留，它们属于兼容层，而不是 mainline 默认口径
- `39 / 40` 的本地账本标准化与增量续跑尚未实施；本卡只为其清理主线前置语义噪音

## 交接

- `38` 收口后，当前正式施工卡切换到 `39-mainline-local-ledger-standardization-bootstrap-card-20260413.md`
- 下一步需围绕 `data` 口径推进本地正式库一次性建仓标准化，再补每日增量与断点续传闭环
