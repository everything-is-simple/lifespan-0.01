# structure/filter 正式分层与最小 snapshot 记录

记录编号：`11`
日期：`2026-04-09`

## 做了什么

1. 在 `src/mlq/structure/bootstrap.py` 与 `src/mlq/structure/runner.py` 落下 `structure_run / structure_snapshot / structure_run_snapshot` 三表和最小 producer，支持：
   - bounded 窗口读取
   - 自然键累积
   - `inserted / reused / rematerialized` 审计
   - `advancing / stalled / failed / unknown` 最小结构状态归类
2. 在 `src/mlq/filter/bootstrap.py` 与 `src/mlq/filter/runner.py` 落下 `filter_run / filter_snapshot / filter_run_snapshot` 三表和最小 producer，支持：
   - 官方 `structure_snapshot` 消费
   - `failed_extreme / structure_failed` 最小硬门
   - 非阻断结构状态保守放行
3. 新增正式脚本入口：
   - `scripts/structure/run_structure_snapshot_build.py`
   - `scripts/filter/run_filter_snapshot_build.py`
4. 调整 `src/mlq/alpha/runner.py` 和 `scripts/alpha/run_alpha_formal_signal_build.py`，让 `alpha` 默认优先消费 `filter_snapshot + structure_snapshot`，仅保留旧 `pas_context_snapshot` 作为兼容兜底。
5. 新增并回跑：
   - `tests/unit/structure/test_runner.py`
   - `tests/unit/filter/test_runner.py`
   - 重写后的 `tests/unit/alpha/test_runner.py`
6. 按入口文件规则同步刷新 `AGENTS.md`、`README.md`、`scripts/README.md`、`pyproject.toml`，并回填本卡 `evidence / record / conclusion` 及执行索引。

## 偏离项

- `alpha_formal_signal_run` 现有 schema 仍保留 `source_context_table` 列名；本轮把它用于记录默认 `filter` 上游表名，结构上游表与 fallback 表则补记进 summary，而没有在 11 号卡里额外扩张 `alpha` run 表 schema。

## 备注

- 当前 `filter` 有意保持“少拦截”的正式口径：`stalled / unknown` 只写 note，不直接 block。
- 本轮已经把真实上游补到 `alpha` 前，不再需要回头扩 `position` 来掩盖上游缺口。
