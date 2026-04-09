# scripts

这里放新仓当前阶段允许保留的正式脚本入口。
本轮允许保留三类脚本：

1. 治理脚本
2. 环境脚本
3. 已冻结合同后的模块 bounded runner

禁止把旧系统的大量业务 runner 整包复制进来。

脚本治理还承担两条仓库纪律：

1. 临时产物必须进入 `H:\Lifespan-temp`，不允许落回仓库根目录。
2. 只要 `scripts/`、`.codex/`、`docs/01-design/`、`docs/02-spec/` 或 `src/mlq/core/paths.py` 发生正式口径变化，就必须同步刷新 `AGENTS.md`、`README.md`、`pyproject.toml`。

## setup

1. `powershell -File scripts/setup/enter_repo.ps1`
   - 进入仓库、设置五根目录环境变量，并可选激活 `.venv`
2. `powershell -File scripts/setup/rebuild_windows_env.ps1`
   - 使用 `D:\miniconda310\python.exe` 重建 `.venv`
   - 安装 `.[dev]`
   - 运行最小导入冒烟、治理检查与单元测试

## system

1. `python scripts/system/check_file_length_governance.py`
   - 检查单文件硬上限与目标上限
2. `python scripts/system/check_chinese_governance.py`
   - 检查正式 Markdown 中文化与 Python 中文注释
3. `python scripts/system/check_repo_hygiene_governance.py`
   - 检查仓库内是否混入缓存、数据库或中间产物
4. `python scripts/system/check_entry_freshness_governance.py`
   - 检查治理入口改动时是否同步刷新 `AGENTS.md`、`README.md`、`pyproject.toml`
5. `python scripts/system/check_doc_first_gating_governance.py`
   - 检查当前待施工卡是否已经具备需求、设计、规格和任务分解
6. `python scripts/system/check_development_governance.py`
   - 串联开发治理检查

## execution

1. `python .codex/skills/lifespan-execution-discipline/scripts/new_execution_bundle.py --number 2 --slug sample --title 示例 --dry-run`
   - 预览生成执行四件套
2. `python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked`
   - 在本地重构阶段检查执行索引、卡目录和完工账本

## alpha

## structure

1. `python scripts/structure/run_structure_snapshot_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --limit 10 --batch-size 10`
   - 从官方 `malf` 结构候选事实与执行上下文做 bounded 读取
   - 物化 `structure_run / structure_snapshot / structure_run_snapshot`
   - 输出可被 `filter / alpha` 稳定消费的官方结构事实层

## filter

1. `python scripts/filter/run_filter_snapshot_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --limit 10 --batch-size 10`
   - 从官方 `structure snapshot` 与最小 `execution_context` 做 bounded 读取
   - 物化 `filter_run / filter_snapshot / filter_run_snapshot`
   - 输出可被 `alpha` 优先消费的官方 pre-trigger 准入层

## alpha

1. `python scripts/alpha/run_alpha_trigger_ledger_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --limit 10 --batch-size 10`
   - 从 bounded detector 输入与官方 `filter / structure snapshot` 上游做 bounded 读取
   - 物化 `alpha_trigger_run / alpha_trigger_event / alpha_trigger_run_event`
   - 产出可被 `alpha formal signal` 稳定引用的官方 `alpha trigger ledger`

2. `python scripts/alpha/run_alpha_formal_signal_build.py --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --limit 10 --batch-size 10`
   - 从官方 `alpha trigger ledger` 与 `filter / structure snapshot` 上游做 bounded 读取
   - 物化 `alpha_formal_signal_run / alpha_formal_signal_event / alpha_formal_signal_run_event`
   - 输出可被 `position` 直接消费的官方 `alpha formal signal`

## position

1. `python scripts/position/run_position_formal_signal_materialization.py --policy-id fixed_notional_full_exit_v1 --capital-base-value 1000000 --signal-start-date 2026-04-08 --signal-end-date 2026-04-08 --limit 10`
   - 从官方 `alpha formal signal` bounded 读取样本
   - 用 `market_base.stock_daily_adjusted.close` 补参考成交日与参考价
   - 复用 `position` 既有 materialization helper 落表
