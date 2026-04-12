# malf 机制层 sidecar 账本 bootstrap 与下游接入结论

结论编号：`25`
日期：`2026-04-11`
状态：`生效中`

## 裁决

- 接受：`pivot_confirmed_break_ledger / same_timeframe_stats_profile / same_timeframe_stats_snapshot` 已经作为 bridge-era 正式机制层账本落地，具备稳定实体锚点、自然键、增量续跑与审计 run 入口。
- 接受：`scripts/malf/run_malf_mechanism_build.py` 现在是 `malf` 机制层 sidecar 的正式 bounded runner 入口；它只消费 bridge v1 `pas_context_snapshot / structure_candidate_snapshot`，不反写 `malf core`。
- 接受：`malf_mechanism_checkpoint` 已经按 `instrument + timeframe` 记录增量边界，支持机制层 sidecar 的日更续跑与 replay 基线。
- 接受：`structure / filter` 已完成最小只读 sidecar 接入。`structure_snapshot` 只附加 break/stats sidecar 字段，`filter_snapshot` 只透传并提示，不改写既有硬判定逻辑。
- 拒绝：把本轮实现描述为 pure semantic canonical runner 落地，或把 break/stats sidecar 解释成 `malf core` 新原语。
- 拒绝：把 `filter` 的 sidecar 提示提升成新的正式阻断规则；当前合同仍然要求它保持只读附加身份。

## 原因

- `24` 号卡只冻结了机制层边界，没有 runner、checkpoint、表族和最小下游接入；若这一步不实现，sidecar 仍停留在纸面合同，无法满足历史账本系统对续跑、复算与审计的最低要求。
- bridge v1 现有输出已经稳定提供 `pas_context_snapshot / structure_candidate_snapshot`，因此最务实的实现路径是先把机制层账本挂在 bridge v1 之上，而不是虚称 canonical runner 已经准备就绪。
- `structure / filter` 的下游接入如果不在这一轮一起落地，机制层 sidecar 就仍会回退成私有字段或临时判断，破坏 `24` 号卡刚冻结的“只读 sidecar、不反写 core”边界。

## 影响

- 当前最新生效结论锚点切换为 `25-malf-mechanism-ledger-bootstrap-and-downstream-sidecar-integration-conclusion-20260411.md`。
- `malf` 当前正式 runner 分成两段：
  - `scripts/malf/run_malf_snapshot_build.py` 继续负责 bridge v1 兼容输出。
  - `scripts/malf/run_malf_mechanism_build.py` 正式负责机制层 sidecar 账本、checkpoint 与 replay 边界。
- `structure / filter` 现在具备消费 break/stats sidecar 的正式只读路径，为后续 `alpha / position` 是否读取这些 sidecar 留下稳定上游，但本轮不扩展到更下游模块。
- `AGENTS.md / README.md / pyproject.toml` 已同步，仓库入口文件不再落后于当前 runner 治理口径。

## 验证

- `python -m pytest tests/unit/malf/test_mechanism_runner.py tests/unit/structure/test_runner.py tests/unit/filter/test_runner.py`
- `python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked`
- `python scripts/system/check_doc_first_gating_governance.py`
- `python scripts/system/check_development_governance.py AGENTS.md README.md pyproject.toml src/mlq/malf src/mlq/structure src/mlq/filter scripts/malf tests/unit/malf tests/unit/structure tests/unit/filter docs/03-execution`
