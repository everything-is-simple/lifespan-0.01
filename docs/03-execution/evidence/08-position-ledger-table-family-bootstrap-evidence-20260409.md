# position 账本表族落库与 bootstrap 证据

证据编号：`08`
日期：`2026-04-09`

## 命令

```text
python -m pytest tests/unit/position/test_bootstrap.py --basetemp=H:/Lifespan-temp/pytest-position-08
python -m pytest tests/unit/core/test_paths.py --basetemp=H:/Lifespan-temp/pytest-core-08
python -m pytest tests/unit/system/test_doc_first_gating_governance.py --basetemp=H:/Lifespan-temp/pytest-system-08
python scripts/system/check_development_governance.py
python .codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py --include-untracked
python scripts/system/check_doc_first_gating_governance.py
@'
import sys
from pathlib import Path
sys.path.insert(0, r'H:\lifespan-0.01\src')
from mlq.core.paths import default_settings
from mlq.position import bootstrap_position_ledger, materialize_position_from_formal_signals, PositionFormalSignalInput, position_ledger_path
import duckdb
'@ | python -
```

## 关键结果

1. `tests/unit/position/test_bootstrap.py` 共 `5` 项全部通过，覆盖：
   - 最小表族创建
   - policy seed 幂等写入
   - admitted 信号写入 `candidate / capacity / sizing / fixed_notional`
   - blocked 信号写入 `candidate / sizing / single_lot`
   - current position 超过允许上限时的 `trim_to_context_cap`
2. 分别运行的 `core` 与 `system` 现有单测都通过，说明本轮 `position` 代码未破坏现有五根目录与文档门禁逻辑。
3. 治理检查全部通过，说明 08 关闭后，执行索引、阅读顺序与当前待施工卡保持一致。
   - 补充：`check_development_governance.py` 给出一条软提示，
     `src/mlq/position/bootstrap.py` 当前为 `934` 行，超过 `800` 行目标上限但未超过 `1000` 行硬上限。
4. bounded smoke 在 `H:\Lifespan-temp\position-smoke-08` 下成功落出最小账本：
   - `position_run = 1`
   - `position_candidate_audit = 2`
   - `position_capacity_snapshot = 2`
   - `position_sizing_snapshot = 2`
   - `position_funding_fixed_notional_snapshot = 2`
5. smoke 中 admitted 样本核查结果：
   - `candidate_status = admitted`
   - `context_code = BULL_MAINSTREAM`
   - `position_action_decision = open_up_to_context_cap`
   - `final_allowed_position_weight = 0.1875`
   - `target_shares = 9300`
6. smoke 中 blocked 样本核查结果：
   - `candidate_status = blocked`
   - `blocked_reason_code = alpha_not_admitted`

## 产物

1. `src/mlq/position/bootstrap.py`
2. `src/mlq/position/__init__.py`
3. `tests/unit/position/test_bootstrap.py`
4. `docs/02-spec/modules/position/02-alpha-to-position-formal-signal-bridge-spec-20260409.md`
5. `docs/03-execution/08-position-ledger-table-family-bootstrap-card-20260409.md`
