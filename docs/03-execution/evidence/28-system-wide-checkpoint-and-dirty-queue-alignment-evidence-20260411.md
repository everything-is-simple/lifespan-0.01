# system-wide checkpoint and dirty queue alignment 证据

证据编号：`28`
日期：`2026-04-11`
状态：`已补证据`

## 命令

```text
python scripts/system/check_doc_first_gating_governance.py
pytest tests/unit/malf/test_malf_runner.py -q
python -m pytest tests/unit -q
```

## 关键结果

- `doc-first gating` 通过，当前待施工卡已按自然数顺排推进到 `100-trade-signal-anchor-contract-freeze-card-20260411.md`。
- `tests/unit/malf/test_malf_runner.py` 通过，bridge v1 兼容路径测试已显式绑定 `pas_context_snapshot + structure_candidate_snapshot`，不再误走 canonical 默认入口。
- `tests/unit -q` 通过，结果为 `59 passed`，说明 `29-32` 收口后没有留下回归失败。
- `29-32` 对应结论均已裁决，形成了 `malf` 优先卡组先落地、`100-105` 后置恢复施工的正式证据链。

## 产物

- `docs/03-execution/28-system-wide-checkpoint-and-dirty-queue-alignment-conclusion-20260411.md`
- `docs/03-execution/evidence/28-system-wide-checkpoint-and-dirty-queue-alignment-evidence-20260411.md`
- `docs/03-execution/records/28-system-wide-checkpoint-and-dirty-queue-alignment-record-20260411.md`
- `docs/03-execution/28-system-wide-checkpoint-and-dirty-queue-alignment-card-20260411.md`

## 证据流图

```mermaid
flowchart LR
    CARD28[28卡 checkpoint对齐] --> C29[29 malf canonical 合同]
    C29 --> C30[30 canonical runner bootstrap]
    C30 --> C31[31 下游改绑]
    C31 --> C32[32 真值复核]
    C32 --> OK[59 passed 无回归]
```
