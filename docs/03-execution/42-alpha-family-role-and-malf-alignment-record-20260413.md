# 42-alpha-family-role-and-malf-alignment 记录
更新时间 `2026-04-13`

## 实施内容

1. 升级 `alpha family` shared contract
   - `src/mlq/alpha/family_shared.py`
   - 新增官方 `structure / malf` 输入常量
   - 将 `family_contract_version` 前推到 `alpha-family-v2`
   - 增补 family context 与 canonical malf 读取所需 dataclass / 归一化工具

2. 补齐官方 source 读取
   - `src/mlq/alpha/family_source.py`
   - `trigger_row` 现在保留 daily / weekly / monthly 上游 context 锚点
   - 新增 `structure_snapshot` 与 `malf_state_snapshot` 的只读加载逻辑
   - 组装 `trigger_event_nk -> family_context` 的官方解释上下文映射

3. 重写 family payload 物化
   - `src/mlq/alpha/family_materialization.py`
   - `family_event_nk` 改为围绕 `source_trigger_event_nk + family_contract_version`
   - `payload_json` 从最小透传升级为正式结构化解释层
   - 保留 `candidate_payload / source_trigger / official_context / source_context_snapshot` 审计信息

4. 更新 runner 与模块导出
   - `src/mlq/alpha/family_runner.py`
   - `src/mlq/alpha/__init__.py`
   - runner 现在正式依赖 `structure` 与 canonical `malf`
   - summary 中补齐 structure / malf ledger 路径与 source table 审计字段

5. 扩展单测与集成夹具
   - `tests/unit/alpha/test_family_runner.py`
   - `tests/unit/alpha/test_pas_runner.py`
   - 覆盖五触发默认角色、`PB` 第一回调升级、`BOF` 冲突降级与 rematerialize、以及 `41 -> 42` 主链衔接

## 关键实现决策

1. `alpha family` 正式承担“家族解释层”
   - `malf` 仍只表达结构与阶段
   - family 只读消费 canonical `malf`，不反写 `malf core`

2. `payload_json` 同时保留真值键与审计快照
   - 正式真值：
     - `family_role`
     - `malf_alignment`
     - `malf_phase_bucket`
     - `family_bias`
   - 审计与 rematerialize 说明：
     - `source_context_fingerprint`
     - `source_context_snapshot`
     - `official_context`

3. `PB` 只在“第一回调 + aligned”时升级为 `mainline`
   - 其余情况保持 `supporting`
   - `CPB` 保持 `scout`
   - `BPB` 保持 `warning`

4. `BOF / TST` 在 `malf_alignment='conflicted'` 时只允许降级
   - 当前统一降级为 `supporting`
   - 不在本卡内触碰 `formal signal / trade` 的 admitted 规则

## 偏差与修正

1. 初版 family 测试只造了 `structure/filter`
   - 卡 42 后 family 正式依赖 canonical `malf`
   - 已在 `test_pas_runner.py` 的夹具中补最小 `malf_state_snapshot`

2. 初版 family payload 只有最小 `candidate_payload` 透传
   - 无法稳定表达 role/alignment，也无法解释 rematerialize 原因
   - 已改为“正式解释键 + 审计快照”双层结构
