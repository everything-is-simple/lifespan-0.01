# malf canonical ledger and data-grade runner bootstrap 证据

日期：`2026-04-11`
状态：`已补证据`

## 代码证据

- `src/mlq/malf/bootstrap.py`
  - canonical 表族 DDL 与 required columns 已落地
- `src/mlq/malf/canonical_runner.py`
  - canonical runner 已落地
- `src/mlq/malf/__init__.py`
  - canonical runner 与表名常量已导出
- `scripts/malf/run_malf_canonical_build.py`
  - bounded runner 脚本入口已落地

## 测试证据

- `tests/unit/malf/test_canonical_runner.py`
  - 覆盖 `confirmed_at`
  - 覆盖 `D / W / M` 落表
  - 覆盖 `work_queue / checkpoint` source advanced 后的再入队与再完成
- `tests/unit/malf/test_malf_runner.py`
- `tests/unit/malf/test_mechanism_runner.py`

## 命令证据

```bash
pytest tests/unit/malf/test_canonical_runner.py -q
pytest tests/unit/malf/test_malf_runner.py tests/unit/malf/test_mechanism_runner.py tests/unit/malf/test_canonical_runner.py -q
```

## 入口同步证据

- `AGENTS.md`
- `README.md`
- `pyproject.toml`
