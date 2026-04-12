"""登记全仓治理扫描中的历史债务清单。

这些条目不会放过按路径触发的严格检查。
它们只用于全仓盘点时，把既有债务从“新违规”降级为“历史债务”。
"""

LEGACY_HARD_OVERSIZE_BACKLOG: tuple[str, ...] = (
    "src/mlq/alpha/runner.py",
    "src/mlq/alpha/trigger_runner.py",
    "src/mlq/data/runner.py",
    "src/mlq/filter/runner.py",
    "src/mlq/malf/canonical_runner.py",
    "src/mlq/malf/mechanism_runner.py",
    "src/mlq/structure/runner.py",
    "src/mlq/system/runner.py",
    "src/mlq/trade/runner.py",
    "tests/unit/data/test_data_runner.py",
)

LEGACY_TARGET_OVERSIZE_BACKLOG: tuple[str, ...] = (
    "src/mlq/alpha/family_runner.py",
    "src/mlq/data/bootstrap.py",
    "src/mlq/malf/bootstrap.py",
    "src/mlq/malf/runner.py",
    "src/mlq/position/bootstrap.py",
)

LEGACY_CHINESE_PYTHON_BACKLOG: tuple[str, ...] = ()
