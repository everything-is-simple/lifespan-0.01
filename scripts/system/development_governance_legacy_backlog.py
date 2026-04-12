"""登记全仓治理扫描中的历史债务清单。

这些条目不会放过按路径触发的严格检查。
它们只用于全仓盘点时，把既有债务从“新增违规”降级为“历史债务”。
"""

LEGACY_HARD_OVERSIZE_BACKLOG: tuple[str, ...] = ()

LEGACY_TARGET_OVERSIZE_BACKLOG: tuple[str, ...] = (
    "src/mlq/alpha/family_runner.py",
    "src/mlq/malf/bootstrap.py",
    "src/mlq/position/bootstrap.py",
)

LEGACY_CHINESE_PYTHON_BACKLOG: tuple[str, ...] = ()
