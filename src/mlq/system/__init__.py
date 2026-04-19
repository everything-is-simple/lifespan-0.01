"""`system` 模块负责编排、治理、审计与冻结元数据。"""

from mlq.system.bootstrap import (
    SYSTEM_CHILD_RUN_READOUT_TABLE,
    SYSTEM_LEDGER_DDL,
    SYSTEM_LEDGER_TABLE_NAMES,
    SYSTEM_MAINLINE_SNAPSHOT_TABLE,
    SYSTEM_RUN_SNAPSHOT_TABLE,
    SYSTEM_RUN_TABLE,
    bootstrap_system_ledger,
    connect_system_ledger,
    system_ledger_path,
)
from mlq.system.runner import (
    DEFAULT_SYSTEM_CONTRACT_VERSION,
    DEFAULT_SYSTEM_SCENE,
    SystemMainlineReadoutSummary,
    run_system_mainline_readout_build,
)

__all__ = [
    "DEFAULT_SYSTEM_CONTRACT_VERSION",
    "DEFAULT_SYSTEM_SCENE",
    "SYSTEM_CHILD_RUN_READOUT_TABLE",
    "SYSTEM_LEDGER_DDL",
    "SYSTEM_LEDGER_TABLE_NAMES",
    "SYSTEM_MAINLINE_SNAPSHOT_TABLE",
    "SYSTEM_RUN_SNAPSHOT_TABLE",
    "SYSTEM_RUN_TABLE",
    "SystemMainlineReadoutSummary",
    "bootstrap_system_ledger",
    "connect_system_ledger",
    "run_system_mainline_readout_build",
    "system_ledger_path",
]
