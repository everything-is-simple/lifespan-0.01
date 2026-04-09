"""`alpha` 模块负责正式信号账本与官方 formal signal producer。"""

from mlq.alpha.bootstrap import (
    ALPHA_FORMAL_SIGNAL_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_RUN_TABLE,
    alpha_ledger_path,
    bootstrap_alpha_formal_signal_ledger,
    connect_alpha_ledger,
)
from mlq.alpha.runner import (
    DEFAULT_ALPHA_FORMAL_SIGNAL_CONTRACT_VERSION,
    DEFAULT_ALPHA_FORMAL_SIGNAL_CONTEXT_TABLE,
    DEFAULT_ALPHA_FORMAL_SIGNAL_TRIGGER_TABLE,
    AlphaFormalSignalBuildSummary,
    run_alpha_formal_signal_build,
)

__all__ = [
    "ALPHA_FORMAL_SIGNAL_EVENT_TABLE",
    "ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE",
    "ALPHA_FORMAL_SIGNAL_RUN_TABLE",
    "DEFAULT_ALPHA_FORMAL_SIGNAL_CONTRACT_VERSION",
    "DEFAULT_ALPHA_FORMAL_SIGNAL_CONTEXT_TABLE",
    "DEFAULT_ALPHA_FORMAL_SIGNAL_TRIGGER_TABLE",
    "AlphaFormalSignalBuildSummary",
    "alpha_ledger_path",
    "bootstrap_alpha_formal_signal_ledger",
    "connect_alpha_ledger",
    "run_alpha_formal_signal_build",
]
