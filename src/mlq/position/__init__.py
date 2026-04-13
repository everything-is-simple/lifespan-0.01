"""`position` 模块负责单标的仓位计划与资金管理账本。"""

from .bootstrap import (
    DEFAULT_POSITION_POLICY_SEEDS,
    POSITION_LEDGER_DDL,
    POSITION_LEDGER_TABLE_NAMES,
    PositionFormalSignalInput,
    PositionMaterializationSummary,
    bootstrap_position_ledger,
    connect_position_ledger,
    materialize_position_from_formal_signals,
    position_ledger_path,
)
from .position_bootstrap_schema import DEFAULT_POSITION_CONTRACT_VERSION
from .runner import (
    DEFAULT_ALPHA_FORMAL_SIGNAL_TABLE,
    DEFAULT_MARKET_BASE_ADJUST_METHOD,
    DEFAULT_MARKET_BASE_PRICE_TABLE,
    PositionFormalSignalRunnerSummary,
    run_position_formal_signal_materialization,
)

__all__ = [
    "DEFAULT_ALPHA_FORMAL_SIGNAL_TABLE",
    "DEFAULT_MARKET_BASE_ADJUST_METHOD",
    "DEFAULT_MARKET_BASE_PRICE_TABLE",
    "DEFAULT_POSITION_POLICY_SEEDS",
    "DEFAULT_POSITION_CONTRACT_VERSION",
    "POSITION_LEDGER_DDL",
    "POSITION_LEDGER_TABLE_NAMES",
    "PositionFormalSignalInput",
    "PositionMaterializationSummary",
    "PositionFormalSignalRunnerSummary",
    "bootstrap_position_ledger",
    "connect_position_ledger",
    "materialize_position_from_formal_signals",
    "position_ledger_path",
    "run_position_formal_signal_materialization",
]
