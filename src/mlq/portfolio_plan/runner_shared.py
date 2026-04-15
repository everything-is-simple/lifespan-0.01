"""`portfolio_plan` runner 的共享常量。"""

from __future__ import annotations

from typing import Final


DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION: Final[str] = "portfolio-plan-v2"
DEFAULT_SOURCE_POSITION_TABLE: Final[str] = (
    "position_candidate_audit+position_capacity_snapshot+position_sizing_snapshot"
)
DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE: Final[str] = "position_candidate_audit"
DEFAULT_POSITION_CAPACITY_TABLE: Final[str] = "position_capacity_snapshot"
DEFAULT_POSITION_SIZING_TABLE: Final[str] = "position_sizing_snapshot"
DEFAULT_PORTFOLIO_PLAN_RUNNER_NAME: Final[str] = "portfolio_plan_builder"
DEFAULT_PORTFOLIO_PLAN_RUNNER_VERSION: Final[str] = "v3"
DEFAULT_CANDIDATE_CHECKPOINT_SCOPE: Final[str] = "candidate"
