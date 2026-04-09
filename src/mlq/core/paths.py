"""Workspace and formal database path contracts for lifespan-0.01."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


_DATA_DIRNAME = "Lifespan-data"
_TEMP_DIRNAME = "Lifespan-temp"
_REPORT_DIRNAME = "Lifespan-report"
_VALIDATED_DIRNAME = "Lifespan-Validated"

FORMAL_MODULES = (
    "core",
    "data",
    "malf",
    "structure",
    "filter",
    "alpha",
    "position",
    "portfolio_plan",
    "trade",
    "system",
)


def discover_repo_root(start: Path | None = None) -> Path:
    """Find the repository root by walking upward to `pyproject.toml`."""
    current = (start or Path(__file__)).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("Could not locate repository root from the current path.")


@dataclass(frozen=True)
class DatabasePaths:
    """Formal historical-ledger databases for the current refactor baseline."""

    raw_market: Path
    market_base: Path
    malf: Path
    structure: Path
    filter: Path
    alpha: Path
    position: Path
    portfolio_plan: Path
    trade_runtime: Path
    system: Path

    def as_dict(self) -> dict[str, Path]:
        return {
            "raw_market": self.raw_market,
            "market_base": self.market_base,
            "malf": self.malf,
            "structure": self.structure,
            "filter": self.filter,
            "alpha": self.alpha,
            "position": self.position,
            "portfolio_plan": self.portfolio_plan,
            "trade_runtime": self.trade_runtime,
            "system": self.system,
        }


@dataclass(frozen=True)
class WorkspaceRoots:
    """Top-level local workspace roots shared across modules."""

    repo_root: Path
    data_root: Path
    temp_root: Path
    report_root: Path
    validated_root: Path

    @property
    def databases(self) -> DatabasePaths:
        return DatabasePaths(
            raw_market=self.data_root / "raw" / "raw_market.duckdb",
            market_base=self.data_root / "base" / "market_base.duckdb",
            malf=self.data_root / "malf" / "malf.duckdb",
            structure=self.data_root / "structure" / "structure.duckdb",
            filter=self.data_root / "filter" / "filter.duckdb",
            alpha=self.data_root / "alpha" / "alpha.duckdb",
            position=self.data_root / "position" / "position.duckdb",
            portfolio_plan=self.data_root / "portfolio_plan" / "portfolio_plan.duckdb",
            trade_runtime=self.data_root / "trade" / "trade_runtime.duckdb",
            system=self.data_root / "system" / "system.duckdb",
        )

    def module_temp_root(self, module_name: str) -> Path:
        """Return the temp workspace reserved for a formal module."""
        _validate_module_name(module_name)
        return self.temp_root / module_name

    def module_report_root(self, module_name: str) -> Path:
        """Return the report workspace reserved for a formal module."""
        _validate_module_name(module_name)
        return self.report_root / module_name

    def module_validated_root(self, module_name: str) -> Path:
        """Return the validated workspace reserved for a formal module."""
        _validate_module_name(module_name)
        return self.validated_root / module_name

    def ensure_directories(self) -> None:
        """Create declared workspace roots and parent directories for formal outputs."""
        for root in (
            self.repo_root,
            self.data_root,
            self.temp_root,
            self.report_root,
            self.validated_root,
        ):
            root.mkdir(parents=True, exist_ok=True)
        for database_path in self.databases.as_dict().values():
            database_path.parent.mkdir(parents=True, exist_ok=True)
        for module_name in FORMAL_MODULES:
            self.module_temp_root(module_name).mkdir(parents=True, exist_ok=True)
            self.module_report_root(module_name).mkdir(parents=True, exist_ok=True)
            self.module_validated_root(module_name).mkdir(parents=True, exist_ok=True)


def _default_external_root(repo_root: Path, target_dirname: str) -> Path:
    return repo_root.parent / target_dirname


def _validate_module_name(module_name: str) -> None:
    if module_name not in FORMAL_MODULES:
        raise ValueError(f"Unknown formal module: {module_name}")


def default_settings(repo_root: Path | None = None) -> WorkspaceRoots:
    """Resolve workspace roots and allow environment variables to override defaults."""
    resolved_repo_root = Path(
        os.getenv("LIFESPAN_REPO_ROOT", repo_root or discover_repo_root())
    ).resolve()
    data_root = Path(
        os.getenv("LIFESPAN_DATA_ROOT", _default_external_root(resolved_repo_root, _DATA_DIRNAME))
    ).resolve()
    temp_root = Path(
        os.getenv("LIFESPAN_TEMP_ROOT", _default_external_root(resolved_repo_root, _TEMP_DIRNAME))
    ).resolve()
    report_root = Path(
        os.getenv(
            "LIFESPAN_REPORT_ROOT",
            _default_external_root(resolved_repo_root, _REPORT_DIRNAME),
        )
    ).resolve()
    validated_root = Path(
        os.getenv(
            "LIFESPAN_VALIDATED_ROOT",
            _default_external_root(resolved_repo_root, _VALIDATED_DIRNAME),
        )
    ).resolve()
    return WorkspaceRoots(
        repo_root=resolved_repo_root,
        data_root=data_root,
        temp_root=temp_root,
        report_root=report_root,
        validated_root=validated_root,
    )
