"""定义新仓五根目录与正式账本路径契约。"""

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
    """向上查找带 `pyproject.toml` 的仓库根目录。"""
    current = (start or Path(__file__)).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("Could not locate repository root from the current path.")


@dataclass(frozen=True)
class DatabasePaths:
    """当前重构基线下的正式历史账本数据库路径。"""

    raw_market_day: Path
    raw_market_week: Path
    raw_market_month: Path
    market_base_day: Path
    market_base_week: Path
    market_base_month: Path
    malf_day: Path
    malf_week: Path
    malf_month: Path
    malf_legacy: Path
    structure: Path
    filter: Path
    alpha: Path
    position: Path
    portfolio_plan: Path
    trade_runtime: Path
    system: Path

    @property
    def raw_market(self) -> Path:
        """兼容旧调用：`raw_market` 默认仍指向 day 官方库。"""

        return self.raw_market_day

    @property
    def market_base(self) -> Path:
        """兼容旧调用：`market_base` 默认仍指向 day 官方库。"""

        return self.market_base_day

    @property
    def malf(self) -> Path:
        return self.malf_legacy

    def as_dict(self) -> dict[str, Path]:
        return {
            "raw_market_day": self.raw_market_day,
            "raw_market_week": self.raw_market_week,
            "raw_market_month": self.raw_market_month,
            "market_base_day": self.market_base_day,
            "market_base_week": self.market_base_week,
            "market_base_month": self.market_base_month,
            "malf_day": self.malf_day,
            "malf_week": self.malf_week,
            "malf_month": self.malf_month,
            "malf_legacy": self.malf_legacy,
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
    """跨模块共享的五根目录路径集合。"""

    repo_root: Path
    data_root: Path
    temp_root: Path
    report_root: Path
    validated_root: Path

    @property
    def databases(self) -> DatabasePaths:
        return DatabasePaths(
            raw_market_day=self.data_root / "raw" / "raw_market.duckdb",
            raw_market_week=self.data_root / "raw" / "raw_market_week.duckdb",
            raw_market_month=self.data_root / "raw" / "raw_market_month.duckdb",
            market_base_day=self.data_root / "base" / "market_base.duckdb",
            market_base_week=self.data_root / "base" / "market_base_week.duckdb",
            market_base_month=self.data_root / "base" / "market_base_month.duckdb",
            malf_day=self.data_root / "malf" / "malf_day.duckdb",
            malf_week=self.data_root / "malf" / "malf_week.duckdb",
            malf_month=self.data_root / "malf" / "malf_month.duckdb",
            malf_legacy=self.data_root / "malf" / "malf.duckdb",
            structure=self.data_root / "structure" / "structure.duckdb",
            filter=self.data_root / "filter" / "filter.duckdb",
            alpha=self.data_root / "alpha" / "alpha.duckdb",
            position=self.data_root / "position" / "position.duckdb",
            portfolio_plan=self.data_root / "portfolio_plan" / "portfolio_plan.duckdb",
            trade_runtime=self.data_root / "trade" / "trade_runtime.duckdb",
            system=self.data_root / "system" / "system.duckdb",
        )

    def module_temp_root(self, module_name: str) -> Path:
        """返回正式模块对应的临时工作目录。"""
        _validate_module_name(module_name)
        return self.temp_root / module_name

    def module_report_root(self, module_name: str) -> Path:
        """返回正式模块对应的报告目录。"""
        _validate_module_name(module_name)
        return self.report_root / module_name

    def module_validated_root(self, module_name: str) -> Path:
        """返回正式模块对应的验证快照目录。"""
        _validate_module_name(module_name)
        return self.validated_root / module_name

    def ensure_directories(self) -> None:
        """创建五根目录、账本父目录和模块级产物目录。"""
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
    """解析当前仓库五根目录设置。

    当显式传入 `repo_root` 时，应以该值为准，不允许再被环境变量里的
    `LIFESPAN_REPO_ROOT` 抢走；否则测试和子工作区会被外层 shell 污染。
    """

    if repo_root is not None:
        resolved_repo_root = Path(repo_root).resolve()
    else:
        resolved_repo_root = Path(os.getenv("LIFESPAN_REPO_ROOT", discover_repo_root())).resolve()
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
