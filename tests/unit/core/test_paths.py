"""覆盖五根目录解析与环境变量覆盖口径。"""

import os
from pathlib import Path

import pytest

from mlq.core.paths import FORMAL_MODULES, default_settings


def _normalized_path_string(path: Path) -> str:
    """统一 Windows 路径比较口径，避免 `\\?\` 前缀干扰断言。"""

    text = str(path.resolve())
    if text.startswith("\\\\?\\"):
        text = text[4:]
    return os.path.normcase(os.path.normpath(text))


def test_default_settings_use_expected_sibling_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for env_name in (
        "LIFESPAN_REPO_ROOT",
        "LIFESPAN_DATA_ROOT",
        "LIFESPAN_TEMP_ROOT",
        "LIFESPAN_REPORT_ROOT",
        "LIFESPAN_VALIDATED_ROOT",
    ):
        monkeypatch.delenv(env_name, raising=False)

    repo_root = tmp_path / "lifespan-0.01"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text("[project]\nname='lifespan-0.01'\n", encoding="utf-8")

    settings = default_settings(repo_root=repo_root)

    assert _normalized_path_string(settings.repo_root) == _normalized_path_string(repo_root)
    assert _normalized_path_string(settings.data_root) == _normalized_path_string(tmp_path / "Lifespan-data")
    assert _normalized_path_string(settings.temp_root) == _normalized_path_string(tmp_path / "Lifespan-temp")
    assert _normalized_path_string(settings.report_root) == _normalized_path_string(tmp_path / "Lifespan-report")
    assert _normalized_path_string(settings.validated_root) == _normalized_path_string(tmp_path / "Lifespan-Validated")
    assert _normalized_path_string(settings.databases.raw_market) == _normalized_path_string(
        tmp_path / "Lifespan-data" / "raw" / "raw_market.duckdb"
    )
    assert _normalized_path_string(settings.databases.market_base) == _normalized_path_string(
        tmp_path / "Lifespan-data" / "base" / "market_base.duckdb"
    )
    assert set(settings.databases.as_dict()) == {
        "raw_market_day",
        "raw_market_week",
        "raw_market_month",
        "market_base_day",
        "market_base_week",
        "market_base_month",
        "malf",
        "structure",
        "filter",
        "alpha",
        "position",
        "portfolio_plan",
        "trade_runtime",
        "system",
    }


def test_ensure_directories_builds_formal_module_workspaces(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for env_name in (
        "LIFESPAN_REPO_ROOT",
        "LIFESPAN_DATA_ROOT",
        "LIFESPAN_TEMP_ROOT",
        "LIFESPAN_REPORT_ROOT",
        "LIFESPAN_VALIDATED_ROOT",
    ):
        monkeypatch.delenv(env_name, raising=False)

    repo_root = tmp_path / "lifespan-0.01"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text("[project]\nname='lifespan-0.01'\n", encoding="utf-8")

    settings = default_settings(repo_root=repo_root)
    settings.ensure_directories()

    for database_path in settings.databases.as_dict().values():
        assert database_path.parent.is_dir()
    for module_name in FORMAL_MODULES:
        assert settings.module_temp_root(module_name).is_dir()
        assert settings.module_report_root(module_name).is_dir()
        assert settings.module_validated_root(module_name).is_dir()


def test_env_vars_can_override_workspace_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text("[project]\nname='lifespan-0.01'\n", encoding="utf-8")

    monkeypatch.setenv("LIFESPAN_DATA_ROOT", str(tmp_path / "custom-data"))
    monkeypatch.setenv("LIFESPAN_TEMP_ROOT", str(tmp_path / "custom-temp"))
    monkeypatch.setenv("LIFESPAN_REPORT_ROOT", str(tmp_path / "custom-report"))
    monkeypatch.setenv("LIFESPAN_VALIDATED_ROOT", str(tmp_path / "custom-validated"))

    settings = default_settings(repo_root=repo_root)

    assert _normalized_path_string(settings.data_root) == _normalized_path_string(tmp_path / "custom-data")
    assert _normalized_path_string(settings.temp_root) == _normalized_path_string(tmp_path / "custom-temp")
    assert _normalized_path_string(settings.report_root) == _normalized_path_string(tmp_path / "custom-report")
    assert _normalized_path_string(settings.validated_root) == _normalized_path_string(tmp_path / "custom-validated")


def test_explicit_repo_root_wins_over_repo_root_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = tmp_path / "explicit-repo"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text("[project]\nname='lifespan-0.01'\n", encoding="utf-8")

    monkeypatch.setenv("LIFESPAN_REPO_ROOT", str(tmp_path / "wrong-repo"))

    settings = default_settings(repo_root=repo_root)

    assert _normalized_path_string(settings.repo_root) == _normalized_path_string(repo_root)
