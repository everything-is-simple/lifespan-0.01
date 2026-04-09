from pathlib import Path

import pytest

from mlq.core.paths import FORMAL_MODULES, default_settings


def test_default_settings_use_expected_sibling_roots(tmp_path: Path) -> None:
    repo_root = tmp_path / "lifespan-0.01"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text("[project]\nname='lifespan-0.01'\n", encoding="utf-8")

    settings = default_settings(repo_root=repo_root)

    assert settings.repo_root == repo_root.resolve()
    assert settings.data_root == (tmp_path / "Lifespan-data").resolve()
    assert settings.temp_root == (tmp_path / "Lifespan-temp").resolve()
    assert settings.report_root == (tmp_path / "Lifespan-report").resolve()
    assert settings.validated_root == (tmp_path / "Lifespan-Validated").resolve()
    assert set(settings.databases.as_dict()) == {
        "raw_market",
        "market_base",
        "malf",
        "structure",
        "filter",
        "alpha",
        "position",
        "portfolio_plan",
        "trade_runtime",
        "system",
    }


def test_ensure_directories_builds_formal_module_workspaces(tmp_path: Path) -> None:
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

    assert settings.data_root == (tmp_path / "custom-data").resolve()
    assert settings.temp_root == (tmp_path / "custom-temp").resolve()
    assert settings.report_root == (tmp_path / "custom-report").resolve()
    assert settings.validated_root == (tmp_path / "custom-validated").resolve()
