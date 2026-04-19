"""`market_base` 相关测试的共享搭建辅助。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from mlq.data.tdxquant import TdxQuantDailyBar, TdxQuantInstrumentInfo


def clear_workspace_env(monkeypatch) -> None:
    for env_name in (
        "LIFESPAN_REPO_ROOT",
        "LIFESPAN_DATA_ROOT",
        "LIFESPAN_TEMP_ROOT",
        "LIFESPAN_REPORT_ROOT",
        "LIFESPAN_VALIDATED_ROOT",
    ):
        monkeypatch.delenv(env_name, raising=False)


def bootstrap_repo_root(tmp_path: Path) -> Path:
    repo_root = tmp_path / "lifespan-0.01"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text("[project]\nname='lifespan-0.01'\n", encoding="utf-8")
    return repo_root


def write_tdx_asset_file(
    root: Path,
    *,
    asset_type: str,
    folder_name: str,
    code: str,
    exchange: str,
    name: str,
    rows: list[tuple[str, float, float, float, float, float, float]],
) -> None:
    folder = root / asset_type / folder_name
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{exchange}#{code}.txt"
    content_lines = [
        f"{code} {name} 日线 {'后复权' if folder_name == 'Backward-Adjusted' else '不复权'}",
        "      日期\t    开盘\t    最高\t    最低\t    收盘\t    成交量\t    成交额",
    ]
    for row in rows:
        content_lines.append(
            "\t".join(
                [
                    row[0],
                    f"{row[1]:.2f}",
                    f"{row[2]:.2f}",
                    f"{row[3]:.2f}",
                    f"{row[4]:.2f}",
                    f"{row[5]:.0f}",
                    f"{row[6]:.2f}",
                ]
            )
        )
    path.write_text("\n".join(content_lines) + "\n", encoding="gbk")


def write_tdx_stock_file(
    root: Path,
    *,
    folder_name: str,
    code: str,
    exchange: str,
    name: str,
    rows: list[tuple[str, float, float, float, float, float, float]],
) -> None:
    write_tdx_asset_file(
        root,
        asset_type="stock",
        folder_name=folder_name,
        code=code,
        exchange=exchange,
        name=name,
        rows=rows,
    )


class FakeTdxQuantClient:
    def __init__(
        self,
        *,
        infos: dict[str, TdxQuantInstrumentInfo],
        bars_by_code: dict[str, tuple[TdxQuantDailyBar, ...]],
        failing_codes: set[str] | None = None,
    ) -> None:
        self._infos = infos
        self._bars_by_code = bars_by_code
        self._failing_codes = failing_codes or set()
        self.closed = False

    def get_instrument_info(self, code: str) -> TdxQuantInstrumentInfo:
        if code in self._failing_codes:
            raise ValueError(f"mock failure for {code}")
        return self._infos[code]

    def get_daily_bars(
        self,
        *,
        code: str,
        end_trade_date: date,
        count: int,
        dividend_type: str = "none",
    ) -> tuple[TdxQuantDailyBar, ...]:
        if code in self._failing_codes:
            raise ValueError(f"mock failure for {code}")
        assert dividend_type == "none"
        assert count > 0
        assert end_trade_date.isoformat() == "2026-04-10"
        return self._bars_by_code[code]

    def close(self) -> None:
        self.closed = True
