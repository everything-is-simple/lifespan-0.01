"""TdxQuant 日更原始事实桥接的最小适配层。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Protocol

import pandas as pd


@dataclass(frozen=True)
class TdxQuantInstrumentInfo:
    code: str
    name: str
    asset_type: str = "stock"


@dataclass(frozen=True)
class TdxQuantDailyBar:
    code: str
    name: str
    asset_type: str
    trade_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    amount: float | None


class TdxQuantClient(Protocol):
    """卡 19 runner 依赖的最小 TdxQuant 客户端契约。"""

    def get_instrument_info(self, code: str) -> TdxQuantInstrumentInfo:
        ...

    def get_daily_bars(
        self,
        *,
        code: str,
        end_trade_date: date,
        count: int,
        dividend_type: str = "none",
    ) -> tuple[TdxQuantDailyBar, ...]:
        ...

    def close(self) -> None:
        ...


class RuntimeTdxQuantClient:
    """把官方 `tqcenter.tq` 包装成仓内可测试的最小桥接接口。"""

    def __init__(self, strategy_path: Path | str) -> None:
        try:
            from tqcenter import tq
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "tqcenter is not available on PYTHONPATH; cannot open TdxQuant runtime client."
            ) from exc
        self._strategy_path = Path(strategy_path)
        self._tq = tq
        self._tq.initialize(str(self._strategy_path))

    def close(self) -> None:
        try:
            self._tq.close()
        except Exception:
            pass

    def get_instrument_info(self, code: str) -> TdxQuantInstrumentInfo:
        raw_info = self._tq.get_stock_info(code)
        if not isinstance(raw_info, dict):
            raise ValueError(f"TdxQuant get_stock_info returned invalid payload for {code}")
        name = _pick_first_non_empty(
            raw_info,
            "Name",
            "name",
            "StockName",
            "stock_name",
            "证券名称",
        )
        return TdxQuantInstrumentInfo(
            code=code,
            name=name or code,
            asset_type="stock",
        )

    def get_daily_bars(
        self,
        *,
        code: str,
        end_trade_date: date,
        count: int,
        dividend_type: str = "none",
    ) -> tuple[TdxQuantDailyBar, ...]:
        payload = self._tq.get_market_data(
            stock_list=[code],
            period="1d",
            count=count,
            end_time=end_trade_date.strftime("%Y%m%d") + "150000",
            dividend_type=dividend_type,
        )
        if not isinstance(payload, dict):
            raise ValueError(f"TdxQuant get_market_data returned invalid payload for {code}")
        info = self.get_instrument_info(code)
        frame = _merge_market_data_frames(payload, code=code)
        if frame.empty:
            return ()
        rows: list[TdxQuantDailyBar] = []
        for record in frame.to_dict(orient="records"):
            rows.append(
                TdxQuantDailyBar(
                    code=code,
                    name=info.name,
                    asset_type=info.asset_type,
                    trade_date=pd.Timestamp(record["trade_date"]).date(),
                    open=_normalize_optional_float(record.get("open")),
                    high=_normalize_optional_float(record.get("high")),
                    low=_normalize_optional_float(record.get("low")),
                    close=_normalize_optional_float(record.get("close")),
                    volume=_normalize_optional_float(record.get("volume")),
                    amount=_normalize_optional_float(record.get("amount")),
                )
            )
        return tuple(rows)


def open_tdxquant_client(strategy_path: Path | str) -> TdxQuantClient:
    """创建默认的 TdxQuant 运行时客户端。"""

    return RuntimeTdxQuantClient(strategy_path)


def _merge_market_data_frames(payload: dict[str, object], *, code: str) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for source_key, target_key in (
        ("Open", "open"),
        ("High", "high"),
        ("Low", "low"),
        ("Close", "close"),
        ("Volume", "volume"),
        ("Amount", "amount"),
    ):
        frame = payload.get(source_key)
        series = _extract_series(frame, code=code)
        if series is None:
            continue
        candidate = series.rename(target_key).to_frame().reset_index()
        candidate = candidate.rename(columns={candidate.columns[0]: "trade_date"})
        merged = candidate if merged is None else merged.merge(candidate, on="trade_date", how="outer")
    if merged is None:
        return pd.DataFrame(
            columns=["trade_date", "open", "high", "low", "close", "volume", "amount"]
        )
    merged = merged.sort_values("trade_date").reset_index(drop=True)
    return merged


def _extract_series(frame: object, *, code: str) -> pd.Series | None:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return None
    if code in frame.columns:
        return frame[code]
    if len(frame.columns) == 1:
        return frame.iloc[:, 0]
    return None


def _pick_first_non_empty(payload: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        candidate = str(value).strip()
        if candidate:
            return candidate
    return None


def _normalize_optional_float(value: object | None) -> float | None:
    if value is None or value == "":
        return None
    return float(value)
