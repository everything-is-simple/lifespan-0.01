"""TdxQuant 日更原始事实桥接的最小适配层。"""

from __future__ import annotations

import json
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
    market_type: str | None = None
    security_type: str | None = None
    suspension_status: str | None = None
    risk_warning_status: str | None = None
    delisting_status: str | None = None
    is_suspended_or_unresumed: bool = False
    is_risk_warning_excluded: bool = False
    is_delisting_arrangement: bool = False
    raw_payload_json: str | None = None


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
        market_type = _normalize_market_type(
            _pick_first_non_empty(
                raw_info,
                "MarketType",
                "market_type",
                "Market",
                "market",
                "Exchange",
                "exchange",
                "市场类型",
            ),
            code=code,
        )
        security_type = _normalize_security_type(
            _pick_first_non_empty(
                raw_info,
                "SecurityType",
                "security_type",
                "Type",
                "type",
                "SecuType",
                "secu_type",
                "证券类型",
                "证券类别",
            ),
            name=name or code,
        )
        suspension_status = _normalize_status_label(
            _pick_first_non_empty(
                raw_info,
                "SuspendStatus",
                "suspend_status",
                "TradingStatus",
                "trading_status",
                "TradeStatus",
                "trade_status",
                "Status",
                "status",
                "停复牌情况",
                "停牌状态",
            )
        )
        risk_warning_status = _normalize_status_label(
            _pick_first_non_empty(
                raw_info,
                "RiskWarningStatus",
                "risk_warning_status",
                "RiskWarning",
                "risk_warning",
                "SpecialTreatment",
                "special_treatment",
                "ST",
                "st",
                "风险警示",
            )
        )
        delisting_status = _normalize_status_label(
            _pick_first_non_empty(
                raw_info,
                "DelistingStatus",
                "delisting_status",
                "Delist",
                "delist",
                "ListingStatus",
                "listing_status",
                "退市整理",
            )
        )
        return TdxQuantInstrumentInfo(
            code=code,
            name=name or code,
            asset_type="stock",
            market_type=market_type,
            security_type=security_type,
            suspension_status=suspension_status,
            risk_warning_status=risk_warning_status,
            delisting_status=delisting_status,
            is_suspended_or_unresumed=_is_suspended_or_unresumed(raw_info, suspension_status=suspension_status),
            is_risk_warning_excluded=_is_risk_warning_excluded(
                raw_info,
                risk_warning_status=risk_warning_status,
                name=name or code,
            ),
            is_delisting_arrangement=_is_delisting_arrangement(
                raw_info,
                delisting_status=delisting_status,
                name=name or code,
            ),
            raw_payload_json=json.dumps(raw_info, ensure_ascii=False, sort_keys=True),
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


def _normalize_status_label(value: str | None) -> str | None:
    if value is None:
        return None
    candidate = str(value).strip()
    return candidate or None


def _normalize_market_type(value: str | None, *, code: str) -> str | None:
    if value is not None and str(value).strip():
        return _normalize_token(str(value))
    if "." in code:
        return _normalize_token(code.rsplit(".", 1)[1])
    return None


def _normalize_security_type(value: str | None, *, name: str) -> str | None:
    if value is not None and str(value).strip():
        return _normalize_token(str(value))
    normalized_name = str(name).strip().upper()
    if "ETF" in normalized_name:
        return "etf"
    return None


def _normalize_token(value: str) -> str:
    return (
        str(value)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )


def _is_suspended_or_unresumed(
    payload: dict[str, object],
    *,
    suspension_status: str | None,
) -> bool:
    if _pick_truthy_value(
        payload,
        "IsSuspended",
        "is_suspended",
        "Suspended",
        "suspended",
        "IsHalted",
        "is_halted",
    ):
        return True
    return _contains_any(
        suspension_status,
        ("suspend", "suspended", "halt", "halted", "停牌", "未复牌"),
    )


def _is_risk_warning_excluded(
    payload: dict[str, object],
    *,
    risk_warning_status: str | None,
    name: str,
) -> bool:
    if _pick_truthy_value(
        payload,
        "IsRiskWarning",
        "is_risk_warning",
        "RiskWarningFlag",
        "risk_warning_flag",
        "IsST",
        "is_st",
    ):
        return True
    if _contains_any(
        risk_warning_status,
        ("st", "*st", "risk_warning", "special_treatment", "风险警示"),
    ):
        return True
    normalized_name = str(name).strip().upper()
    return normalized_name.startswith("ST") or normalized_name.startswith("*ST")


def _is_delisting_arrangement(
    payload: dict[str, object],
    *,
    delisting_status: str | None,
    name: str,
) -> bool:
    if _pick_truthy_value(
        payload,
        "IsDelisting",
        "is_delisting",
        "IsDelistingArrangement",
        "is_delisting_arrangement",
    ):
        return True
    if _contains_any(
        delisting_status,
        ("delist", "delisting", "退市"),
    ):
        return True
    return "退市" in str(name)


def _pick_truthy_value(payload: dict[str, object], *keys: str) -> bool:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "y"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
    return False


def _contains_any(value: str | None, candidates: tuple[str, ...]) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().lower()
    if not normalized:
        return False
    return any(candidate.lower() in normalized for candidate in candidates)
