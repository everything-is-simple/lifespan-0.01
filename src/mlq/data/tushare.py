"""Tushare 历史 objective source 的最小适配层。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

import pandas as pd


@dataclass(frozen=True)
class TushareStockBasicRow:
    ts_code: str
    name: str
    market: str | None
    exchange: str | None
    list_status: str | None
    list_date: date | None
    delist_date: date | None


@dataclass(frozen=True)
class TushareSuspendRow:
    ts_code: str
    trade_date: date
    suspend_timing: str | None
    suspend_type: str | None


@dataclass(frozen=True)
class TushareStockStRow:
    ts_code: str
    name: str | None
    trade_date: date
    type: str | None
    type_name: str | None


@dataclass(frozen=True)
class TushareNameChangeRow:
    ts_code: str
    name: str | None
    start_date: date | None
    end_date: date | None
    ann_date: date | None
    change_reason: str | None


class TushareClient(Protocol):
    """卡 71 runner 依赖的最小 Tushare 客户端契约。"""

    def list_stock_basic(self, *, exchange: str, list_status: str) -> tuple[TushareStockBasicRow, ...]:
        ...

    def list_suspend_d(self, *, trade_date: date) -> tuple[TushareSuspendRow, ...]:
        ...

    def list_stock_st(self, *, trade_date: date) -> tuple[TushareStockStRow, ...]:
        ...

    def list_namechange(self, *, ts_code: str) -> tuple[TushareNameChangeRow, ...]:
        ...

    def close(self) -> None:
        ...


class RuntimeTushareClient:
    """把官方 tushare pro 包装成仓内可测试的最小接口。"""

    def __init__(self, *, token: str | None = None, token_env_var: str = "TUSHARE_TOKEN") -> None:
        resolved_token = str(token or os.environ.get(token_env_var, "")).strip()
        if not resolved_token:
            raise ValueError(
                f"Tushare token is required; pass `token=` or set environment variable `{token_env_var}`."
            )
        try:
            import tushare as ts
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError("tushare package is not installed in the current environment.") from exc
        self._pro = ts.pro_api(resolved_token)

    def close(self) -> None:
        return None

    def list_stock_basic(self, *, exchange: str, list_status: str) -> tuple[TushareStockBasicRow, ...]:
        frame = self._pro.stock_basic(
            exchange=exchange,
            list_status=list_status,
            fields="ts_code,name,market,exchange,list_status,list_date,delist_date",
        )
        if not isinstance(frame, pd.DataFrame):
            raise ValueError("Tushare stock_basic returned invalid payload.")
        return tuple(
            TushareStockBasicRow(
                ts_code=str(row.get("ts_code", "")).strip().upper(),
                name=str(row.get("name", "")).strip(),
                market=_normalize_optional_str(row.get("market")),
                exchange=_normalize_optional_str(row.get("exchange")),
                list_status=_normalize_optional_str(row.get("list_status")),
                list_date=_normalize_optional_date(row.get("list_date")),
                delist_date=_normalize_optional_date(row.get("delist_date")),
            )
            for row in frame.to_dict(orient="records")
            if str(row.get("ts_code", "")).strip()
        )

    def list_suspend_d(self, *, trade_date: date) -> tuple[TushareSuspendRow, ...]:
        frame = self._pro.suspend_d(trade_date=trade_date.strftime("%Y%m%d"))
        if not isinstance(frame, pd.DataFrame):
            raise ValueError("Tushare suspend_d returned invalid payload.")
        return tuple(
            TushareSuspendRow(
                ts_code=str(row.get("ts_code", "")).strip().upper(),
                trade_date=_require_date(row.get("trade_date"), field_name="trade_date"),
                suspend_timing=_normalize_optional_str(row.get("suspend_timing")),
                suspend_type=_normalize_optional_str(row.get("suspend_type")),
            )
            for row in frame.to_dict(orient="records")
            if str(row.get("ts_code", "")).strip()
        )

    def list_stock_st(self, *, trade_date: date) -> tuple[TushareStockStRow, ...]:
        frame = self._pro.stock_st(trade_date=trade_date.strftime("%Y%m%d"))
        if not isinstance(frame, pd.DataFrame):
            raise ValueError("Tushare stock_st returned invalid payload.")
        return tuple(
            TushareStockStRow(
                ts_code=str(row.get("ts_code", "")).strip().upper(),
                name=_normalize_optional_str(row.get("name")),
                trade_date=_require_date(row.get("trade_date"), field_name="trade_date"),
                type=_normalize_optional_str(row.get("type")),
                type_name=_normalize_optional_str(row.get("type_name")),
            )
            for row in frame.to_dict(orient="records")
            if str(row.get("ts_code", "")).strip()
        )

    def list_namechange(self, *, ts_code: str) -> tuple[TushareNameChangeRow, ...]:
        frame = self._pro.namechange(ts_code=ts_code)
        if not isinstance(frame, pd.DataFrame):
            raise ValueError("Tushare namechange returned invalid payload.")
        return tuple(
            TushareNameChangeRow(
                ts_code=str(row.get("ts_code", "")).strip().upper(),
                name=_normalize_optional_str(row.get("name")),
                start_date=_normalize_optional_date(row.get("start_date")),
                end_date=_normalize_optional_date(row.get("end_date")),
                ann_date=_normalize_optional_date(row.get("ann_date")),
                change_reason=_normalize_optional_str(row.get("change_reason")),
            )
            for row in frame.to_dict(orient="records")
            if str(row.get("ts_code", "")).strip()
        )


def open_tushare_client(*, token: str | None = None, token_env_var: str = "TUSHARE_TOKEN") -> TushareClient:
    """创建默认 Tushare 运行时客户端。"""

    return RuntimeTushareClient(token=token, token_env_var=token_env_var)


def _normalize_optional_str(value: object | None) -> str | None:
    if value is None:
        return None
    candidate = str(value).strip()
    return candidate or None


def _normalize_optional_date(value: object | None) -> date | None:
    if value is None:
        return None
    candidate = str(value).strip()
    if not candidate or candidate.lower() == "nan":
        return None
    return _parse_date(candidate)


def _require_date(value: object | None, *, field_name: str) -> date:
    parsed = _normalize_optional_date(value)
    if parsed is None:
        raise ValueError(f"Tushare field `{field_name}` is required.")
    return parsed


def _parse_date(value: str) -> date:
    normalized = value.strip()
    if len(normalized) == 8 and normalized.isdigit():
        return datetime.strptime(normalized, "%Y%m%d").date()
    return datetime.fromisoformat(normalized).date()
