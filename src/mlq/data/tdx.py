"""解析 TDX 离线股票日线文本。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


_ADJUST_METHOD_FOLDER_MAP = {
    "Backward-Adjusted": "backward",
    "Forward-Adjusted": "forward",
    "Non-Adjusted": "none",
}


@dataclass(frozen=True)
class TdxStockDailyBar:
    code: str
    name: str
    trade_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    amount: float | None


@dataclass(frozen=True)
class TdxParsedStockFile:
    code: str
    name: str
    adjust_method: str
    header: str
    rows: tuple[TdxStockDailyBar, ...]


def resolve_adjust_method_folder(adjust_method: str) -> str:
    """把标准复权方式映射到离线目录名。"""

    normalized = adjust_method.strip().lower()
    mapping = {
        "backward": "Backward-Adjusted",
        "forward": "Forward-Adjusted",
        "none": "Non-Adjusted",
    }
    if normalized not in mapping:
        raise ValueError(f"Unsupported adjust method: {adjust_method}")
    return mapping[normalized]


def resolve_adjust_method_name(folder_name: str) -> str:
    """把离线目录名映射回标准复权方式。"""

    if folder_name not in _ADJUST_METHOD_FOLDER_MAP:
        raise ValueError(f"Unsupported TDX folder: {folder_name}")
    return _ADJUST_METHOD_FOLDER_MAP[folder_name]


def parse_tdx_stock_file(path: Path) -> TdxParsedStockFile:
    """读取单个 TDX 股票日线文本。"""

    lines = path.read_text(encoding="gbk").splitlines()
    if len(lines) < 3:
        raise ValueError(f"Unexpected TDX file format: {path}")
    header = lines[0].strip()
    header_parts = header.split()
    if len(header_parts) < 2:
        raise ValueError(f"Cannot parse TDX header: {header}")
    code = _normalize_code_from_filename(path)
    name = header_parts[1].strip()
    adjust_method = resolve_adjust_method_name(path.parent.name)
    rows: list[TdxStockDailyBar] = []
    for raw_line in lines[2:]:
        line = raw_line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split("\t") if part.strip()]
        if len(parts) < 7:
            continue
        rows.append(
            TdxStockDailyBar(
                code=code,
                name=name,
                trade_date=date.fromisoformat(parts[0].replace("/", "-")),
                open=_parse_float(parts[1]),
                high=_parse_float(parts[2]),
                low=_parse_float(parts[3]),
                close=_parse_float(parts[4]),
                volume=_parse_float(parts[5]),
                amount=_parse_float(parts[6]),
            )
        )
    return TdxParsedStockFile(
        code=code,
        name=name,
        adjust_method=adjust_method,
        header=header,
        rows=tuple(rows),
    )


def _normalize_code_from_filename(path: Path) -> str:
    stem = path.stem
    if "#" not in stem:
        raise ValueError(f"Unexpected TDX file name: {path.name}")
    exchange, code = stem.split("#", 1)
    return f"{code}.{exchange}"


def _parse_float(value: str) -> float | None:
    candidate = value.strip()
    if candidate == "":
        return None
    return float(candidate)
