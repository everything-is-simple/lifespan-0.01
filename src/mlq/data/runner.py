"""执行 `TDX -> raw_market -> market_base` 的最小正式桥接。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import (
    MARKET_BASE_STOCK_DAILY_TABLE,
    RAW_STOCK_DAILY_BAR_TABLE,
    RAW_STOCK_FILE_REGISTRY_TABLE,
    bootstrap_market_base_ledger,
    bootstrap_raw_market_ledger,
    market_base_ledger_path,
    raw_market_ledger_path,
)
from mlq.data.tdx import parse_tdx_stock_file, resolve_adjust_method_folder


DEFAULT_TDX_SOURCE_ROOT: Final[Path] = Path("H:/tdx_offline_Data")
DEFAULT_ASSET_TYPE: Final[str] = "stock"


@dataclass(frozen=True)
class TdxStockRawIngestSummary:
    run_id: str
    asset_type: str
    adjust_method: str
    candidate_file_count: int
    ingested_file_count: int
    skipped_unchanged_file_count: int
    bar_inserted_count: int
    bar_reused_count: int
    bar_rematerialized_count: int
    raw_market_path: str
    source_root: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MarketBaseBuildSummary:
    run_id: str
    adjust_method: str
    source_row_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    raw_market_path: str
    market_base_path: str
    raw_table: str
    market_table: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def run_tdx_stock_raw_ingest(
    *,
    settings: WorkspaceRoots | None = None,
    source_root: Path | str | None = None,
    adjust_method: str = "backward",
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> TdxStockRawIngestSummary:
    """把 TDX 离线股票日线增量 ingest 到正式 `raw_market`。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    resolved_source_root = Path(source_root or DEFAULT_TDX_SOURCE_ROOT)
    folder_path = resolved_source_root / DEFAULT_ASSET_TYPE / resolve_adjust_method_folder(adjust_method)
    if not folder_path.exists():
        raise FileNotFoundError(f"Missing TDX source directory: {folder_path}")

    normalized_instruments = _normalize_instruments(instruments)
    normalized_limit = max(int(limit), 1)
    materialization_run_id = run_id or _build_run_id(prefix="raw-stock-ingest")
    candidate_files = [
        path
        for path in sorted(folder_path.glob("*.txt"))
        if _match_instrument_filter(path, normalized_instruments)
    ][:normalized_limit]

    connection = duckdb.connect(str(raw_market_ledger_path(workspace)))
    try:
        ingested_file_count = 0
        skipped_unchanged_file_count = 0
        bar_inserted_count = 0
        bar_reused_count = 0
        bar_rematerialized_count = 0
        for path in candidate_files:
            stat_result = path.stat()
            source_size_bytes = stat_result.st_size
            source_mtime_utc = datetime.fromtimestamp(stat_result.st_mtime).replace(microsecond=0)
            parsed = parse_tdx_stock_file(path)
            file_nk = _build_file_nk(
                asset_type=DEFAULT_ASSET_TYPE,
                adjust_method=adjust_method,
                code=parsed.code,
                name=parsed.name,
                source_path=path,
            )
            registry_row = connection.execute(
                f"""
                SELECT source_size_bytes, source_mtime_utc
                FROM {RAW_STOCK_FILE_REGISTRY_TABLE}
                WHERE file_nk = ?
                """,
                [file_nk],
            ).fetchone()
            if registry_row is not None and int(registry_row[0]) == source_size_bytes and _normalize_timestamp(
                registry_row[1]
            ) == source_mtime_utc:
                skipped_unchanged_file_count += 1
                continue

            ingested_file_count += 1
            for row in parsed.rows:
                action = _upsert_raw_bar(
                    connection,
                    file_nk=file_nk,
                    adjust_method=adjust_method,
                    row=row,
                    source_path=path,
                    source_mtime_utc=source_mtime_utc,
                    run_id=materialization_run_id,
                )
                if action == "inserted":
                    bar_inserted_count += 1
                elif action == "reused":
                    bar_reused_count += 1
                else:
                    bar_rematerialized_count += 1
            _upsert_file_registry(
                connection,
                file_nk=file_nk,
                adjust_method=adjust_method,
                parsed_name=parsed.name,
                parsed_code=parsed.code,
                source_path=path,
                source_size_bytes=source_size_bytes,
                source_mtime_utc=source_mtime_utc,
                source_line_count=len(parsed.rows),
                source_header=parsed.header,
                run_id=materialization_run_id,
            )

        summary = TdxStockRawIngestSummary(
            run_id=materialization_run_id,
            asset_type=DEFAULT_ASSET_TYPE,
            adjust_method=adjust_method,
            candidate_file_count=len(candidate_files),
            ingested_file_count=ingested_file_count,
            skipped_unchanged_file_count=skipped_unchanged_file_count,
            bar_inserted_count=bar_inserted_count,
            bar_reused_count=bar_reused_count,
            bar_rematerialized_count=bar_rematerialized_count,
            raw_market_path=str(raw_market_ledger_path(workspace)),
            source_root=str(resolved_source_root),
        )
        _write_summary(summary.as_dict(), summary_path)
        return summary
    finally:
        connection.close()


def run_market_base_build(
    *,
    settings: WorkspaceRoots | None = None,
    adjust_method: str = "backward",
    instruments: list[str] | tuple[str, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    limit: int = 1000,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> MarketBaseBuildSummary:
    """从官方 `raw_market` 物化 `market_base.stock_daily_adjusted`。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    normalized_start_date = _coerce_date(start_date)
    normalized_end_date = _coerce_date(end_date)
    normalized_limit = max(int(limit), 1)
    materialization_run_id = run_id or _build_run_id(prefix="market-base")

    raw_connection = duckdb.connect(str(raw_market_ledger_path(workspace)), read_only=True)
    market_connection = duckdb.connect(str(market_base_ledger_path(workspace)))
    try:
        rows = _load_raw_rows(
            raw_connection,
            adjust_method=adjust_method,
            instruments=normalized_instruments,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
            limit=normalized_limit,
        )
        inserted_count = 0
        reused_count = 0
        rematerialized_count = 0
        for row in rows:
            action = _upsert_market_base_row(
                market_connection,
                row=row,
                run_id=materialization_run_id,
            )
            if action == "inserted":
                inserted_count += 1
            elif action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

        summary = MarketBaseBuildSummary(
            run_id=materialization_run_id,
            adjust_method=adjust_method,
            source_row_count=len(rows),
            inserted_count=inserted_count,
            reused_count=reused_count,
            rematerialized_count=rematerialized_count,
            raw_market_path=str(raw_market_ledger_path(workspace)),
            market_base_path=str(market_base_ledger_path(workspace)),
            raw_table=RAW_STOCK_DAILY_BAR_TABLE,
            market_table=MARKET_BASE_STOCK_DAILY_TABLE,
        )
        _write_summary(summary.as_dict(), summary_path)
        return summary
    finally:
        raw_connection.close()
        market_connection.close()


def _normalize_instruments(instruments: list[str] | tuple[str, ...] | None) -> set[str]:
    normalized: set[str] = set()
    for instrument in instruments or ():
        candidate = str(instrument).strip().upper()
        if not candidate:
            continue
        normalized.add(candidate)
        if "." in candidate:
            normalized.add(candidate.split(".", 1)[0])
    return normalized


def _match_instrument_filter(path: Path, normalized_instruments: set[str]) -> bool:
    if not normalized_instruments:
        return True
    stem = path.stem
    if "#" not in stem:
        return False
    exchange, code = stem.split("#", 1)
    return code.upper() in normalized_instruments or f"{code}.{exchange}".upper() in normalized_instruments


def _build_file_nk(
    *,
    asset_type: str,
    adjust_method: str,
    code: str,
    name: str,
    source_path: Path,
) -> str:
    return "|".join([asset_type, adjust_method, code, name, str(source_path)])


def _build_bar_nk(*, code: str, trade_date: date, adjust_method: str) -> str:
    return "|".join([code, trade_date.isoformat(), adjust_method])


def _upsert_file_registry(
    connection: duckdb.DuckDBPyConnection,
    *,
    file_nk: str,
    adjust_method: str,
    parsed_name: str,
    parsed_code: str,
    source_path: Path,
    source_size_bytes: int,
    source_mtime_utc: datetime,
    source_line_count: int,
    source_header: str,
    run_id: str,
) -> None:
    existing = connection.execute(
        f"SELECT file_nk FROM {RAW_STOCK_FILE_REGISTRY_TABLE} WHERE file_nk = ?",
        [file_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {RAW_STOCK_FILE_REGISTRY_TABLE} (
                file_nk,
                asset_type,
                adjust_method,
                code,
                name,
                source_path,
                source_size_bytes,
                source_mtime_utc,
                source_line_count,
                source_header,
                last_ingested_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                file_nk,
                DEFAULT_ASSET_TYPE,
                adjust_method,
                parsed_code,
                parsed_name,
                str(source_path),
                source_size_bytes,
                source_mtime_utc,
                source_line_count,
                source_header,
                run_id,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {RAW_STOCK_FILE_REGISTRY_TABLE}
        SET
            code = ?,
            name = ?,
            source_path = ?,
            source_size_bytes = ?,
            source_mtime_utc = ?,
            source_line_count = ?,
            source_header = ?,
            last_ingested_run_id = ?,
            last_ingested_at = CURRENT_TIMESTAMP
        WHERE file_nk = ?
        """,
        [
            parsed_code,
            parsed_name,
            str(source_path),
            source_size_bytes,
            source_mtime_utc,
            source_line_count,
            source_header,
            run_id,
            file_nk,
        ],
    )


def _upsert_raw_bar(
    connection: duckdb.DuckDBPyConnection,
    *,
    file_nk: str,
    adjust_method: str,
    row,
    source_path: Path,
    source_mtime_utc: datetime,
    run_id: str,
) -> str:
    bar_nk = _build_bar_nk(code=row.code, trade_date=row.trade_date, adjust_method=adjust_method)
    existing = connection.execute(
        f"""
        SELECT
            name, open, high, low, close, volume, amount, source_path, source_mtime_utc, first_seen_run_id
        FROM {RAW_STOCK_DAILY_BAR_TABLE}
        WHERE bar_nk = ?
        """,
        [bar_nk],
    ).fetchone()
    fingerprint = (
        row.name,
        _normalize_float(row.open),
        _normalize_float(row.high),
        _normalize_float(row.low),
        _normalize_float(row.close),
        _normalize_float(row.volume),
        _normalize_float(row.amount),
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {RAW_STOCK_DAILY_BAR_TABLE} (
                bar_nk,
                source_file_nk,
                asset_type,
                code,
                name,
                trade_date,
                adjust_method,
                open,
                high,
                low,
                close,
                volume,
                amount,
                source_path,
                source_mtime_utc,
                first_seen_run_id,
                last_ingested_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                bar_nk,
                file_nk,
                DEFAULT_ASSET_TYPE,
                row.code,
                row.name,
                row.trade_date,
                adjust_method,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                row.amount,
                str(source_path),
                source_mtime_utc,
                run_id,
                run_id,
            ],
        )
        return "inserted"
    existing_fingerprint = (
        str(existing[0]),
        _normalize_float(existing[1]),
        _normalize_float(existing[2]),
        _normalize_float(existing[3]),
        _normalize_float(existing[4]),
        _normalize_float(existing[5]),
        _normalize_float(existing[6]),
    )
    first_seen_run_id = str(existing[9]) if existing[9] is not None else run_id
    connection.execute(
        f"""
        UPDATE {RAW_STOCK_DAILY_BAR_TABLE}
        SET
            source_file_nk = ?,
            name = ?,
            open = ?,
            high = ?,
            low = ?,
            close = ?,
            volume = ?,
            amount = ?,
            source_path = ?,
            source_mtime_utc = ?,
            first_seen_run_id = ?,
            last_ingested_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE bar_nk = ?
        """,
        [
            file_nk,
            row.name,
            row.open,
            row.high,
            row.low,
            row.close,
            row.volume,
            row.amount,
            str(source_path),
            source_mtime_utc,
            first_seen_run_id,
            run_id,
            bar_nk,
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _load_raw_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    adjust_method: str,
    instruments: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
    limit: int,
) -> list[dict[str, object]]:
    parameters: list[object] = [adjust_method]
    where_clauses = ["adjust_method = ?"]
    if start_date is not None:
        where_clauses.append("trade_date >= ?")
        parameters.append(start_date)
    if end_date is not None:
        where_clauses.append("trade_date <= ?")
        parameters.append(end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"code IN ({placeholders})")
        parameters.extend(instruments)
    rows = connection.execute(
        f"""
        SELECT
            bar_nk,
            code,
            COALESCE(name, code) AS name,
            trade_date,
            adjust_method,
            open,
            high,
            low,
            close,
            volume,
            amount
        FROM {RAW_STOCK_DAILY_BAR_TABLE}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY code, trade_date
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        {
            "bar_nk": str(row[0]),
            "code": str(row[1]),
            "name": str(row[2]),
            "trade_date": _coerce_date(row[3]),
            "adjust_method": str(row[4]),
            "open": _normalize_float(row[5]),
            "high": _normalize_float(row[6]),
            "low": _normalize_float(row[7]),
            "close": _normalize_float(row[8]),
            "volume": _normalize_float(row[9]),
            "amount": _normalize_float(row[10]),
        }
        for row in rows
    ]


def _upsert_market_base_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    row: dict[str, object],
    run_id: str,
) -> str:
    daily_bar_nk = _build_bar_nk(
        code=str(row["code"]),
        trade_date=_coerce_date(row["trade_date"]),
        adjust_method=str(row["adjust_method"]),
    )
    existing = connection.execute(
        f"""
        SELECT
            name, open, high, low, close, volume, amount, source_bar_nk, first_seen_run_id
        FROM {MARKET_BASE_STOCK_DAILY_TABLE}
        WHERE code = ?
          AND trade_date = ?
          AND adjust_method = ?
        """,
        [row["code"], row["trade_date"], row["adjust_method"]],
    ).fetchone()
    fingerprint = (
        str(row["name"]),
        _normalize_float(row["open"]),
        _normalize_float(row["high"]),
        _normalize_float(row["low"]),
        _normalize_float(row["close"]),
        _normalize_float(row["volume"]),
        _normalize_float(row["amount"]),
        str(row["bar_nk"]),
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {MARKET_BASE_STOCK_DAILY_TABLE} (
                daily_bar_nk,
                code,
                name,
                trade_date,
                adjust_method,
                open,
                high,
                low,
                close,
                volume,
                amount,
                source_bar_nk,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                daily_bar_nk,
                row["code"],
                row["name"],
                row["trade_date"],
                row["adjust_method"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["amount"],
                row["bar_nk"],
                run_id,
                run_id,
            ],
        )
        return "inserted"
    existing_fingerprint = (
        str(existing[0]) if existing[0] is not None else str(row["name"]),
        _normalize_float(existing[1]),
        _normalize_float(existing[2]),
        _normalize_float(existing[3]),
        _normalize_float(existing[4]),
        _normalize_float(existing[5]),
        _normalize_float(existing[6]),
        str(existing[7]) if existing[7] is not None else "",
    )
    first_seen_run_id = str(existing[8]) if existing[8] is not None else run_id
    connection.execute(
        f"""
        UPDATE {MARKET_BASE_STOCK_DAILY_TABLE}
        SET
            daily_bar_nk = ?,
            name = ?,
            open = ?,
            high = ?,
            low = ?,
            close = ?,
            volume = ?,
            amount = ?,
            source_bar_nk = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE code = ?
          AND trade_date = ?
          AND adjust_method = ?
        """,
        [
            daily_bar_nk,
            row["name"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            row["amount"],
            row["bar_nk"],
            first_seen_run_id,
            run_id,
            row["code"],
            row["trade_date"],
            row["adjust_method"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _coerce_date(value: str | date | datetime | object | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(microsecond=0)
        return value.astimezone().replace(tzinfo=None, microsecond=0)
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(microsecond=0)
    return parsed.astimezone().replace(tzinfo=None, microsecond=0)


def _normalize_float(value: object | None) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _build_run_id(*, prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"


def _write_summary(payload: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
