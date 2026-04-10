"""执行 `TDX -> raw_market -> market_base` 的最小正式桥接。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb
import pandas as pd

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
RAW_INGEST_COMMIT_INTERVAL: Final[int] = 50
RAW_STAGE_RELATION_NAME: Final[str] = "_raw_stock_daily_stage"
MARKET_BASE_STAGE_TABLE: Final[str] = "stage_market_base"
MARKET_BASE_EXISTING_STAGE_TABLE: Final[str] = "stage_market_base_existing"
MARKET_BASE_FINAL_STAGE_TABLE: Final[str] = "stage_market_base_final"


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
    normalized_limit = _normalize_limit(limit)
    materialization_run_id = run_id or _build_run_id(prefix="raw-stock-ingest")
    matching_files = [
        path
        for path in sorted(folder_path.glob("*.txt"))
        if _match_instrument_filter(path, normalized_instruments)
    ]
    candidate_files = matching_files if normalized_limit is None else matching_files[:normalized_limit]

    connection = duckdb.connect(str(raw_market_ledger_path(workspace)))
    try:
        connection.execute("BEGIN TRANSACTION")
        ingested_file_count = 0
        skipped_unchanged_file_count = 0
        bar_inserted_count = 0
        bar_reused_count = 0
        bar_rematerialized_count = 0
        batched_file_count = 0
        for path in candidate_files:
            stat_result = path.stat()
            source_size_bytes = stat_result.st_size
            source_mtime_utc = datetime.fromtimestamp(stat_result.st_mtime).replace(microsecond=0)
            code = _resolve_code_from_filename(path)
            registry_row = connection.execute(
                f"""
                SELECT source_size_bytes, source_mtime_utc
                FROM {RAW_STOCK_FILE_REGISTRY_TABLE}
                WHERE asset_type = ?
                  AND adjust_method = ?
                  AND code = ?
                  AND source_path = ?
                """,
                [DEFAULT_ASSET_TYPE, adjust_method, code, str(path)],
            ).fetchone()
            if registry_row is not None and int(registry_row[0]) == source_size_bytes and _normalize_timestamp(
                registry_row[1]
            ) == source_mtime_utc:
                skipped_unchanged_file_count += 1
                continue

            parsed = parse_tdx_stock_file(path)
            file_nk = _build_file_nk(
                asset_type=DEFAULT_ASSET_TYPE,
                adjust_method=adjust_method,
                code=parsed.code,
                name=parsed.name,
                source_path=path,
            )
            inserted_count, reused_count, rematerialized_count = _replace_raw_bars_for_file(
                connection,
                file_nk=file_nk,
                adjust_method=adjust_method,
                parsed=parsed,
                source_path=path,
                source_mtime_utc=source_mtime_utc,
                run_id=materialization_run_id,
            )
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
            ingested_file_count += 1
            bar_inserted_count += inserted_count
            bar_reused_count += reused_count
            bar_rematerialized_count += rematerialized_count
            batched_file_count += 1
            if batched_file_count >= RAW_INGEST_COMMIT_INTERVAL:
                connection.execute("COMMIT")
                connection.execute("BEGIN TRANSACTION")
                batched_file_count = 0

        connection.execute("COMMIT")
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
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
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
    """从官方 `raw_market` 批量物化 `market_base.stock_daily_adjusted`。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    normalized_start_date = _coerce_date(start_date)
    normalized_end_date = _coerce_date(end_date)
    normalized_limit = _normalize_limit(limit)
    materialization_run_id = run_id or _build_run_id(prefix="market-base")

    market_connection = duckdb.connect(str(market_base_ledger_path(workspace)))
    try:
        _attach_raw_market_ledger(
            market_connection,
            raw_market_path=raw_market_ledger_path(workspace),
        )
        market_connection.execute("BEGIN TRANSACTION")
        _stage_market_base_rows(
            connection=market_connection,
            adjust_method=adjust_method,
            instruments=normalized_instruments,
            start_date=normalized_start_date,
            end_date=normalized_end_date,
            limit=normalized_limit,
        )
        source_row_count = int(
            market_connection.execute(f"SELECT COUNT(*) FROM {MARKET_BASE_STAGE_TABLE}").fetchone()[0]
        )
        inserted_count, reused_count, rematerialized_count = _count_market_base_actions(market_connection)
        _materialize_market_base_stage(
            market_connection,
            adjust_method=adjust_method,
            run_id=materialization_run_id,
            full_scope=_is_full_market_base_scope(
                instruments=normalized_instruments,
                start_date=normalized_start_date,
                end_date=normalized_end_date,
                limit=normalized_limit,
            ),
        )
        market_connection.execute("COMMIT")
        summary = MarketBaseBuildSummary(
            run_id=materialization_run_id,
            adjust_method=adjust_method,
            source_row_count=source_row_count,
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
    except Exception:
        try:
            market_connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        market_connection.close()


def _normalize_limit(limit: int | None) -> int | None:
    if limit is None:
        return None
    normalized = int(limit)
    if normalized <= 0:
        return None
    return normalized


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


def _resolve_code_from_filename(path: Path) -> str:
    stem = path.stem
    if "#" not in stem:
        raise ValueError(f"Unexpected TDX file name: {path.name}")
    exchange, code = stem.split("#", 1)
    return f"{code}.{exchange}"


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
        f"""
        SELECT file_nk
        FROM {RAW_STOCK_FILE_REGISTRY_TABLE}
        WHERE asset_type = ?
          AND adjust_method = ?
          AND code = ?
          AND source_path = ?
        """,
        [DEFAULT_ASSET_TYPE, adjust_method, parsed_code, str(source_path)],
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
            file_nk = ?,
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
            file_nk,
            parsed_name,
            str(source_path),
            source_size_bytes,
            source_mtime_utc,
            source_line_count,
            source_header,
            run_id,
            str(existing[0]),
        ],
    )


def _replace_raw_bars_for_file(
    connection: duckdb.DuckDBPyConnection,
    *,
    file_nk: str,
    adjust_method: str,
    parsed,
    source_path: Path,
    source_mtime_utc: datetime,
    run_id: str,
) -> tuple[int, int, int]:
    existing_rows = connection.execute(
        f"""
        SELECT
            bar_nk,
            name,
            open,
            high,
            low,
            close,
            volume,
            amount,
            first_seen_run_id,
            created_at
        FROM {RAW_STOCK_DAILY_BAR_TABLE}
        WHERE code = ?
          AND adjust_method = ?
        """,
        [parsed.code, adjust_method],
    ).fetchall()
    existing_by_bar_nk = {str(row[0]): row for row in existing_rows}
    now = datetime.now().replace(microsecond=0)
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    records: list[dict[str, object]] = []
    for row in parsed.rows:
        bar_nk = _build_bar_nk(code=row.code, trade_date=row.trade_date, adjust_method=adjust_method)
        existing = existing_by_bar_nk.get(bar_nk)
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
            inserted_count += 1
            first_seen_run_id = run_id
            created_at = now
        else:
            existing_fingerprint = (
                str(existing[1]),
                _normalize_float(existing[2]),
                _normalize_float(existing[3]),
                _normalize_float(existing[4]),
                _normalize_float(existing[5]),
                _normalize_float(existing[6]),
                _normalize_float(existing[7]),
            )
            if existing_fingerprint == fingerprint:
                reused_count += 1
            else:
                rematerialized_count += 1
            first_seen_run_id = str(existing[8]) if existing[8] is not None else run_id
            created_at = existing[9] if existing[9] is not None else now
        records.append(
            {
                "bar_nk": bar_nk,
                "source_file_nk": file_nk,
                "asset_type": DEFAULT_ASSET_TYPE,
                "code": row.code,
                "name": row.name,
                "trade_date": row.trade_date,
                "adjust_method": adjust_method,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
                "amount": row.amount,
                "source_path": str(source_path),
                "source_mtime_utc": source_mtime_utc,
                "first_seen_run_id": first_seen_run_id,
                "last_ingested_run_id": run_id,
                "created_at": created_at,
                "updated_at": now,
            }
        )
    connection.execute(
        f"""
        DELETE FROM {RAW_STOCK_DAILY_BAR_TABLE}
        WHERE code = ?
          AND adjust_method = ?
        """,
        [parsed.code, adjust_method],
    )
    if records:
        frame = pd.DataFrame.from_records(records)
        connection.register(RAW_STAGE_RELATION_NAME, frame)
        try:
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
                    last_ingested_run_id,
                    created_at,
                    updated_at
                )
                SELECT
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
                    last_ingested_run_id,
                    created_at,
                    updated_at
                FROM {RAW_STAGE_RELATION_NAME}
                """
            )
        finally:
            connection.unregister(RAW_STAGE_RELATION_NAME)
    return inserted_count, reused_count, rematerialized_count


def _attach_raw_market_ledger(
    connection: duckdb.DuckDBPyConnection,
    *,
    raw_market_path: Path,
) -> None:
    normalized_path = str(raw_market_path).replace("\\", "/")
    connection.execute(f"ATTACH '{normalized_path}' AS raw_source")


def _stage_market_base_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    adjust_method: str,
    instruments: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
    limit: int | None,
) -> None:
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
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        parameters.append(limit)
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {MARKET_BASE_STAGE_TABLE} AS
        SELECT
            code || '|' || CAST(trade_date AS VARCHAR) || '|' || adjust_method AS daily_bar_nk,
            code,
            COALESCE(name, code) AS name,
            trade_date,
            adjust_method,
            open,
            high,
            low,
            close,
            volume,
            amount,
            bar_nk AS source_bar_nk
        FROM raw_source.{RAW_STOCK_DAILY_BAR_TABLE}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY code, trade_date
        {limit_sql}
        """,
        parameters,
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {MARKET_BASE_EXISTING_STAGE_TABLE} AS
        SELECT
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
            created_at,
            updated_at
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY code, trade_date, adjust_method
                    ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                ) AS row_number_in_key
            FROM {MARKET_BASE_STOCK_DAILY_TABLE}
        )
        WHERE row_number_in_key = 1
        """
    )


def _count_market_base_actions(connection: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    reused_condition = _build_market_base_reused_condition(stage_alias="stage", existing_alias="existing")
    inserted_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {MARKET_BASE_STAGE_TABLE} AS stage
            LEFT JOIN {MARKET_BASE_EXISTING_STAGE_TABLE} AS existing
              ON existing.code = stage.code
             AND existing.trade_date = stage.trade_date
             AND existing.adjust_method = stage.adjust_method
            WHERE existing.code IS NULL
            """
        ).fetchone()[0]
    )
    reused_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {MARKET_BASE_STAGE_TABLE} AS stage
            JOIN {MARKET_BASE_EXISTING_STAGE_TABLE} AS existing
              ON existing.code = stage.code
             AND existing.trade_date = stage.trade_date
             AND existing.adjust_method = stage.adjust_method
            WHERE {reused_condition}
            """
        ).fetchone()[0]
    )
    rematerialized_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {MARKET_BASE_STAGE_TABLE} AS stage
            JOIN {MARKET_BASE_EXISTING_STAGE_TABLE} AS existing
              ON existing.code = stage.code
             AND existing.trade_date = stage.trade_date
             AND existing.adjust_method = stage.adjust_method
            WHERE NOT ({reused_condition})
            """
        ).fetchone()[0]
    )
    return inserted_count, reused_count, rematerialized_count


def _materialize_market_base_stage(
    connection: duckdb.DuckDBPyConnection,
    *,
    adjust_method: str,
    run_id: str,
    full_scope: bool,
) -> None:
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {MARKET_BASE_FINAL_STAGE_TABLE} AS
        SELECT
            stage.daily_bar_nk,
            stage.code,
            stage.name,
            stage.trade_date,
            stage.adjust_method,
            stage.open,
            stage.high,
            stage.low,
            stage.close,
            stage.volume,
            stage.amount,
            stage.source_bar_nk,
            COALESCE(existing.first_seen_run_id, ?) AS first_seen_run_id,
            ? AS last_materialized_run_id,
            COALESCE(existing.created_at, CURRENT_TIMESTAMP) AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM {MARKET_BASE_STAGE_TABLE} AS stage
        LEFT JOIN {MARKET_BASE_EXISTING_STAGE_TABLE} AS existing
          ON existing.code = stage.code
         AND existing.trade_date = stage.trade_date
         AND existing.adjust_method = stage.adjust_method
        """,
        [run_id, run_id],
    )
    if full_scope:
        connection.execute(
            f"DELETE FROM {MARKET_BASE_STOCK_DAILY_TABLE} WHERE adjust_method = ?",
            [adjust_method],
        )
    else:
        connection.execute(
            f"""
            DELETE FROM {MARKET_BASE_STOCK_DAILY_TABLE}
            USING {MARKET_BASE_STAGE_TABLE} AS stage
            WHERE {MARKET_BASE_STOCK_DAILY_TABLE}.code = stage.code
              AND {MARKET_BASE_STOCK_DAILY_TABLE}.trade_date = stage.trade_date
              AND {MARKET_BASE_STOCK_DAILY_TABLE}.adjust_method = stage.adjust_method
            """
        )
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
            last_materialized_run_id,
            created_at,
            updated_at
        )
        SELECT
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
            last_materialized_run_id,
            created_at,
            updated_at
        FROM {MARKET_BASE_FINAL_STAGE_TABLE}
        """
    )


def _is_full_market_base_scope(
    *,
    instruments: tuple[str, ...],
    start_date: date | None,
    end_date: date | None,
    limit: int | None,
) -> bool:
    return not instruments and start_date is None and end_date is None and limit is None


def _build_market_base_reused_condition(*, stage_alias: str, existing_alias: str) -> str:
    comparisons = [
        f"COALESCE({stage_alias}.name, '') = COALESCE({existing_alias}.name, '')",
        f"COALESCE({stage_alias}.open, -1e308) = COALESCE({existing_alias}.open, -1e308)",
        f"COALESCE({stage_alias}.high, -1e308) = COALESCE({existing_alias}.high, -1e308)",
        f"COALESCE({stage_alias}.low, -1e308) = COALESCE({existing_alias}.low, -1e308)",
        f"COALESCE({stage_alias}.close, -1e308) = COALESCE({existing_alias}.close, -1e308)",
        f"COALESCE({stage_alias}.volume, -1e308) = COALESCE({existing_alias}.volume, -1e308)",
        f"COALESCE({stage_alias}.amount, -1e308) = COALESCE({existing_alias}.amount, -1e308)",
        f"COALESCE({stage_alias}.source_bar_nk, '') = COALESCE({existing_alias}.source_bar_nk, '')",
    ]
    return " AND ".join(comparisons)


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
