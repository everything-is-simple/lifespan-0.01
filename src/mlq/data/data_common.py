"""`data` 模块的通用规范化与键构造辅助函数。"""

from __future__ import annotations

from mlq.data.data_shared import *

def _normalize_limit(limit: int | None) -> int | None:
    if limit is None:
        return None
    normalized = int(limit)
    if normalized <= 0:
        return None
    return normalized


def _normalize_asset_type(asset_type: str) -> str:
    normalized = str(asset_type).strip().lower()
    if normalized not in TDX_ASSET_TYPES:
        raise ValueError(f"Unsupported asset type: {asset_type}")
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


def _build_dirty_nk(*, code: str, adjust_method: str) -> str:
    return "|".join([code, adjust_method])


def _build_dirty_nk_by_asset(*, asset_type: str, code: str, adjust_method: str) -> str:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        return _build_dirty_nk(code=code, adjust_method=adjust_method)
    return "|".join([normalized_asset_type, code, adjust_method])


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
    source_content_hash: str | None,
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
                source_content_hash,
                last_ingested_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                source_content_hash,
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
            source_content_hash = ?,
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
            source_content_hash,
            run_id,
            str(existing[0]),
        ],
    )


def _upsert_file_registry_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    asset_type: str,
    file_nk: str,
    adjust_method: str,
    parsed_name: str,
    parsed_code: str,
    source_path: Path,
    source_size_bytes: int,
    source_mtime_utc: datetime,
    source_line_count: int,
    source_header: str,
    source_content_hash: str | None,
    run_id: str,
) -> None:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE and table_name == RAW_STOCK_FILE_REGISTRY_TABLE:
        _upsert_file_registry(
            connection,
            file_nk=file_nk,
            adjust_method=adjust_method,
            parsed_name=parsed_name,
            parsed_code=parsed_code,
            source_path=source_path,
            source_size_bytes=source_size_bytes,
            source_mtime_utc=source_mtime_utc,
            source_line_count=source_line_count,
            source_header=source_header,
            source_content_hash=source_content_hash,
            run_id=run_id,
        )
        return
    existing = connection.execute(
        f"""
        SELECT file_nk
        FROM {table_name}
        WHERE adjust_method = ?
          AND code = ?
          AND source_path = ?
        """,
        [adjust_method, parsed_code, str(source_path)],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {table_name} (
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
                source_content_hash,
                last_ingested_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                file_nk,
                normalized_asset_type,
                adjust_method,
                parsed_code,
                parsed_name,
                str(source_path),
                source_size_bytes,
                source_mtime_utc,
                source_line_count,
                source_header,
                source_content_hash,
                run_id,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {table_name}
        SET
            asset_type = ?,
            code = ?,
            file_nk = ?,
            name = ?,
            source_path = ?,
            source_size_bytes = ?,
            source_mtime_utc = ?,
            source_line_count = ?,
            source_header = ?,
            source_content_hash = ?,
            last_ingested_run_id = ?,
            last_ingested_at = CURRENT_TIMESTAMP
        WHERE file_nk = ?
        """,
        [
            normalized_asset_type,
            parsed_code,
            file_nk,
            parsed_name,
            str(source_path),
            source_size_bytes,
            source_mtime_utc,
            source_line_count,
            source_header,
            source_content_hash,
            run_id,
            str(existing[0]),
        ],
    )


def _refresh_file_registry_fingerprint(
    connection: duckdb.DuckDBPyConnection,
    *,
    file_nk: str,
    source_size_bytes: int,
    source_mtime_utc: datetime,
    source_content_hash: str | None,
) -> None:
    connection.execute(
        f"""
        UPDATE {RAW_STOCK_FILE_REGISTRY_TABLE}
        SET
            source_size_bytes = ?,
            source_mtime_utc = ?,
            source_content_hash = ?
        WHERE file_nk = ?
        """,
        [
            source_size_bytes,
            source_mtime_utc,
            source_content_hash,
            file_nk,
        ],
    )


def _refresh_file_registry_fingerprint_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    file_nk: str,
    source_size_bytes: int,
    source_mtime_utc: datetime,
    source_content_hash: str | None,
) -> None:
    if table_name == RAW_STOCK_FILE_REGISTRY_TABLE:
        _refresh_file_registry_fingerprint(
            connection,
            file_nk=file_nk,
            source_size_bytes=source_size_bytes,
            source_mtime_utc=source_mtime_utc,
            source_content_hash=source_content_hash,
        )
        return
    connection.execute(
        f"""
        UPDATE {table_name}
        SET
            source_size_bytes = ?,
            source_mtime_utc = ?,
            source_content_hash = ?
        WHERE file_nk = ?
        """,
        [
            source_size_bytes,
            source_mtime_utc,
            source_content_hash,
            file_nk,
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
    if records:
        frame = pd.DataFrame.from_records(records)
        connection.register(RAW_STAGE_RELATION_NAME, frame)
        try:
            connection.execute(
                f"""
                DELETE FROM {RAW_STOCK_DAILY_BAR_TABLE}
                WHERE code = ?
                  AND adjust_method = ?
                  AND bar_nk NOT IN (
                      SELECT bar_nk
                      FROM {RAW_STAGE_RELATION_NAME}
                  )
                """,
                [parsed.code, adjust_method],
            )
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {RAW_STOCK_DAILY_BAR_TABLE} (
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
    else:
        connection.execute(
            f"""
            DELETE FROM {RAW_STOCK_DAILY_BAR_TABLE}
            WHERE code = ?
              AND adjust_method = ?
            """,
            [parsed.code, adjust_method],
        )
    return inserted_count, reused_count, rematerialized_count


def _replace_raw_bars_for_file_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    asset_type: str,
    file_nk: str,
    adjust_method: str,
    parsed,
    source_path: Path,
    source_mtime_utc: datetime,
    run_id: str,
) -> tuple[int, int, int]:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE and table_name == RAW_STOCK_DAILY_BAR_TABLE:
        return _replace_raw_bars_for_file(
            connection,
            file_nk=file_nk,
            adjust_method=adjust_method,
            parsed=parsed,
            source_path=source_path,
            source_mtime_utc=source_mtime_utc,
            run_id=run_id,
        )
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
        FROM {table_name}
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
                "asset_type": normalized_asset_type,
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
    if records:
        frame = pd.DataFrame.from_records(records)
        connection.register(RAW_STAGE_RELATION_NAME, frame)
        try:
            connection.execute(
                f"""
                DELETE FROM {table_name}
                WHERE code = ?
                  AND adjust_method = ?
                  AND bar_nk NOT IN (
                      SELECT bar_nk
                      FROM {RAW_STAGE_RELATION_NAME}
                  )
                """,
                [parsed.code, adjust_method],
            )
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {table_name} (
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
    else:
        connection.execute(
            f"""
            DELETE FROM {table_name}
            WHERE code = ?
              AND adjust_method = ?
            """,
            [parsed.code, adjust_method],
        )
    return inserted_count, reused_count, rematerialized_count


def _attach_raw_market_ledger(
    connection: duckdb.DuckDBPyConnection,
    *,
    raw_market_path: Path,
) -> None:
    normalized_path = str(raw_market_path).replace("\\", "/")
    connection.execute(f"ATTACH '{normalized_path}' AS raw_source")


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

__all__ = [name for name in globals() if not name.startswith("__")]

