"""`day raw -> week/month raw` 派生 helper。"""

from __future__ import annotations

from types import SimpleNamespace

from mlq.data.data_common import *
from mlq.data.data_raw_support import *
from mlq.data.data_shared import *


def _resample_parsed_rows(parsed, *, timeframe: str):
    """把日线记录聚合成周线或月线。"""

    normalized_timeframe = _normalize_timeframe(timeframe)
    if normalized_timeframe == "day":
        return parsed
    frame = pd.DataFrame.from_records(
        [
            {
                "code": row.code,
                "name": row.name,
                "trade_date": row.trade_date,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
                "amount": row.amount,
            }
            for row in parsed.rows
        ]
    )
    if frame.empty:
        return SimpleNamespace(code=parsed.code, name=parsed.name, header=parsed.header, rows=[])
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    indexed = frame.sort_values("trade_date").set_index("trade_date")
    grouped = indexed.groupby(indexed.index.to_period("W-FRI" if normalized_timeframe == "week" else "M"))
    rows = []
    for _, group in grouped:
        clean_group = group.dropna(subset=["open", "high", "low", "close"])
        if clean_group.empty:
            continue
        rows.append(
            SimpleNamespace(
                code=str(clean_group["code"].iloc[-1]),
                name=str(clean_group["name"].iloc[-1]),
                trade_date=clean_group.index[-1].date(),
                open=float(clean_group["open"].iloc[0]),
                high=float(clean_group["high"].max()),
                low=float(clean_group["low"].min()),
                close=float(clean_group["close"].iloc[-1]),
                volume=float(clean_group["volume"].fillna(0.0).sum()),
                amount=float(clean_group["amount"].fillna(0.0).sum()),
            )
        )
    return SimpleNamespace(code=parsed.code, name=parsed.name, header=parsed.header, rows=rows)


def _build_day_raw_derived_source_path(
    workspace: WorkspaceRoots,
    *,
    asset_type: str,
    timeframe: str,
    adjust_method: str,
    code: str,
) -> Path:
    """为周/月派生账本构造稳定的合成 source_path。"""

    day_raw_path = raw_market_ledger_path(workspace)
    safe_code = code.replace(".", "_")
    return (
        day_raw_path.parent
        / "_derived_from_day_raw"
        / asset_type
        / timeframe
        / adjust_method
        / f"{safe_code}.ledger"
    )


def _fetch_day_raw_candidate_codes(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    adjust_method: str,
    instruments: set[str],
) -> tuple[str, ...]:
    """从 day raw 官方库盘点 week/month 派生候选 code。"""

    day_raw_table = _resolve_raw_bar_table(asset_type=asset_type, timeframe="day")
    rows = connection.execute(
        f"""
        SELECT DISTINCT code
        FROM {day_raw_table}
        WHERE adjust_method = ?
        ORDER BY code
        """,
        [adjust_method],
    ).fetchall()
    candidate_codes = tuple(str(row[0]) for row in rows)
    if not instruments:
        return candidate_codes
    return tuple(
        code for code in candidate_codes if code in instruments or code.split(".", 1)[0] in instruments
    )


def _filter_derived_candidate_codes_for_resume_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    timeframe: str,
    adjust_method: str,
    source_root: Path,
    candidate_codes: tuple[str, ...],
    source_path_by_code: dict[str, Path],
    continue_from_last_run: bool,
) -> tuple[str, ...]:
    """在合成 source_path 语义下，为派生 runner 过滤 resume 缺口。"""

    if not continue_from_last_run:
        return candidate_codes
    normalized_asset_type = _normalize_asset_type(asset_type)
    normalized_timeframe = _normalize_timeframe(timeframe)
    last_failed_run = connection.execute(
        f"""
        SELECT run_id
        FROM {RAW_INGEST_RUN_TABLE}
        WHERE COALESCE(asset_type, ?) = ?
          AND COALESCE(timeframe, 'day') = ?
          AND adjust_method = ?
          AND source_root = ?
          AND run_status = 'failed'
        ORDER BY started_at DESC NULLS LAST, run_id DESC
        LIMIT 1
        """,
        [DEFAULT_ASSET_TYPE, normalized_asset_type, normalized_timeframe, adjust_method, str(source_root)],
    ).fetchone()
    if last_failed_run is None:
        return candidate_codes
    completed_source_paths = {
        str(row[0])
        for row in connection.execute(
            f"""
            SELECT source_path
            FROM {RAW_INGEST_FILE_TABLE}
            WHERE run_id = ?
              AND COALESCE(asset_type, ?) = ?
              AND COALESCE(timeframe, 'day') = ?
              AND action <> 'failed'
            """,
            [str(last_failed_run[0]), DEFAULT_ASSET_TYPE, normalized_asset_type, normalized_timeframe],
        ).fetchall()
    }
    if not completed_source_paths:
        return candidate_codes
    return tuple(code for code in candidate_codes if str(source_path_by_code[code]) not in completed_source_paths)


def _load_day_raw_rows_as_parsed(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    code: str,
    adjust_method: str,
) -> tuple[SimpleNamespace, int, datetime, str]:
    """读取单个标的 day raw 行并转成派生聚合所需的 parsed 结构。"""

    day_raw_table = _resolve_raw_bar_table(asset_type=asset_type, timeframe="day")
    rows = connection.execute(
        f"""
        SELECT code, name, trade_date, open, high, low, close, volume, amount, updated_at
        FROM {day_raw_table}
        WHERE code = ?
          AND adjust_method = ?
        ORDER BY trade_date
        """,
        [code, adjust_method],
    ).fetchall()
    if not rows:
        raise ValueError(f"Missing day raw rows for {asset_type} {code} {adjust_method}")
    parsed_rows = tuple(
        SimpleNamespace(
            code=str(row[0]),
            name=str(row[1]),
            trade_date=row[2],
            open=None if row[3] is None else float(row[3]),
            high=None if row[4] is None else float(row[4]),
            low=None if row[5] is None else float(row[5]),
            close=None if row[6] is None else float(row[6]),
            volume=None if row[7] is None else float(row[7]),
            amount=None if row[8] is None else float(row[8]),
        )
        for row in rows
    )
    source_mtime_utc = max(_normalize_timestamp(row[9]) for row in rows if row[9] is not None)
    source_digest = hashlib.sha256()
    for row in rows:
        source_digest.update(
            "|".join(
                [
                    str(row[0]),
                    str(row[2]),
                    "" if row[3] is None else str(row[3]),
                    "" if row[4] is None else str(row[4]),
                    "" if row[5] is None else str(row[5]),
                    "" if row[6] is None else str(row[6]),
                    "" if row[7] is None else str(row[7]),
                    "" if row[8] is None else str(row[8]),
                    "" if row[9] is None else str(_normalize_timestamp(row[9])),
                ]
            ).encode("utf-8")
        )
        source_digest.update(b"\n")
    parsed = SimpleNamespace(
        code=str(rows[-1][0]),
        name=str(rows[-1][1]),
        header=f"derived_from_day_raw|asset={asset_type}|code={code}|adjust={adjust_method}",
        rows=parsed_rows,
    )
    return parsed, len(rows), source_mtime_utc, source_digest.hexdigest()


def _should_skip_derived_raw_source(
    *,
    source_line_count: int,
    source_mtime_utc: datetime,
    source_content_hash: str,
    registry_row: tuple[object, ...],
) -> bool:
    """比较 day raw 指纹，判断派生 source 是否可直接跳过。"""

    stored_hash = None if registry_row[5] is None else str(registry_row[5])
    if stored_hash != source_content_hash:
        return False
    return int(registry_row[0]) == source_line_count and _normalize_timestamp(registry_row[1]) == source_mtime_utc


def _run_day_raw_derived_ingest_for_asset(
    *,
    asset_type: str,
    timeframe: str,
    settings: WorkspaceRoots,
    adjust_method: str,
    run_mode: str,
    continue_from_last_run: bool,
    instruments: list[str] | tuple[str, ...] | None,
    limit: int,
    run_id: str | None,
    summary_path: Path | None,
) -> TdxStockRawIngestSummary:
    """执行 `day raw -> week/month raw` 正式派生写库。"""

    normalized_asset_type = _normalize_asset_type(asset_type)
    normalized_timeframe = _normalize_timeframe(timeframe)
    workspace = settings
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_raw_market_timeframe_ledger(workspace, timeframe=normalized_timeframe)
    bootstrap_market_base_timeframe_ledger(workspace, timeframe=normalized_timeframe)
    source_root = raw_market_ledger_path(workspace)
    raw_registry_table = RAW_FILE_REGISTRY_TABLE_BY_ASSET_TYPE[normalized_asset_type]
    raw_bar_table = _resolve_raw_bar_table(asset_type=normalized_asset_type, timeframe=normalized_timeframe)
    normalized_instruments = _normalize_instruments(instruments)
    normalized_limit = _normalize_limit(limit)
    normalized_run_mode = _normalize_raw_run_mode(run_mode)
    materialization_run_id = run_id or _build_run_id(prefix=f"raw-{normalized_asset_type}-{normalized_timeframe}-ingest")

    day_connection = duckdb.connect(str(raw_market_ledger_path(workspace)), read_only=True)
    connection = duckdb.connect(str(raw_market_timeframe_ledger_path(workspace, timeframe=normalized_timeframe)))
    base_connection: duckdb.DuckDBPyConnection | None = None
    try:
        candidate_codes = _fetch_day_raw_candidate_codes(
            day_connection,
            asset_type=normalized_asset_type,
            adjust_method=adjust_method,
            instruments=normalized_instruments,
        )
        if normalized_limit is not None:
            candidate_codes = candidate_codes[:normalized_limit]
        source_path_by_code = {
            code: _build_day_raw_derived_source_path(
                workspace,
                asset_type=normalized_asset_type,
                timeframe=normalized_timeframe,
                adjust_method=adjust_method,
                code=code,
            )
            for code in candidate_codes
        }
        candidate_codes = _filter_derived_candidate_codes_for_resume_by_asset(
            connection,
            asset_type=normalized_asset_type,
            timeframe=normalized_timeframe,
            adjust_method=adjust_method,
            source_root=source_root,
            candidate_codes=candidate_codes,
            source_path_by_code=source_path_by_code,
            continue_from_last_run=continue_from_last_run,
        )
        _insert_raw_ingest_run_start_by_asset(
            connection,
            run_id=materialization_run_id,
            asset_type=normalized_asset_type,
            timeframe=normalized_timeframe,
            adjust_method=adjust_method,
            run_mode=normalized_run_mode,
            source_root=source_root,
            candidate_file_count=len(candidate_codes),
        )
        ingested_file_count = 0
        skipped_unchanged_file_count = 0
        failed_file_count = 0
        bar_inserted_count = 0
        bar_reused_count = 0
        bar_rematerialized_count = 0
        for code in candidate_codes:
            source_path = source_path_by_code[code]
            fallback_file_nk = _build_file_nk(
                asset_type=normalized_asset_type,
                timeframe=normalized_timeframe,
                adjust_method=adjust_method,
                code=code,
                name=code,
                source_path=source_path,
            )
            try:
                parsed_day, source_line_count, source_mtime_utc, source_content_hash = _load_day_raw_rows_as_parsed(
                    day_connection,
                    asset_type=normalized_asset_type,
                    code=code,
                    adjust_method=adjust_method,
                )
                materialized = _resample_parsed_rows(parsed_day, timeframe=normalized_timeframe)
                row_count = len(materialized.rows)
                registry_row = connection.execute(
                    f"""
                    SELECT source_size_bytes, source_mtime_utc, source_line_count, name, file_nk, source_content_hash
                    FROM {raw_registry_table}
                    WHERE COALESCE(timeframe, 'day') = ?
                      AND adjust_method = ?
                      AND code = ?
                      AND source_path = ?
                    """,
                    [normalized_timeframe, adjust_method, code, str(source_path)],
                ).fetchone()
                if registry_row is not None and _should_skip_derived_raw_source(
                    source_line_count=source_line_count,
                    source_mtime_utc=source_mtime_utc,
                    source_content_hash=source_content_hash,
                    registry_row=registry_row,
                ):
                    skipped_unchanged_file_count += 1
                    _refresh_file_registry_fingerprint_by_asset(
                        connection,
                        table_name=raw_registry_table,
                        file_nk=str(registry_row[4]),
                        source_size_bytes=source_line_count,
                        source_mtime_utc=source_mtime_utc,
                        source_content_hash=source_content_hash,
                    )
                    _record_raw_ingest_file_by_asset(
                        connection,
                        run_id=materialization_run_id,
                        asset_type=normalized_asset_type,
                        timeframe=normalized_timeframe,
                        file_nk=str(registry_row[4]),
                        code=materialized.code,
                        name=materialized.name,
                        adjust_method=adjust_method,
                        source_path=source_path,
                        fingerprint_mode="day_raw_digest",
                        action="skipped_unchanged",
                        row_count=row_count,
                        error_message=None,
                    )
                    continue
                file_nk = _build_file_nk(
                    asset_type=normalized_asset_type,
                    timeframe=normalized_timeframe,
                    adjust_method=adjust_method,
                    code=materialized.code,
                    name=materialized.name,
                    source_path=source_path,
                )
                connection.execute("BEGIN TRANSACTION")
                inserted_count, reused_count, rematerialized_count = _replace_raw_bars_for_file_by_asset(
                    connection,
                    table_name=raw_bar_table,
                    asset_type=normalized_asset_type,
                    file_nk=file_nk,
                    timeframe=normalized_timeframe,
                    adjust_method=adjust_method,
                    parsed=materialized,
                    source_path=source_path,
                    source_mtime_utc=source_mtime_utc,
                    run_id=materialization_run_id,
                )
                _upsert_file_registry_by_asset(
                    connection,
                    table_name=raw_registry_table,
                    asset_type=normalized_asset_type,
                    file_nk=file_nk,
                    timeframe=normalized_timeframe,
                    adjust_method=adjust_method,
                    parsed_name=materialized.name,
                    parsed_code=materialized.code,
                    source_path=source_path,
                    source_size_bytes=source_line_count,
                    source_mtime_utc=source_mtime_utc,
                    source_line_count=source_line_count,
                    source_header=parsed_day.header,
                    source_content_hash=source_content_hash,
                    run_id=materialization_run_id,
                )
                raw_file_action = _resolve_raw_file_action(
                    inserted_count=inserted_count,
                    reused_count=reused_count,
                    rematerialized_count=rematerialized_count,
                )
                if inserted_count > 0 or rematerialized_count > 0:
                    if base_connection is None:
                        base_connection = duckdb.connect(
                            str(market_base_timeframe_ledger_path(workspace, timeframe=normalized_timeframe))
                        )
                    _upsert_dirty_instrument_by_asset(
                        base_connection,
                        table_name=BASE_DIRTY_INSTRUMENT_TABLE,
                        asset_type=normalized_asset_type,
                        timeframe=normalized_timeframe,
                        code=materialized.code,
                        adjust_method=adjust_method,
                        dirty_reason=f"raw_{raw_file_action}",
                        source_run_id=materialization_run_id,
                        source_file_nk=file_nk,
                    )
                _record_raw_ingest_file_by_asset(
                    connection,
                    run_id=materialization_run_id,
                    asset_type=normalized_asset_type,
                    timeframe=normalized_timeframe,
                    file_nk=file_nk,
                    code=materialized.code,
                    name=materialized.name,
                    adjust_method=adjust_method,
                    source_path=source_path,
                    fingerprint_mode="day_raw_digest",
                    action=raw_file_action,
                    row_count=row_count,
                    error_message=None,
                )
                connection.execute("COMMIT")
                ingested_file_count += 1
                bar_inserted_count += inserted_count
                bar_reused_count += reused_count
                bar_rematerialized_count += rematerialized_count
            except Exception as exc:
                failed_file_count += 1
                try:
                    connection.execute("ROLLBACK")
                except Exception:
                    pass
                _record_raw_ingest_file_by_asset(
                    connection,
                    run_id=materialization_run_id,
                    asset_type=normalized_asset_type,
                    timeframe=normalized_timeframe,
                    file_nk=fallback_file_nk,
                    code=code,
                    name=code,
                    adjust_method=adjust_method,
                    source_path=source_path,
                    fingerprint_mode="day_raw_digest",
                    action="failed",
                    row_count=0,
                    error_message=str(exc),
                )
                processed_file_count = ingested_file_count + skipped_unchanged_file_count + failed_file_count
                _update_raw_ingest_run_failure_by_asset(
                    connection,
                    run_id=materialization_run_id,
                    asset_type=normalized_asset_type,
                    timeframe=normalized_timeframe,
                    adjust_method=adjust_method,
                    run_mode=normalized_run_mode,
                    source_root=source_root,
                    candidate_file_count=len(candidate_codes),
                    processed_file_count=processed_file_count,
                    skipped_file_count=skipped_unchanged_file_count,
                    inserted_bar_count=bar_inserted_count,
                    reused_bar_count=bar_reused_count,
                    rematerialized_bar_count=bar_rematerialized_count,
                    failed_file_count=failed_file_count,
                    error_message=str(exc),
                )
                raise
        processed_file_count = ingested_file_count + skipped_unchanged_file_count + failed_file_count
        summary = TdxStockRawIngestSummary(
            run_id=materialization_run_id,
            asset_type=normalized_asset_type,
            timeframe=normalized_timeframe,
            adjust_method=adjust_method,
            run_mode=normalized_run_mode,
            candidate_file_count=len(candidate_codes),
            processed_file_count=processed_file_count,
            ingested_file_count=ingested_file_count,
            skipped_unchanged_file_count=skipped_unchanged_file_count,
            failed_file_count=failed_file_count,
            bar_inserted_count=bar_inserted_count,
            bar_reused_count=bar_reused_count,
            bar_rematerialized_count=bar_rematerialized_count,
            raw_market_path=str(raw_market_timeframe_ledger_path(workspace, timeframe=normalized_timeframe)),
            source_root=str(source_root),
        )
        _update_raw_ingest_run_success(connection, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    finally:
        if base_connection is not None:
            base_connection.close()
        connection.close()
        day_connection.close()
