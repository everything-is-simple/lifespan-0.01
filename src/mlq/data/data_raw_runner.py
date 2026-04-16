"""raw ingest 正式 runner 入口实现。"""

from __future__ import annotations

from mlq.data.data_common import *
from mlq.data.data_raw_support import *
from mlq.data.data_shared import *


def _resolve_tdx_daily_folder(source_root: Path, *, asset_type: str, adjust_method: str) -> Path:
    """兼容当前离线源的 `{asset_type}-day` 目录与早期 `{asset_type}` 目录。"""

    folder_name = resolve_adjust_method_folder(adjust_method)
    candidates = (
        source_root / asset_type / folder_name,
        source_root / f"{asset_type}-day" / folder_name,
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Missing TDX source directory, tried: " + ", ".join(str(candidate) for candidate in candidates)
    )


def run_tdx_stock_raw_ingest(
    *,
    settings: WorkspaceRoots | None = None,
    source_root: Path | str | None = None,
    adjust_method: str = "backward",
    run_mode: str = "incremental",
    force_hash: bool = False,
    continue_from_last_run: bool = False,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> TdxStockRawIngestSummary:
    """? TDX ???????????? `raw_market`?"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    resolved_source_root = Path(source_root or DEFAULT_TDX_SOURCE_ROOT)
    folder_path = _resolve_tdx_daily_folder(
        resolved_source_root,
        asset_type=DEFAULT_ASSET_TYPE,
        adjust_method=adjust_method,
    )

    normalized_instruments = _normalize_instruments(instruments)
    normalized_limit = _normalize_limit(limit)
    normalized_run_mode = _normalize_raw_run_mode(run_mode)
    materialization_run_id = run_id or _build_run_id(prefix="raw-stock-ingest")
    matching_files = [
        path
        for path in sorted(folder_path.glob("*.txt"))
        if _match_instrument_filter(path, normalized_instruments)
    ]
    limited_candidate_files = matching_files if normalized_limit is None else matching_files[:normalized_limit]

    connection = duckdb.connect(str(raw_market_ledger_path(workspace)))
    base_connection: duckdb.DuckDBPyConnection | None = None
    try:
        candidate_files = _resolve_raw_candidate_files(
            connection,
            adjust_method=adjust_method,
            source_root=resolved_source_root,
            candidate_files=limited_candidate_files,
            continue_from_last_run=continue_from_last_run,
        )
        _insert_raw_ingest_run_start(
            connection,
            run_id=materialization_run_id,
            adjust_method=adjust_method,
            run_mode=normalized_run_mode,
            source_root=resolved_source_root,
            candidate_file_count=len(candidate_files),
        )
        ingested_file_count = 0
        skipped_unchanged_file_count = 0
        failed_file_count = 0
        bar_inserted_count = 0
        bar_reused_count = 0
        bar_rematerialized_count = 0
        for path in candidate_files:
            code = _resolve_code_from_filename(path)
            fallback_file_nk = _build_file_nk(
                asset_type=DEFAULT_ASSET_TYPE,
                adjust_method=adjust_method,
                code=code,
                name=code,
                source_path=path,
            )
            try:
                used_content_hash_fingerprint = bool(force_hash)
                stat_result = path.stat()
                source_size_bytes = stat_result.st_size
                source_mtime_utc = datetime.fromtimestamp(stat_result.st_mtime).replace(microsecond=0)
                registry_row = connection.execute(
                    f"""
                    SELECT source_size_bytes, source_mtime_utc, source_line_count, name, file_nk, source_content_hash
                    FROM {RAW_STOCK_FILE_REGISTRY_TABLE}
                    WHERE asset_type = ?
                      AND adjust_method = ?
                      AND code = ?
                      AND source_path = ?
                    """,
                    [DEFAULT_ASSET_TYPE, adjust_method, code, str(path)],
                ).fetchone()
                used_content_hash_fingerprint = force_hash or (
                    registry_row is not None
                    and int(registry_row[0]) == source_size_bytes
                    and _normalize_timestamp(registry_row[1]) != source_mtime_utc
                )
                fingerprint_mode = "size_mtime"
                current_content_hash: str | None = None
                if registry_row is not None and _should_skip_raw_file(
                    path=path,
                    source_size_bytes=source_size_bytes,
                    source_mtime_utc=source_mtime_utc,
                    registry_row=registry_row,
                    force_hash=force_hash,
                ):
                    if used_content_hash_fingerprint:
                        fingerprint_mode = "content_hash"
                        current_content_hash = _compute_file_content_hash(path)
                        stored_hash = None if registry_row[5] is None else str(registry_row[5])
                        if stored_hash == current_content_hash:
                            _refresh_file_registry_fingerprint(
                                connection,
                                file_nk=str(registry_row[4]),
                                source_size_bytes=source_size_bytes,
                                source_mtime_utc=source_mtime_utc,
                                source_content_hash=current_content_hash,
                            )
                    else:
                        fingerprint_mode = "size_mtime"
                    skipped_unchanged_file_count += 1
                    _record_raw_ingest_file(
                        connection,
                        run_id=materialization_run_id,
                        file_nk=str(registry_row[4]),
                        code=code,
                        name=str(registry_row[3]),
                        adjust_method=adjust_method,
                        source_path=path,
                        fingerprint_mode=fingerprint_mode,
                        action="skipped_unchanged",
                        row_count=int(registry_row[2]),
                        error_message=None,
                    )
                    continue

                parsed = parse_tdx_stock_file(path)
                current_content_hash = current_content_hash or _compute_file_content_hash(path)
                file_nk = _build_file_nk(
                    asset_type=DEFAULT_ASSET_TYPE,
                    adjust_method=adjust_method,
                    code=parsed.code,
                    name=parsed.name,
                    source_path=path,
                )
                connection.execute("BEGIN TRANSACTION")
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
                    source_content_hash=current_content_hash,
                    run_id=materialization_run_id,
                )
                raw_file_action = _resolve_raw_file_action(
                    inserted_count=inserted_count,
                    reused_count=reused_count,
                    rematerialized_count=rematerialized_count,
                )
                if inserted_count > 0 or rematerialized_count > 0:
                    if base_connection is None:
                        base_connection = duckdb.connect(str(market_base_ledger_path(workspace)))
                    _upsert_dirty_instrument_on_connection(
                        base_connection,
                        table_name=BASE_DIRTY_INSTRUMENT_TABLE,
                        code=parsed.code,
                        adjust_method=adjust_method,
                        dirty_reason=f"raw_{raw_file_action}",
                        source_run_id=materialization_run_id,
                        source_file_nk=file_nk,
                    )
                _record_raw_ingest_file(
                    connection,
                    run_id=materialization_run_id,
                    file_nk=file_nk,
                    code=parsed.code,
                    name=parsed.name,
                    adjust_method=adjust_method,
                    source_path=path,
                    fingerprint_mode="content_hash" if used_content_hash_fingerprint else "size_mtime",
                    action=raw_file_action,
                    row_count=len(parsed.rows),
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
                _record_raw_ingest_file(
                    connection,
                    run_id=materialization_run_id,
                    file_nk=fallback_file_nk,
                    code=code,
                    name=code,
                    adjust_method=adjust_method,
                    source_path=path,
                    fingerprint_mode="content_hash" if used_content_hash_fingerprint else "size_mtime",
                    action="failed",
                    row_count=0,
                    error_message=str(exc),
                )
                processed_file_count = ingested_file_count + skipped_unchanged_file_count + failed_file_count
                _update_raw_ingest_run_failure(
                    connection,
                    run_id=materialization_run_id,
                    adjust_method=adjust_method,
                    run_mode=normalized_run_mode,
                    source_root=resolved_source_root,
                    candidate_file_count=len(candidate_files),
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
            asset_type=DEFAULT_ASSET_TYPE,
            adjust_method=adjust_method,
            run_mode=normalized_run_mode,
            candidate_file_count=len(candidate_files),
            processed_file_count=processed_file_count,
            ingested_file_count=ingested_file_count,
            skipped_unchanged_file_count=skipped_unchanged_file_count,
            failed_file_count=failed_file_count,
            bar_inserted_count=bar_inserted_count,
            bar_reused_count=bar_reused_count,
            bar_rematerialized_count=bar_rematerialized_count,
            raw_market_path=str(raw_market_ledger_path(workspace)),
            source_root=str(resolved_source_root),
        )
        _update_raw_ingest_run_success(connection, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    finally:
        if base_connection is not None:
            base_connection.close()
        connection.close()


def run_tdx_asset_raw_ingest_batched(
    *,
    asset_type: str,
    settings: WorkspaceRoots | None = None,
    source_root: Path | str | None = None,
    adjust_method: str = "backward",
    run_mode: str = "full",
    batch_size: int = 100,
    force_hash: bool = False,
    instruments: list[str] | tuple[str, ...] | None = None,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> dict[str, object]:
    """按标的批次执行 TDX raw ingest，避免把全量文件作为单个 run 推进。"""

    normalized_asset_type = _normalize_asset_type(asset_type)
    normalized_batch_size = int(batch_size)
    if normalized_batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")
    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    resolved_source_root = Path(source_root or DEFAULT_TDX_SOURCE_ROOT)
    folder_path = _resolve_tdx_daily_folder(
        resolved_source_root,
        asset_type=normalized_asset_type,
        adjust_method=adjust_method,
    )
    normalized_instruments = _normalize_instruments(instruments)
    matching_files = [
        path
        for path in sorted(folder_path.glob("*.txt"))
        if _match_instrument_filter(path, normalized_instruments)
    ]
    candidate_codes = tuple(_resolve_code_from_filename(path) for path in matching_files)
    parent_run_id = run_id or _build_run_id(prefix=f"raw-{normalized_asset_type}-ingest-batch")
    child_summaries: list[dict[str, object]] = []
    for batch_number, offset in enumerate(range(0, len(candidate_codes), normalized_batch_size), start=1):
        batch_codes = candidate_codes[offset : offset + normalized_batch_size]
        child_summary = run_tdx_asset_raw_ingest(
            asset_type=normalized_asset_type,
            settings=workspace,
            source_root=resolved_source_root,
            adjust_method=adjust_method,
            run_mode=run_mode,
            force_hash=force_hash,
            instruments=batch_codes,
            limit=0,
            run_id=f"{parent_run_id}-b{batch_number:04d}",
        )
        child_summaries.append(child_summary.as_dict())

    summary: dict[str, object] = {
        "run_id": parent_run_id,
        "asset_type": normalized_asset_type,
        "adjust_method": adjust_method,
        "run_mode": run_mode,
        "batch_size": normalized_batch_size,
        "batch_count": len(child_summaries),
        "candidate_file_count": len(candidate_codes),
        "processed_file_count": sum(int(item["processed_file_count"]) for item in child_summaries),
        "ingested_file_count": sum(int(item["ingested_file_count"]) for item in child_summaries),
        "skipped_unchanged_file_count": sum(
            int(item["skipped_unchanged_file_count"]) for item in child_summaries
        ),
        "failed_file_count": sum(int(item["failed_file_count"]) for item in child_summaries),
        "bar_inserted_count": sum(int(item["bar_inserted_count"]) for item in child_summaries),
        "bar_reused_count": sum(int(item["bar_reused_count"]) for item in child_summaries),
        "bar_rematerialized_count": sum(int(item["bar_rematerialized_count"]) for item in child_summaries),
        "child_runs": child_summaries,
        "raw_market_path": str(raw_market_ledger_path(workspace)),
        "source_root": str(resolved_source_root),
    }
    _write_summary(summary, summary_path)
    return summary


def run_tdx_asset_raw_ingest(
    *,
    asset_type: str,
    settings: WorkspaceRoots | None = None,
    source_root: Path | str | None = None,
    adjust_method: str = "backward",
    run_mode: str = "incremental",
    force_hash: bool = False,
    continue_from_last_run: bool = False,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> TdxStockRawIngestSummary:
    """? TDX ???????????? `raw_market`?"""

    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        return run_tdx_stock_raw_ingest(
            settings=settings,
            source_root=source_root,
            adjust_method=adjust_method,
            run_mode=run_mode,
            force_hash=force_hash,
            continue_from_last_run=continue_from_last_run,
            instruments=instruments,
            limit=limit,
            run_id=run_id,
            summary_path=summary_path,
        )

    workspace = settings or default_settings()
    workspace.ensure_directories()
    bootstrap_raw_market_ledger(workspace)
    bootstrap_market_base_ledger(workspace)
    resolved_source_root = Path(source_root or DEFAULT_TDX_SOURCE_ROOT)
    folder_path = _resolve_tdx_daily_folder(
        resolved_source_root,
        asset_type=normalized_asset_type,
        adjust_method=adjust_method,
    )

    raw_registry_table = RAW_FILE_REGISTRY_TABLE_BY_ASSET_TYPE[normalized_asset_type]
    raw_bar_table = RAW_DAILY_BAR_TABLE_BY_ASSET_TYPE[normalized_asset_type]
    normalized_instruments = _normalize_instruments(instruments)
    normalized_limit = _normalize_limit(limit)
    normalized_run_mode = _normalize_raw_run_mode(run_mode)
    materialization_run_id = run_id or _build_run_id(prefix=f"raw-{normalized_asset_type}-ingest")
    matching_files = [
        path
        for path in sorted(folder_path.glob("*.txt"))
        if _match_instrument_filter(path, normalized_instruments)
    ]
    limited_candidate_files = matching_files if normalized_limit is None else matching_files[:normalized_limit]

    connection = duckdb.connect(str(raw_market_ledger_path(workspace)))
    base_connection: duckdb.DuckDBPyConnection | None = None
    try:
        candidate_files = _resolve_raw_candidate_files_by_asset(
            connection,
            asset_type=normalized_asset_type,
            adjust_method=adjust_method,
            source_root=resolved_source_root,
            candidate_files=limited_candidate_files,
            continue_from_last_run=continue_from_last_run,
        )
        _insert_raw_ingest_run_start_by_asset(
            connection,
            run_id=materialization_run_id,
            asset_type=normalized_asset_type,
            adjust_method=adjust_method,
            run_mode=normalized_run_mode,
            source_root=resolved_source_root,
            candidate_file_count=len(candidate_files),
        )
        ingested_file_count = 0
        skipped_unchanged_file_count = 0
        failed_file_count = 0
        bar_inserted_count = 0
        bar_reused_count = 0
        bar_rematerialized_count = 0
        for path in candidate_files:
            code = _resolve_code_from_filename(path)
            fallback_file_nk = _build_file_nk(
                asset_type=normalized_asset_type,
                adjust_method=adjust_method,
                code=code,
                name=code,
                source_path=path,
            )
            used_content_hash_fingerprint = bool(force_hash)
            try:
                stat_result = path.stat()
                source_size_bytes = stat_result.st_size
                source_mtime_utc = datetime.fromtimestamp(stat_result.st_mtime).replace(microsecond=0)
                registry_row = connection.execute(
                    f"""
                    SELECT source_size_bytes, source_mtime_utc, source_line_count, name, file_nk, source_content_hash
                    FROM {raw_registry_table}
                    WHERE adjust_method = ?
                      AND code = ?
                      AND source_path = ?
                    """,
                    [adjust_method, code, str(path)],
                ).fetchone()
                used_content_hash_fingerprint = force_hash or (
                    registry_row is not None
                    and int(registry_row[0]) == source_size_bytes
                    and _normalize_timestamp(registry_row[1]) != source_mtime_utc
                )
                fingerprint_mode = "size_mtime"
                current_content_hash: str | None = None
                if registry_row is not None and _should_skip_raw_file(
                    path=path,
                    source_size_bytes=source_size_bytes,
                    source_mtime_utc=source_mtime_utc,
                    registry_row=registry_row,
                    force_hash=force_hash,
                ):
                    if used_content_hash_fingerprint:
                        fingerprint_mode = "content_hash"
                        current_content_hash = _compute_file_content_hash(path)
                        stored_hash = None if registry_row[5] is None else str(registry_row[5])
                        if stored_hash == current_content_hash:
                            _refresh_file_registry_fingerprint_by_asset(
                                connection,
                                table_name=raw_registry_table,
                                file_nk=str(registry_row[4]),
                                source_size_bytes=source_size_bytes,
                                source_mtime_utc=source_mtime_utc,
                                source_content_hash=current_content_hash,
                            )
                    skipped_unchanged_file_count += 1
                    _record_raw_ingest_file_by_asset(
                        connection,
                        run_id=materialization_run_id,
                        asset_type=normalized_asset_type,
                        file_nk=str(registry_row[4]),
                        code=code,
                        name=str(registry_row[3]),
                        adjust_method=adjust_method,
                        source_path=path,
                        fingerprint_mode=fingerprint_mode,
                        action="skipped_unchanged",
                        row_count=int(registry_row[2]),
                        error_message=None,
                    )
                    continue

                parsed = parse_tdx_stock_file(path)
                current_content_hash = current_content_hash or _compute_file_content_hash(path)
                file_nk = _build_file_nk(
                    asset_type=normalized_asset_type,
                    adjust_method=adjust_method,
                    code=parsed.code,
                    name=parsed.name,
                    source_path=path,
                )
                connection.execute("BEGIN TRANSACTION")
                inserted_count, reused_count, rematerialized_count = _replace_raw_bars_for_file_by_asset(
                    connection,
                    table_name=raw_bar_table,
                    asset_type=normalized_asset_type,
                    file_nk=file_nk,
                    adjust_method=adjust_method,
                    parsed=parsed,
                    source_path=path,
                    source_mtime_utc=source_mtime_utc,
                    run_id=materialization_run_id,
                )
                _upsert_file_registry_by_asset(
                    connection,
                    table_name=raw_registry_table,
                    asset_type=normalized_asset_type,
                    file_nk=file_nk,
                    adjust_method=adjust_method,
                    parsed_name=parsed.name,
                    parsed_code=parsed.code,
                    source_path=path,
                    source_size_bytes=source_size_bytes,
                    source_mtime_utc=source_mtime_utc,
                    source_line_count=len(parsed.rows),
                    source_header=parsed.header,
                    source_content_hash=current_content_hash,
                    run_id=materialization_run_id,
                )
                raw_file_action = _resolve_raw_file_action(
                    inserted_count=inserted_count,
                    reused_count=reused_count,
                    rematerialized_count=rematerialized_count,
                )
                if inserted_count > 0 or rematerialized_count > 0:
                    if base_connection is None:
                        base_connection = duckdb.connect(str(market_base_ledger_path(workspace)))
                    dirty_reason = "raw_inserted" if rematerialized_count == 0 else "raw_rematerialized"
                    _upsert_dirty_instrument_by_asset(
                        base_connection,
                        table_name=BASE_DIRTY_INSTRUMENT_TABLE,
                        asset_type=normalized_asset_type,
                        code=parsed.code,
                        adjust_method=adjust_method,
                        dirty_reason=dirty_reason,
                        source_run_id=materialization_run_id,
                        source_file_nk=file_nk,
                    )
                _record_raw_ingest_file_by_asset(
                    connection,
                    run_id=materialization_run_id,
                    asset_type=normalized_asset_type,
                    file_nk=file_nk,
                    code=parsed.code,
                    name=parsed.name,
                    adjust_method=adjust_method,
                    source_path=path,
                    fingerprint_mode="content_hash" if used_content_hash_fingerprint else "size_mtime",
                    action=raw_file_action,
                    row_count=len(parsed.rows),
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
                    file_nk=fallback_file_nk,
                    code=code,
                    name=code,
                    adjust_method=adjust_method,
                    source_path=path,
                    fingerprint_mode="content_hash" if used_content_hash_fingerprint else "size_mtime",
                    action="failed",
                    row_count=0,
                    error_message=str(exc),
                )
                processed_file_count = ingested_file_count + skipped_unchanged_file_count + failed_file_count
                _update_raw_ingest_run_failure_by_asset(
                    connection,
                    run_id=materialization_run_id,
                    asset_type=normalized_asset_type,
                    adjust_method=adjust_method,
                    run_mode=normalized_run_mode,
                    source_root=resolved_source_root,
                    candidate_file_count=len(candidate_files),
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
            adjust_method=adjust_method,
            run_mode=normalized_run_mode,
            candidate_file_count=len(candidate_files),
            processed_file_count=processed_file_count,
            ingested_file_count=ingested_file_count,
            skipped_unchanged_file_count=skipped_unchanged_file_count,
            failed_file_count=failed_file_count,
            bar_inserted_count=bar_inserted_count,
            bar_reused_count=bar_reused_count,
            bar_rematerialized_count=bar_rematerialized_count,
            raw_market_path=str(raw_market_ledger_path(workspace)),
            source_root=str(resolved_source_root),
        )
        _update_raw_ingest_run_success(connection, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    finally:
        if base_connection is not None:
            base_connection.close()
        connection.close()


