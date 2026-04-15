"""`Tushare objective source -> raw_tdxquant_instrument_profile` 正式 runner。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from mlq.data.data_common import *
from mlq.data.data_shared import *
from mlq.data.data_tushare_objective_support import (
    build_objective_profile_checkpoint_nk as _build_objective_profile_checkpoint_nk,
    build_records_digest as _build_records_digest,
    build_tushare_event_nk as _build_tushare_event_nk,
    build_tushare_request_nk as _build_tushare_request_nk,
    fetch_tushare_checkpoint as _fetch_tushare_checkpoint,
    insert_objective_profile_materialization_run_start as _insert_objective_profile_materialization_run_start,
    insert_tushare_objective_run_start as _insert_tushare_objective_run_start,
    record_objective_profile_run_profile as _record_objective_profile_run_profile,
    record_tushare_objective_request as _record_tushare_objective_request,
    should_skip_tushare_request as _should_skip_tushare_request,
    update_objective_profile_materialization_run_failure as _update_objective_profile_materialization_run_failure,
    update_objective_profile_materialization_run_success as _update_objective_profile_materialization_run_success,
    update_tushare_objective_run_failure as _update_tushare_objective_run_failure,
    update_tushare_objective_run_success as _update_tushare_objective_run_success,
    upsert_objective_profile_checkpoint as _upsert_objective_profile_checkpoint,
    upsert_tushare_checkpoint as _upsert_tushare_checkpoint,
    upsert_tushare_events as _upsert_tushare_events,
)
from mlq.data.tushare import (
    TushareClient,
    TushareNameChangeRow,
    TushareStockBasicRow,
    TushareStockStRow,
    TushareSuspendRow,
    open_tushare_client,
)


DEFAULT_TUSHARE_SOURCE_APIS: tuple[str, ...] = ("stock_basic", "suspend_d", "stock_st", "namechange")
_SUPPORTED_TUSHARE_SOURCE_APIS = frozenset(DEFAULT_TUSHARE_SOURCE_APIS)
_STOCK_BASIC_EXCHANGE_STATUS_SCOPE: tuple[tuple[str, str], ...] = (
    ("SSE", "L"),
    ("SSE", "P"),
    ("SSE", "D"),
    ("SZSE", "L"),
    ("SZSE", "P"),
    ("SZSE", "D"),
    ("BSE", "L"),
    ("BSE", "P"),
    ("BSE", "D"),
)
_DATE_FLOOR = date(1900, 1, 1)


@dataclass(frozen=True)
class _TushareRequestPlan:
    source_api: str
    cursor_type: str
    cursor_value: str


@dataclass(frozen=True)
class _ObjectiveEventRow:
    event_nk: str
    asset_type: str
    code: str
    source_api: str
    objective_dimension: str
    effective_start_date: date
    effective_end_date: date | None
    status_value_code: str | None
    status_value_text: str | None
    source_trade_date: date | None
    source_ann_date: date | None
    payload_json: str | None
    first_seen_run_id: str
    last_seen_run_id: str


def run_tushare_objective_source_sync(
    *,
    settings: WorkspaceRoots | None = None,
    raw_db_path: Path | str | None = None,
    source_apis: list[str] | tuple[str, ...] | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instrument_limit: int | None = None,
    instrument_list: list[str] | tuple[str, ...] | None = None,
    use_checkpoint_queue: bool = False,
    run_id: str | None = None,
    summary_path: Path | None = None,
    tushare_token: str | None = None,
    client_factory: Callable[[str | None], TushareClient] | None = None,
) -> TushareObjectiveSourceSyncSummary:
    """把 Tushare 历史 objective 事实同步进正式 source ledger。"""

    normalized_start_date, normalized_end_date = _normalize_bounded_mode(
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        use_checkpoint_queue=use_checkpoint_queue,
    )
    normalized_source_apis = _normalize_tushare_source_apis(source_apis)
    normalized_instruments = _normalize_tushare_codes(instrument_list)
    normalized_limit = _normalize_limit(instrument_limit)
    workspace = settings or default_settings()
    connection, resolved_raw_market_path, owns_connection = _open_raw_market_connection(
        settings=workspace,
        raw_db_path=raw_db_path,
    )
    client: TushareClient | None = None
    source_run_id = run_id or _build_run_id(prefix="tushare-objective-source")
    run_terminal_recorded = False
    try:
        bootstrap_raw_market_ledger(workspace, connection=connection)
        scope_codes = _resolve_scope_codes(
            connection,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instrument_list=normalized_instruments,
            instrument_limit=normalized_limit,
        )
        trade_dates = _resolve_trade_dates(
            connection,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
        )
        request_plans = (
            _load_checkpoint_request_plans(
                connection,
                source_apis=normalized_source_apis,
                instrument_list=normalized_instruments,
                instrument_limit=normalized_limit,
            )
            if use_checkpoint_queue
            else _build_bounded_request_plans(
                source_apis=normalized_source_apis,
                trade_dates=trade_dates,
                scope_codes=scope_codes,
            )
        )
        _insert_tushare_objective_run_start(
            connection,
            run_id=source_run_id,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            source_api_scope=normalized_source_apis,
            candidate_cursor_count=len(request_plans),
        )
        client = (client_factory or open_tushare_client)(tushare_token)
        processed_request_count = 0
        successful_request_count = 0
        failed_request_count = 0
        inserted_event_count = 0
        reused_event_count = 0
        rematerialized_event_count = 0
        for request_plan in request_plans:
            request_nk = _build_tushare_request_nk(
                run_id=source_run_id,
                source_api=request_plan.source_api,
                cursor_type=request_plan.cursor_type,
                cursor_value=request_plan.cursor_value,
            )
            normalized_records: list[dict[str, object]] = []
            response_digest: str | None = None
            observed_max_date: date | None = None
            response_row_count = 0
            try:
                normalized_records, response_row_count, observed_max_date = _fetch_and_normalize_tushare_request(
                    client=client,
                    request_plan=request_plan,
                    run_id=source_run_id,
                    request_nk=request_nk,
                    scope_codes=scope_codes,
                )
                response_digest = _build_records_digest(
                    payload=[
                        _serialize_event_for_digest(record)
                        for record in sorted(normalized_records, key=lambda row: str(row["event_nk"]))
                    ]
                )
                checkpoint_row = _fetch_tushare_checkpoint(
                    connection,
                    source_api=request_plan.source_api,
                    cursor_type=request_plan.cursor_type,
                    cursor_value=request_plan.cursor_value,
                )
                if _should_skip_tushare_request(
                    checkpoint_row=checkpoint_row,
                    response_digest=response_digest,
                    last_observed_max_date=observed_max_date,
                ):
                    _record_tushare_objective_request(
                        connection,
                        request_nk=request_nk,
                        run_id=source_run_id,
                        source_api=request_plan.source_api,
                        cursor_type=request_plan.cursor_type,
                        cursor_value=request_plan.cursor_value,
                        response_row_count=response_row_count,
                        inserted_event_count=0,
                        reused_event_count=len(normalized_records),
                        rematerialized_event_count=0,
                        response_digest=response_digest,
                        request_status="skipped_unchanged",
                        error_message=None,
                    )
                    _upsert_tushare_checkpoint(
                        connection,
                        source_api=request_plan.source_api,
                        cursor_type=request_plan.cursor_type,
                        cursor_value=request_plan.cursor_value,
                        last_success_run_id=source_run_id,
                        last_response_digest=response_digest,
                        last_observed_max_date=observed_max_date,
                        last_status="skipped_unchanged",
                    )
                    processed_request_count += 1
                    successful_request_count += 1
                    reused_event_count += len(normalized_records)
                    continue

                connection.execute("BEGIN TRANSACTION")
                inserted_count, reused_count, rematerialized_count = _upsert_tushare_events(
                    connection,
                    records=normalized_records,
                )
                _record_tushare_objective_request(
                    connection,
                    request_nk=request_nk,
                    run_id=source_run_id,
                    source_api=request_plan.source_api,
                    cursor_type=request_plan.cursor_type,
                    cursor_value=request_plan.cursor_value,
                    response_row_count=response_row_count,
                    inserted_event_count=inserted_count,
                    reused_event_count=reused_count,
                    rematerialized_event_count=rematerialized_count,
                    response_digest=response_digest,
                    request_status="completed",
                    error_message=None,
                )
                _upsert_tushare_checkpoint(
                    connection,
                    source_api=request_plan.source_api,
                    cursor_type=request_plan.cursor_type,
                    cursor_value=request_plan.cursor_value,
                    last_success_run_id=source_run_id,
                    last_response_digest=response_digest,
                    last_observed_max_date=observed_max_date,
                    last_status="completed",
                )
                connection.execute("COMMIT")
                processed_request_count += 1
                successful_request_count += 1
                inserted_event_count += inserted_count
                reused_event_count += reused_count
                rematerialized_event_count += rematerialized_count
            except Exception as exc:
                failed_request_count += 1
                processed_request_count = successful_request_count + failed_request_count
                try:
                    connection.execute("ROLLBACK")
                except Exception:
                    pass
                _record_tushare_objective_request(
                    connection,
                    request_nk=request_nk,
                    run_id=source_run_id,
                    source_api=request_plan.source_api,
                    cursor_type=request_plan.cursor_type,
                    cursor_value=request_plan.cursor_value,
                    response_row_count=response_row_count,
                    inserted_event_count=0,
                    reused_event_count=0,
                    rematerialized_event_count=0,
                    response_digest=response_digest,
                    request_status="failed",
                    error_message=str(exc),
                )
                _update_tushare_objective_run_failure(
                    connection,
                    run_id=source_run_id,
                    summary_payload={
                        "run_id": source_run_id,
                        "error_message": str(exc),
                        "processed_request_count": processed_request_count,
                        "successful_request_count": successful_request_count,
                        "failed_request_count": failed_request_count,
                        "inserted_event_count": inserted_event_count,
                        "reused_event_count": reused_event_count,
                        "rematerialized_event_count": rematerialized_event_count,
                    },
                )
                run_terminal_recorded = True
                raise
        summary = TushareObjectiveSourceSyncSummary(
            run_id=source_run_id,
            source_api_scope=normalized_source_apis,
            signal_start_date=None if normalized_start_date is None else normalized_start_date.isoformat(),
            signal_end_date=None if normalized_end_date is None else normalized_end_date.isoformat(),
            candidate_cursor_count=len(request_plans),
            processed_request_count=processed_request_count,
            successful_request_count=successful_request_count,
            failed_request_count=failed_request_count,
            inserted_event_count=inserted_event_count,
            reused_event_count=reused_event_count,
            rematerialized_event_count=rematerialized_event_count,
            raw_market_path=str(resolved_raw_market_path),
        )
        _update_tushare_objective_run_success(connection, summary=summary)
        run_terminal_recorded = True
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception as exc:
        if not run_terminal_recorded:
            _update_tushare_objective_run_failure(
                connection,
                run_id=source_run_id,
                summary_payload={
                    "run_id": source_run_id,
                    "error_message": str(exc),
                    "processed_request_count": 0,
                    "successful_request_count": 0,
                    "failed_request_count": 0,
                    "inserted_event_count": 0,
                    "reused_event_count": 0,
                    "rematerialized_event_count": 0,
                },
            )
        raise
    finally:
        if client is not None:
            client.close()
        if owns_connection:
            connection.close()


def run_tushare_objective_profile_materialization(
    *,
    settings: WorkspaceRoots | None = None,
    raw_db_path: Path | str | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instrument_limit: int | None = None,
    instrument_list: list[str] | tuple[str, ...] | None = None,
    use_checkpoint_queue: bool = False,
    run_id: str | None = None,
    summary_path: Path | None = None,
) -> ObjectiveProfileMaterializationSummary:
    """把 Tushare objective event 有边界地物化进 `raw_tdxquant_instrument_profile`。"""

    normalized_start_date, normalized_end_date = _normalize_bounded_mode(
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        use_checkpoint_queue=use_checkpoint_queue,
    )
    normalized_instruments = _normalize_tushare_codes(instrument_list)
    normalized_limit = _normalize_limit(instrument_limit)
    workspace = settings or default_settings()
    connection, resolved_raw_market_path, owns_connection = _open_raw_market_connection(
        settings=workspace,
        raw_db_path=raw_db_path,
    )
    materialization_run_id = run_id or _build_run_id(prefix="tushare-objective-profile")
    run_terminal_recorded = False
    try:
        bootstrap_raw_market_ledger(workspace, connection=connection)
        candidate_profiles = (
            _load_profile_candidates_from_checkpoint_queue(
                connection,
                instrument_list=normalized_instruments,
                instrument_limit=normalized_limit,
            )
            if use_checkpoint_queue
            else _load_profile_candidates_from_trade_dates(
                connection,
                signal_start_date=normalized_start_date,
                signal_end_date=normalized_end_date,
                instrument_list=normalized_instruments,
                instrument_limit=normalized_limit,
            )
        )
        _insert_objective_profile_materialization_run_start(
            connection,
            run_id=materialization_run_id,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            candidate_profile_count=len(candidate_profiles),
        )
        events_by_code = _load_objective_events(
            connection,
            codes=tuple(sorted({code for _, code, _ in candidate_profiles})),
            signal_end_date=normalized_end_date,
        )
        processed_profile_count = 0
        inserted_profile_count = 0
        reused_profile_count = 0
        rematerialized_profile_count = 0
        for asset_type, code, observed_trade_date in candidate_profiles:
            try:
                derived_payload = _derive_profile_payload(
                    events_by_code=events_by_code,
                    asset_type=asset_type,
                    code=code,
                    observed_trade_date=observed_trade_date,
                    materialization_run_id=materialization_run_id,
                )
                checkpoint_nk = _build_objective_profile_checkpoint_nk(
                    asset_type=asset_type,
                    code=code,
                    observed_trade_date=observed_trade_date,
                )
                checkpoint_row = connection.execute(
                    f"""
                    SELECT source_digest
                    FROM {OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE}
                    WHERE checkpoint_nk = ?
                    """,
                    [checkpoint_nk],
                ).fetchone()
                existing_profile_row = connection.execute(
                    f"""
                    SELECT profile_nk, first_seen_run_id
                    FROM {RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE}
                    WHERE profile_nk = ?
                    """,
                    [derived_payload["profile_nk"]],
                ).fetchone()
                source_digest = _build_records_digest(payload=[derived_payload["source_detail"]])
                action = "inserted"
                if existing_profile_row is not None:
                    if checkpoint_row is not None and str(checkpoint_row[0]) == source_digest:
                        action = "reused"
                    else:
                        action = "rematerialized"
                connection.execute("BEGIN TRANSACTION")
                _upsert_profile_row(
                    connection,
                    payload=derived_payload,
                    existing_first_seen_run_id=(
                        None if existing_profile_row is None else _normalize_nullable_str(existing_profile_row[1])
                    ),
                )
                _upsert_objective_profile_checkpoint(
                    connection,
                    asset_type=asset_type,
                    code=code,
                    observed_trade_date=observed_trade_date,
                    source_digest=source_digest,
                    last_materialized_run_id=materialization_run_id,
                )
                _record_objective_profile_run_profile(
                    connection,
                    run_id=materialization_run_id,
                    asset_type=asset_type,
                    code=code,
                    observed_trade_date=observed_trade_date,
                    materialization_action=action,
                    source_digest=source_digest,
                )
                connection.execute("COMMIT")
                processed_profile_count += 1
                if action == "inserted":
                    inserted_profile_count += 1
                elif action == "reused":
                    reused_profile_count += 1
                else:
                    rematerialized_profile_count += 1
            except Exception as exc:
                try:
                    connection.execute("ROLLBACK")
                except Exception:
                    pass
                _update_objective_profile_materialization_run_failure(
                    connection,
                    run_id=materialization_run_id,
                    summary_payload={
                        "run_id": materialization_run_id,
                        "error_message": str(exc),
                        "processed_profile_count": processed_profile_count,
                        "inserted_profile_count": inserted_profile_count,
                        "reused_profile_count": reused_profile_count,
                        "rematerialized_profile_count": rematerialized_profile_count,
                    },
                )
                run_terminal_recorded = True
                raise
        summary = ObjectiveProfileMaterializationSummary(
            run_id=materialization_run_id,
            signal_start_date=None if normalized_start_date is None else normalized_start_date.isoformat(),
            signal_end_date=None if normalized_end_date is None else normalized_end_date.isoformat(),
            candidate_profile_count=len(candidate_profiles),
            processed_profile_count=processed_profile_count,
            inserted_profile_count=inserted_profile_count,
            reused_profile_count=reused_profile_count,
            rematerialized_profile_count=rematerialized_profile_count,
            raw_market_path=str(resolved_raw_market_path),
        )
        _update_objective_profile_materialization_run_success(connection, summary=summary)
        run_terminal_recorded = True
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception as exc:
        if not run_terminal_recorded:
            _update_objective_profile_materialization_run_failure(
                connection,
                run_id=materialization_run_id,
                summary_payload={
                    "run_id": materialization_run_id,
                    "error_message": str(exc),
                    "processed_profile_count": 0,
                    "inserted_profile_count": 0,
                    "reused_profile_count": 0,
                    "rematerialized_profile_count": 0,
                },
            )
        raise
    finally:
        if owns_connection:
            connection.close()


def _normalize_bounded_mode(
    *,
    signal_start_date: str | date | None,
    signal_end_date: str | date | None,
    use_checkpoint_queue: bool,
) -> tuple[date | None, date | None]:
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    has_window = normalized_start_date is not None or normalized_end_date is not None
    if use_checkpoint_queue and has_window:
        raise ValueError("Either provide bounded window or --use-checkpoint-queue, not both.")
    if not use_checkpoint_queue and (normalized_start_date is None or normalized_end_date is None):
        raise ValueError("Bounded mode requires both signal_start_date and signal_end_date.")
    if normalized_start_date is not None and normalized_end_date is not None and normalized_start_date > normalized_end_date:
        raise ValueError("signal_start_date must be <= signal_end_date")
    return normalized_start_date, normalized_end_date


def _open_raw_market_connection(
    *,
    settings: WorkspaceRoots,
    raw_db_path: Path | str | None,
) -> tuple[duckdb.DuckDBPyConnection, Path, bool]:
    if raw_db_path is None:
        settings.ensure_directories()
        resolved_path = raw_market_ledger_path(settings)
        return duckdb.connect(str(resolved_path)), resolved_path, True
    resolved_path = Path(raw_db_path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(resolved_path)), resolved_path, True


def _normalize_tushare_source_apis(source_apis: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    normalized_values: list[str] = []
    for source_api in source_apis or DEFAULT_TUSHARE_SOURCE_APIS:
        candidate = str(source_api).strip().lower()
        if not candidate:
            continue
        if candidate not in _SUPPORTED_TUSHARE_SOURCE_APIS:
            raise ValueError(f"Unsupported source_api: {source_api}")
        if candidate not in normalized_values:
            normalized_values.append(candidate)
    if not normalized_values:
        raise ValueError("source_apis must not be empty")
    return tuple(normalized_values)


def _normalize_tushare_codes(instruments: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    normalized_codes: list[str] = []
    for instrument in instruments or ():
        candidate = str(instrument).strip().upper()
        if not candidate:
            continue
        if "." not in candidate:
            raise ValueError(f"Tushare instruments must use full code format like 600000.SH: {instrument}")
        normalized_codes.append(candidate)
    return tuple(sorted(set(normalized_codes)))


def _normalize_nullable_str(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _resolve_scope_codes(
    connection: duckdb.DuckDBPyConnection,
    *,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instrument_list: tuple[str, ...],
    instrument_limit: int | None,
) -> tuple[str, ...]:
    if instrument_list:
        return instrument_list[:instrument_limit] if instrument_limit is not None else instrument_list
    where_clauses = ["asset_type = 'stock'"]
    parameters: list[object] = []
    if signal_start_date is not None:
        where_clauses.append("trade_date >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append("trade_date <= ?")
        parameters.append(signal_end_date)
    limit_sql = ""
    if instrument_limit is not None:
        limit_sql = "LIMIT ?"
        parameters.append(instrument_limit)
    rows = connection.execute(
        f"""
        SELECT DISTINCT code
        FROM {RAW_STOCK_DAILY_BAR_TABLE}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY code
        {limit_sql}
        """,
        parameters,
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _resolve_trade_dates(
    connection: duckdb.DuckDBPyConnection,
    *,
    signal_start_date: date | None,
    signal_end_date: date | None,
) -> tuple[date, ...]:
    if signal_start_date is None or signal_end_date is None:
        return ()
    rows = connection.execute(
        f"""
        SELECT DISTINCT trade_date
        FROM {RAW_STOCK_DAILY_BAR_TABLE}
        WHERE asset_type = 'stock'
          AND trade_date >= ?
          AND trade_date <= ?
        ORDER BY trade_date
        """,
        [signal_start_date, signal_end_date],
    ).fetchall()
    if rows:
        return tuple(_coerce_date(row[0]) for row in rows if _coerce_date(row[0]) is not None)
    return tuple(pd.date_range(signal_start_date, signal_end_date, freq="D").date)


def _build_bounded_request_plans(
    *,
    source_apis: tuple[str, ...],
    trade_dates: tuple[date, ...],
    scope_codes: tuple[str, ...],
) -> tuple[_TushareRequestPlan, ...]:
    request_plans: list[_TushareRequestPlan] = []
    for source_api in source_apis:
        if source_api == "stock_basic":
            request_plans.extend(
                _TushareRequestPlan(
                    source_api="stock_basic",
                    cursor_type="exchange_status",
                    cursor_value=f"{exchange}|{list_status}",
                )
                for exchange, list_status in _STOCK_BASIC_EXCHANGE_STATUS_SCOPE
            )
            continue
        if source_api in {"suspend_d", "stock_st"}:
            request_plans.extend(
                _TushareRequestPlan(
                    source_api=source_api,
                    cursor_type="trade_date",
                    cursor_value=trade_date.isoformat(),
                )
                for trade_date in trade_dates
            )
            continue
        if source_api == "namechange":
            request_plans.extend(
                _TushareRequestPlan(
                    source_api="namechange",
                    cursor_type="instrument",
                    cursor_value=code,
                )
                for code in scope_codes
            )
    return tuple(request_plans)


def _load_checkpoint_request_plans(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_apis: tuple[str, ...],
    instrument_list: tuple[str, ...],
    instrument_limit: int | None,
) -> tuple[_TushareRequestPlan, ...]:
    where_clauses = ["source_api IN (" + ", ".join("?" for _ in source_apis) + ")"]
    parameters: list[object] = [*source_apis]
    if instrument_list:
        where_clauses.append("(cursor_type <> 'instrument' OR cursor_value IN (" + ", ".join("?" for _ in instrument_list) + "))")
        parameters.extend(instrument_list)
    limit_sql = ""
    if instrument_limit is not None:
        limit_sql = "LIMIT ?"
        parameters.append(instrument_limit)
    rows = connection.execute(
        f"""
        SELECT source_api, cursor_type, cursor_value
        FROM {TUSHARE_OBJECTIVE_CHECKPOINT_TABLE}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY source_api, cursor_type, cursor_value
        {limit_sql}
        """,
        parameters,
    ).fetchall()
    return tuple(
        _TushareRequestPlan(source_api=str(row[0]), cursor_type=str(row[1]), cursor_value=str(row[2]))
        for row in rows
    )


def _fetch_and_normalize_tushare_request(
    *,
    client: TushareClient,
    request_plan: _TushareRequestPlan,
    run_id: str,
    request_nk: str,
    scope_codes: tuple[str, ...],
) -> tuple[list[dict[str, object]], int, date | None]:
    stable_request_ref = "|".join([request_plan.source_api, request_plan.cursor_type, request_plan.cursor_value])
    if request_plan.source_api == "stock_basic":
        exchange, list_status = request_plan.cursor_value.split("|", 1)
        rows = tuple(client.list_stock_basic(exchange=exchange, list_status=list_status))
        records = _normalize_stock_basic_rows(
            rows,
            run_id=run_id,
            request_nk=stable_request_ref,
            scope_codes=scope_codes,
        )
        records = _dedupe_event_records(records)
        return records, len(rows), _max_record_dates(records)
    if request_plan.source_api == "suspend_d":
        trade_date = _coerce_date(request_plan.cursor_value)
        if trade_date is None:
            raise ValueError(f"Invalid trade_date cursor: {request_plan.cursor_value}")
        rows = tuple(client.list_suspend_d(trade_date=trade_date))
        records = _normalize_suspend_rows(rows, run_id=run_id, request_nk=stable_request_ref)
        records = _dedupe_event_records(records)
        return records, len(rows), trade_date if not records else _max_record_dates(records)
    if request_plan.source_api == "stock_st":
        trade_date = _coerce_date(request_plan.cursor_value)
        if trade_date is None:
            raise ValueError(f"Invalid trade_date cursor: {request_plan.cursor_value}")
        rows = tuple(client.list_stock_st(trade_date=trade_date))
        records = _normalize_stock_st_rows(rows, run_id=run_id, request_nk=stable_request_ref)
        records = _dedupe_event_records(records)
        return records, len(rows), trade_date if not records else _max_record_dates(records)
    if request_plan.source_api == "namechange":
        rows = tuple(client.list_namechange(ts_code=request_plan.cursor_value))
        records = _normalize_namechange_rows(rows, run_id=run_id, request_nk=stable_request_ref)
        records = _dedupe_event_records(records)
        return records, len(rows), _max_record_dates(records)
    raise ValueError(f"Unsupported source_api: {request_plan.source_api}")


def _normalize_stock_basic_rows(
    rows: tuple[TushareStockBasicRow, ...],
    *,
    run_id: str,
    request_nk: str,
    scope_codes: tuple[str, ...],
) -> list[dict[str, object]]:
    allowed_codes = set(scope_codes)
    records: list[dict[str, object]] = []
    for row in rows:
        code = str(row.ts_code).strip().upper()
        if allowed_codes and code not in allowed_codes:
            continue
        payload = {
            "request_nk": request_nk,
            "ts_code": code,
            "name": row.name,
            "market": row.market,
            "exchange": row.exchange,
            "market_type": _map_tushare_exchange_to_market_type(row.exchange),
            "security_type": _map_stock_basic_to_security_type(row.market, row.exchange),
            "list_status": _map_list_status(row.list_status),
            "list_date": None if row.list_date is None else row.list_date.isoformat(),
            "delist_date": None if row.delist_date is None else row.delist_date.isoformat(),
        }
        records.append(
            _build_event_record(
                asset_type="stock",
                code=code,
                source_api="stock_basic",
                objective_dimension="instrument_metadata",
                effective_start_date=row.list_date or _DATE_FLOOR,
                effective_end_date=None,
                status_value_code=_map_list_status(row.list_status),
                status_value_text=row.market,
                source_trade_date=None,
                source_ann_date=None,
                payload=payload,
                run_id=run_id,
            )
        )
    return records


def _normalize_suspend_rows(
    rows: tuple[TushareSuspendRow, ...],
    *,
    run_id: str,
    request_nk: str,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in rows:
        payload = {
            "request_nk": request_nk,
            "ts_code": row.ts_code,
            "trade_date": row.trade_date.isoformat(),
            "suspend_timing": row.suspend_timing,
            "suspend_type": row.suspend_type,
        }
        status_value_code = "suspended" if str(row.suspend_type).strip().upper() == "S" else "resumed"
        records.append(
            _build_event_record(
                asset_type="stock",
                code=str(row.ts_code).strip().upper(),
                source_api="suspend_d",
                objective_dimension="suspension_status",
                effective_start_date=row.trade_date,
                effective_end_date=row.trade_date,
                status_value_code=status_value_code,
                status_value_text=status_value_code,
                source_trade_date=row.trade_date,
                source_ann_date=None,
                payload=payload,
                run_id=run_id,
            )
        )
    return records


def _normalize_stock_st_rows(
    rows: tuple[TushareStockStRow, ...],
    *,
    run_id: str,
    request_nk: str,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in rows:
        payload = {
            "request_nk": request_nk,
            "ts_code": row.ts_code,
            "trade_date": row.trade_date.isoformat(),
            "type": row.type,
            "type_name": row.type_name,
            "name": row.name,
        }
        records.append(
            _build_event_record(
                asset_type="stock",
                code=str(row.ts_code).strip().upper(),
                source_api="stock_st",
                objective_dimension="risk_warning_status",
                effective_start_date=row.trade_date,
                effective_end_date=row.trade_date,
                status_value_code="risk_warning",
                status_value_text=row.type_name or row.type,
                source_trade_date=row.trade_date,
                source_ann_date=None,
                payload=payload,
                run_id=run_id,
            )
        )
    return records


def _normalize_namechange_rows(
    rows: tuple[TushareNameChangeRow, ...],
    *,
    run_id: str,
    request_nk: str,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in rows:
        start_date = row.start_date or row.ann_date or _DATE_FLOOR
        payload = {
            "request_nk": request_nk,
            "ts_code": row.ts_code,
            "name": row.name,
            "start_date": None if row.start_date is None else row.start_date.isoformat(),
            "end_date": None if row.end_date is None else row.end_date.isoformat(),
            "ann_date": None if row.ann_date is None else row.ann_date.isoformat(),
            "change_reason": row.change_reason,
        }
        if _is_namechange_risk_warning(row):
            records.append(
                _build_event_record(
                    asset_type="stock",
                    code=str(row.ts_code).strip().upper(),
                    source_api="namechange",
                    objective_dimension="risk_warning_status",
                    effective_start_date=start_date,
                    effective_end_date=row.end_date,
                    status_value_code="risk_warning",
                    status_value_text=row.name or row.change_reason,
                    source_trade_date=None,
                    source_ann_date=row.ann_date,
                    payload=payload,
                    run_id=run_id,
                )
            )
        if _is_namechange_delisting(row):
            records.append(
                _build_event_record(
                    asset_type="stock",
                    code=str(row.ts_code).strip().upper(),
                    source_api="namechange",
                    objective_dimension="delisting_status",
                    effective_start_date=start_date,
                    effective_end_date=row.end_date,
                    status_value_code="delisting_arrangement",
                    status_value_text=row.name or row.change_reason,
                    source_trade_date=None,
                    source_ann_date=row.ann_date,
                    payload=payload,
                    run_id=run_id,
                )
            )
    return records


def _build_event_record(
    *,
    asset_type: str,
    code: str,
    source_api: str,
    objective_dimension: str,
    effective_start_date: date,
    effective_end_date: date | None,
    status_value_code: str | None,
    status_value_text: str | None,
    source_trade_date: date | None,
    source_ann_date: date | None,
    payload: dict[str, object],
    run_id: str,
) -> dict[str, object]:
    payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    source_record_hash = hashlib.sha256(
        json.dumps(
            {
                "source_api": source_api,
                "objective_dimension": objective_dimension,
                "effective_start_date": effective_start_date.isoformat(),
                "effective_end_date": None if effective_end_date is None else effective_end_date.isoformat(),
                "status_value_code": status_value_code,
                "payload": payload,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return {
        "event_nk": _build_tushare_event_nk(
            asset_type=asset_type,
            code=code,
            source_api=source_api,
            objective_dimension=objective_dimension,
            effective_start_date=effective_start_date,
            source_record_hash=source_record_hash,
        ),
        "asset_type": asset_type,
        "code": code,
        "source_api": source_api,
        "objective_dimension": objective_dimension,
        "effective_start_date": effective_start_date,
        "effective_end_date": effective_end_date,
        "status_value_code": status_value_code,
        "status_value_text": status_value_text,
        "source_record_hash": source_record_hash,
        "source_trade_date": source_trade_date,
        "source_ann_date": source_ann_date,
        "payload_json": payload_json,
        "first_seen_run_id": run_id,
        "last_seen_run_id": run_id,
    }


def _serialize_event_for_digest(record: dict[str, object]) -> dict[str, object]:
    return {
        "event_nk": record["event_nk"],
        "source_api": record["source_api"],
        "objective_dimension": record["objective_dimension"],
        "effective_start_date": _serialize_date(record["effective_start_date"]),
        "effective_end_date": _serialize_date(record["effective_end_date"]),
        "status_value_code": record["status_value_code"],
        "status_value_text": record["status_value_text"],
        "source_trade_date": _serialize_date(record["source_trade_date"]),
        "source_ann_date": _serialize_date(record["source_ann_date"]),
        "payload_json": record["payload_json"],
    }


def _serialize_date(value: object) -> str | None:
    coerced = _coerce_date(value)
    return None if coerced is None else coerced.isoformat()


def _max_record_dates(records: list[dict[str, object]]) -> date | None:
    max_date: date | None = None
    for record in records:
        for field_name in ("effective_start_date", "effective_end_date", "source_trade_date", "source_ann_date"):
            candidate = _coerce_date(record.get(field_name))
            if candidate is None:
                continue
            if max_date is None or candidate > max_date:
                max_date = candidate
    return max_date


def _dedupe_event_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    deduped: dict[str, dict[str, object]] = {}
    for record in records:
        deduped[str(record["event_nk"])] = record
    return [deduped[event_nk] for event_nk in sorted(deduped)]


def _map_tushare_exchange_to_market_type(exchange: str | None) -> str | None:
    normalized = str(exchange or "").strip().upper()
    if normalized in {"SSE", "SH"}:
        return "sh"
    if normalized in {"SZSE", "SZ"}:
        return "sz"
    if normalized in {"BSE", "BJ"}:
        return "bj"
    return None


def _map_stock_basic_to_security_type(market: str | None, exchange: str | None) -> str:
    _ = market
    _ = exchange
    return "stock"


def _map_list_status(list_status: str | None) -> str | None:
    normalized = str(list_status or "").strip().upper()
    if normalized == "L":
        return "listed"
    if normalized == "P":
        return "paused"
    if normalized == "D":
        return "delisted"
    return None


def _is_namechange_risk_warning(row: TushareNameChangeRow) -> bool:
    upper_name = str(row.name or "").upper()
    upper_reason = str(row.change_reason or "").upper()
    if "退市整理" in str(row.name or "") or "退市整理" in str(row.change_reason or ""):
        return False
    if "ST" in upper_name:
        return True
    if "ST" in upper_reason and "撤销" not in str(row.change_reason or "") and "摘帽" not in str(row.change_reason or ""):
        return True
    return False


def _is_namechange_delisting(row: TushareNameChangeRow) -> bool:
    return "退市整理" in str(row.name or "") or "退市整理" in str(row.change_reason or "")


def _load_profile_candidates_from_trade_dates(
    connection: duckdb.DuckDBPyConnection,
    *,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instrument_list: tuple[str, ...],
    instrument_limit: int | None,
) -> tuple[tuple[str, str, date], ...]:
    parameters: list[object] = []
    where_clauses = ["asset_type = 'stock'"]
    if signal_start_date is not None:
        where_clauses.append("trade_date >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append("trade_date <= ?")
        parameters.append(signal_end_date)
    if instrument_list:
        where_clauses.append("code IN (" + ", ".join("?" for _ in instrument_list) + ")")
        parameters.extend(instrument_list)
    limit_sql = ""
    if instrument_limit is not None:
        limit_sql = "LIMIT ?"
        parameters.append(instrument_limit)
    rows = connection.execute(
        f"""
        SELECT DISTINCT asset_type, code, trade_date
        FROM {RAW_STOCK_DAILY_BAR_TABLE}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY code, trade_date
        {limit_sql}
        """,
        parameters,
    ).fetchall()
    return tuple(
        (str(row[0]), str(row[1]), _coerce_date(row[2]) or _DATE_FLOOR)
        for row in rows
    )


def _load_profile_candidates_from_checkpoint_queue(
    connection: duckdb.DuckDBPyConnection,
    *,
    instrument_list: tuple[str, ...],
    instrument_limit: int | None,
) -> tuple[tuple[str, str, date], ...]:
    where_clauses = ["1 = 1"]
    parameters: list[object] = []
    if instrument_list:
        where_clauses.append("code IN (" + ", ".join("?" for _ in instrument_list) + ")")
        parameters.extend(instrument_list)
    limit_sql = ""
    if instrument_limit is not None:
        limit_sql = "LIMIT ?"
        parameters.append(instrument_limit)
    rows = connection.execute(
        f"""
        SELECT asset_type, code, observed_trade_date
        FROM {OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY code, observed_trade_date
        {limit_sql}
        """,
        parameters,
    ).fetchall()
    return tuple(
        (str(row[0]), str(row[1]), _coerce_date(row[2]) or _DATE_FLOOR)
        for row in rows
    )


def _load_objective_events(
    connection: duckdb.DuckDBPyConnection,
    *,
    codes: tuple[str, ...],
    signal_end_date: date | None,
) -> dict[str, list[_ObjectiveEventRow]]:
    if not codes:
        return {}
    parameters: list[object] = [*codes]
    where_clauses = ["code IN (" + ", ".join("?" for _ in codes) + ")"]
    if signal_end_date is not None:
        where_clauses.append("effective_start_date <= ?")
        parameters.append(signal_end_date)
    rows = connection.execute(
        f"""
        SELECT
            event_nk,
            asset_type,
            code,
            source_api,
            objective_dimension,
            effective_start_date,
            effective_end_date,
            status_value_code,
            status_value_text,
            source_trade_date,
            source_ann_date,
            payload_json,
            first_seen_run_id,
            last_seen_run_id
        FROM {TUSHARE_OBJECTIVE_EVENT_TABLE}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY code, objective_dimension, effective_start_date, event_nk
        """,
        parameters,
    ).fetchall()
    events_by_code: dict[str, list[_ObjectiveEventRow]] = {}
    for row in rows:
        code = str(row[2])
        events_by_code.setdefault(code, []).append(
            _ObjectiveEventRow(
                event_nk=str(row[0]),
                asset_type=str(row[1]),
                code=code,
                source_api=str(row[3]),
                objective_dimension=str(row[4]),
                effective_start_date=_coerce_date(row[5]) or _DATE_FLOOR,
                effective_end_date=_coerce_date(row[6]),
                status_value_code=_normalize_nullable_str(row[7]),
                status_value_text=_normalize_nullable_str(row[8]),
                source_trade_date=_coerce_date(row[9]),
                source_ann_date=_coerce_date(row[10]),
                payload_json=_normalize_nullable_str(row[11]),
                first_seen_run_id=str(row[12]),
                last_seen_run_id=str(row[13]),
            )
        )
    return events_by_code


def _derive_profile_payload(
    *,
    events_by_code: dict[str, list[_ObjectiveEventRow]],
    asset_type: str,
    code: str,
    observed_trade_date: date,
    materialization_run_id: str,
) -> dict[str, object]:
    code_events = events_by_code.get(code, [])
    metadata_event = _select_active_event(
        code_events,
        objective_dimension="instrument_metadata",
        observed_trade_date=observed_trade_date,
    )
    suspension_event = _select_active_event(
        code_events,
        objective_dimension="suspension_status",
        observed_trade_date=observed_trade_date,
    )
    stock_st_risk_event = _select_active_event(
        code_events,
        objective_dimension="risk_warning_status",
        observed_trade_date=observed_trade_date,
        source_api="stock_st",
    )
    namechange_risk_event = _select_active_event(
        code_events,
        objective_dimension="risk_warning_status",
        observed_trade_date=observed_trade_date,
        source_api="namechange",
    )
    delisting_event = _select_active_event(
        code_events,
        objective_dimension="delisting_status",
        observed_trade_date=observed_trade_date,
    )
    metadata_payload = _parse_payload_json(None if metadata_event is None else metadata_event.payload_json)
    instrument_name = _normalize_nullable_str(metadata_payload.get("name")) if metadata_payload else None
    market_type = _normalize_nullable_str(metadata_payload.get("market_type")) if metadata_payload else None
    if market_type is None:
        market_type = _infer_market_type_from_code(code)
    security_type = _normalize_nullable_str(metadata_payload.get("security_type")) if metadata_payload else "stock"
    list_date = _coerce_date(metadata_payload.get("list_date")) if metadata_payload else None
    delist_date = _coerce_date(metadata_payload.get("delist_date")) if metadata_payload else None
    raw_list_status = _normalize_nullable_str(metadata_payload.get("list_status")) if metadata_payload else None
    list_status = _derive_profile_list_status(
        observed_trade_date=observed_trade_date,
        list_date=list_date,
        delist_date=delist_date,
        raw_list_status=raw_list_status,
    )
    risk_event = stock_st_risk_event or namechange_risk_event
    is_suspended = suspension_event is not None and suspension_event.status_value_code == "suspended"
    is_risk_warning = risk_event is not None and risk_event.status_value_code == "risk_warning"
    is_delisting_arrangement = (
        delisting_event is not None and delisting_event.status_value_code == "delisting_arrangement"
    ) or (delist_date is not None and observed_trade_date >= delist_date)
    suspension_status = "suspended" if is_suspended else "trading"
    risk_warning_status = "st" if is_risk_warning else None
    delisting_status = "delisting_arrangement" if is_delisting_arrangement else None
    selected_events = [event for event in (metadata_event, suspension_event, risk_event, delisting_event) if event is not None]
    request_nks = tuple(
        sorted(
            {
                request_nk
                for event in selected_events
                for request_nk in _extract_request_nks_from_event(event)
            }
        )
    )
    source_detail = {
        "code": code,
        "asset_type": asset_type,
        "observed_trade_date": observed_trade_date.isoformat(),
        "dimensions": {
            "instrument_metadata": _serialize_selected_event(metadata_event),
            "suspension_status": _serialize_selected_event(suspension_event),
            "risk_warning_status": _serialize_selected_event(risk_event),
            "delisting_status": _serialize_selected_event(delisting_event),
        },
        "derived": {
            "instrument_name": instrument_name,
            "market_type": market_type,
            "security_type": security_type,
            "list_status": list_status,
            "list_date": None if list_date is None else list_date.isoformat(),
            "delist_date": None if delist_date is None else delist_date.isoformat(),
            "suspension_status": suspension_status,
            "risk_warning_status": risk_warning_status,
            "delisting_status": delisting_status,
            "is_suspended_or_unresumed": is_suspended,
            "is_risk_warning_excluded": is_risk_warning,
            "is_delisting_arrangement": is_delisting_arrangement,
        },
        "request_nks": list(request_nks),
    }
    profile_nk = "|".join([code, asset_type, observed_trade_date.isoformat()])
    source_request_nk = ",".join(request_nks) if request_nks else f"{materialization_run_id}|{code}|{observed_trade_date.isoformat()}"
    return {
        "profile_nk": profile_nk,
        "code": code,
        "asset_type": asset_type,
        "observed_trade_date": observed_trade_date,
        "name": instrument_name,
        "instrument_name": instrument_name,
        "market_type": market_type,
        "security_type": security_type,
        "list_status": list_status,
        "list_date": list_date,
        "delist_date": delist_date,
        "suspension_status": suspension_status,
        "risk_warning_status": risk_warning_status,
        "delisting_status": delisting_status,
        "is_suspended_or_unresumed": is_suspended,
        "is_risk_warning_excluded": is_risk_warning,
        "is_delisting_arrangement": is_delisting_arrangement,
        "source_owner": "tushare",
        "source_detail": source_detail,
        "source_run_id": materialization_run_id,
        "source_request_nk": source_request_nk,
        "raw_payload_json": json.dumps(source_detail, ensure_ascii=False, sort_keys=True),
        "first_seen_run_id": materialization_run_id if metadata_event is None else metadata_event.first_seen_run_id,
        "last_materialized_run_id": materialization_run_id,
    }


def _select_active_event(
    code_events: list[_ObjectiveEventRow],
    *,
    objective_dimension: str,
    observed_trade_date: date,
    source_api: str | None = None,
) -> _ObjectiveEventRow | None:
    selected: _ObjectiveEventRow | None = None
    for event in code_events:
        if event.objective_dimension != objective_dimension:
            continue
        if source_api is not None and event.source_api != source_api:
            continue
        if event.effective_start_date > observed_trade_date:
            continue
        if event.effective_end_date is not None and event.effective_end_date < observed_trade_date:
            continue
        if selected is None or event.effective_start_date >= selected.effective_start_date:
            selected = event
    return selected


def _parse_payload_json(payload_json: str | None) -> dict[str, object]:
    if payload_json is None or not payload_json.strip():
        return {}
    loaded = json.loads(payload_json)
    return loaded if isinstance(loaded, dict) else {}


def _extract_request_nks_from_event(event: _ObjectiveEventRow) -> tuple[str, ...]:
    payload = _parse_payload_json(event.payload_json)
    request_nk = _normalize_nullable_str(payload.get("request_nk"))
    if request_nk is None:
        return ()
    return (request_nk,)


def _serialize_selected_event(event: _ObjectiveEventRow | None) -> dict[str, object] | None:
    if event is None:
        return None
    payload = _parse_payload_json(event.payload_json)
    return {
        "event_nk": event.event_nk,
        "source_api": event.source_api,
        "objective_dimension": event.objective_dimension,
        "effective_start_date": event.effective_start_date.isoformat(),
        "effective_end_date": None if event.effective_end_date is None else event.effective_end_date.isoformat(),
        "status_value_code": event.status_value_code,
        "status_value_text": event.status_value_text,
        "request_nk": _normalize_nullable_str(payload.get("request_nk")),
    }


def _infer_market_type_from_code(code: str) -> str | None:
    normalized = str(code).strip().upper()
    if "." not in normalized:
        return None
    return _map_tushare_exchange_to_market_type(normalized.rsplit(".", 1)[1])


def _derive_profile_list_status(
    *,
    observed_trade_date: date,
    list_date: date | None,
    delist_date: date | None,
    raw_list_status: str | None,
) -> str | None:
    if list_date is not None and observed_trade_date < list_date:
        return None
    if delist_date is not None and observed_trade_date >= delist_date:
        return "delisted"
    if raw_list_status in {"paused", "delisted", "listed"}:
        if raw_list_status == "delisted":
            return "listed"
        return raw_list_status
    return "listed"


def _upsert_profile_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    payload: dict[str, object],
    existing_first_seen_run_id: str | None,
) -> None:
    source_detail_json = json.dumps(payload["source_detail"], ensure_ascii=False, sort_keys=True)
    existing = connection.execute(
        f"""
        SELECT profile_nk
        FROM {RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE}
        WHERE profile_nk = ?
        """,
        [payload["profile_nk"]],
    ).fetchone()
    first_seen_run_id = existing_first_seen_run_id or str(payload["first_seen_run_id"])
    parameters = [
        payload["profile_nk"],
        payload["code"],
        payload["asset_type"],
        payload["observed_trade_date"],
        payload["name"],
        payload["instrument_name"],
        payload["market_type"],
        payload["security_type"],
        payload["list_status"],
        payload["list_date"],
        payload["delist_date"],
        payload["suspension_status"],
        payload["risk_warning_status"],
        payload["delisting_status"],
        payload["is_suspended_or_unresumed"],
        payload["is_risk_warning_excluded"],
        payload["is_delisting_arrangement"],
        payload["source_owner"],
        source_detail_json,
        first_seen_run_id,
        payload["last_materialized_run_id"],
        payload["source_run_id"],
        payload["source_request_nk"],
        payload["raw_payload_json"],
    ]
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE} (
                profile_nk,
                code,
                asset_type,
                observed_trade_date,
                name,
                instrument_name,
                market_type,
                security_type,
                list_status,
                list_date,
                delist_date,
                suspension_status,
                risk_warning_status,
                delisting_status,
                is_suspended_or_unresumed,
                is_risk_warning_excluded,
                is_delisting_arrangement,
                source_owner,
                source_detail_json,
                first_seen_run_id,
                last_materialized_run_id,
                source_run_id,
                source_request_nk,
                raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            parameters,
        )
        return
    connection.execute(
        f"""
        UPDATE {RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE}
        SET
            code = ?,
            asset_type = ?,
            observed_trade_date = ?,
            name = ?,
            instrument_name = ?,
            market_type = ?,
            security_type = ?,
            list_status = ?,
            list_date = ?,
            delist_date = ?,
            suspension_status = ?,
            risk_warning_status = ?,
            delisting_status = ?,
            is_suspended_or_unresumed = ?,
            is_risk_warning_excluded = ?,
            is_delisting_arrangement = ?,
            source_owner = ?,
            source_detail_json = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            source_run_id = ?,
            source_request_nk = ?,
            raw_payload_json = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE profile_nk = ?
        """,
        [
            payload["code"],
            payload["asset_type"],
            payload["observed_trade_date"],
            payload["name"],
            payload["instrument_name"],
            payload["market_type"],
            payload["security_type"],
            payload["list_status"],
            payload["list_date"],
            payload["delist_date"],
            payload["suspension_status"],
            payload["risk_warning_status"],
            payload["delisting_status"],
            payload["is_suspended_or_unresumed"],
            payload["is_risk_warning_excluded"],
            payload["is_delisting_arrangement"],
            payload["source_owner"],
            source_detail_json,
            first_seen_run_id,
            payload["last_materialized_run_id"],
            payload["source_run_id"],
            payload["source_request_nk"],
            payload["raw_payload_json"],
            payload["profile_nk"],
        ],
    )


__all__ = [name for name in globals() if not name.startswith("__")]
