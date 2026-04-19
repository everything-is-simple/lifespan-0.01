"""`Tushare objective source -> raw_tdxquant_instrument_profile` ?? runner?"""

from __future__ import annotations

from pathlib import Path

from mlq.data.data_common import *
from mlq.data.data_shared import *
from mlq.data.data_tushare_objective_helpers import (
    DEFAULT_TUSHARE_SOURCE_APIS,
    _SUPPORTED_TUSHARE_SOURCE_APIS,
    _build_bounded_request_plans,
    _derive_profile_payload,
    _fetch_and_normalize_tushare_request,
    _load_checkpoint_request_plans,
    _load_objective_events,
    _load_profile_candidates_from_checkpoint_queue,
    _load_profile_candidates_from_trade_dates,
    _resolve_scope_codes,
    _resolve_trade_dates,
    _serialize_event_for_digest,
    _upsert_profile_row,
)
from mlq.data.data_tushare_objective_support import (
    build_objective_profile_checkpoint_nk as _build_objective_profile_checkpoint_nk,
    build_records_digest as _build_records_digest,
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
from mlq.data.tushare import TushareClient, open_tushare_client

_OBJECTIVE_PROFILE_MATERIALIZATION_BATCH_SIZE = 1000

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

def _load_existing_materialization_state(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_profiles: tuple[tuple[str, str, date], ...],
) -> tuple[dict[str, str], dict[str, str | None]]:
    if not candidate_profiles:
        return {}, {}
    codes = tuple(sorted({code for _, code, _ in candidate_profiles}))
    observed_dates = tuple(observed_trade_date for _, _, observed_trade_date in candidate_profiles)
    min_observed_trade_date = min(observed_dates)
    max_observed_trade_date = max(observed_dates)
    code_predicate = ", ".join("?" for _ in codes)
    parameters = [*codes, min_observed_trade_date, max_observed_trade_date]
    checkpoint_rows = connection.execute(
        f"""
        SELECT checkpoint_nk, source_digest
        FROM {OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE}
        WHERE code IN ({code_predicate})
          AND observed_trade_date >= ?
          AND observed_trade_date <= ?
        """,
        parameters,
    ).fetchall()
    profile_rows = connection.execute(
        f"""
        SELECT profile_nk, first_seen_run_id
        FROM {RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE}
        WHERE code IN ({code_predicate})
          AND observed_trade_date >= ?
          AND observed_trade_date <= ?
        """,
        parameters,
    ).fetchall()
    return (
        {
            str(checkpoint_nk): _normalize_nullable_str(source_digest) or ""
            for checkpoint_nk, source_digest in checkpoint_rows
        },
        {
            str(profile_nk): _normalize_nullable_str(first_seen_run_id)
            for profile_nk, first_seen_run_id in profile_rows
        },
    )



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
        if client_factory is None:
            client = open_tushare_client(token=tushare_token)
        else:
            client = client_factory(tushare_token)
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
        checkpoint_digest_by_nk, profile_first_seen_run_id_by_nk = _load_existing_materialization_state(
            connection,
            candidate_profiles=candidate_profiles,
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
        batch_processed_profile_count = 0
        batch_inserted_profile_count = 0
        batch_reused_profile_count = 0
        batch_rematerialized_profile_count = 0
        transaction_open = False
        for asset_type, code, observed_trade_date in candidate_profiles:
            try:
                if not transaction_open:
                    connection.execute("BEGIN TRANSACTION")
                    transaction_open = True
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
                profile_nk = str(derived_payload["profile_nk"])
                existing_first_seen_run_id = profile_first_seen_run_id_by_nk.get(profile_nk)
                checkpoint_digest = checkpoint_digest_by_nk.get(checkpoint_nk)
                source_digest = _build_records_digest(payload=[derived_payload["source_detail"]])
                action = "inserted"
                if existing_first_seen_run_id is not None:
                    if checkpoint_digest == source_digest:
                        action = "reused"
                    else:
                        action = "rematerialized"
                resolved_first_seen_run_id = existing_first_seen_run_id or _normalize_nullable_str(
                    derived_payload["first_seen_run_id"]
                )
                _upsert_profile_row(
                    connection,
                    payload=derived_payload,
                    existing_first_seen_run_id=resolved_first_seen_run_id,
                    profile_exists=existing_first_seen_run_id is not None,
                )
                _upsert_objective_profile_checkpoint(
                    connection,
                    asset_type=asset_type,
                    code=code,
                    observed_trade_date=observed_trade_date,
                    source_digest=source_digest,
                    last_materialized_run_id=materialization_run_id,
                    checkpoint_exists=checkpoint_digest is not None,
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
                profile_first_seen_run_id_by_nk[profile_nk] = resolved_first_seen_run_id or materialization_run_id
                checkpoint_digest_by_nk[checkpoint_nk] = source_digest
                batch_processed_profile_count += 1
                if action == "inserted":
                    batch_inserted_profile_count += 1
                elif action == "reused":
                    batch_reused_profile_count += 1
                else:
                    batch_rematerialized_profile_count += 1
                if batch_processed_profile_count >= _OBJECTIVE_PROFILE_MATERIALIZATION_BATCH_SIZE:
                    connection.execute("COMMIT")
                    transaction_open = False
                    processed_profile_count += batch_processed_profile_count
                    inserted_profile_count += batch_inserted_profile_count
                    reused_profile_count += batch_reused_profile_count
                    rematerialized_profile_count += batch_rematerialized_profile_count
                    batch_processed_profile_count = 0
                    batch_inserted_profile_count = 0
                    batch_reused_profile_count = 0
                    batch_rematerialized_profile_count = 0
            except Exception as exc:
                try:
                    if transaction_open:
                        connection.execute("ROLLBACK")
                except Exception:
                    pass
                transaction_open = False
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
        if transaction_open:
            connection.execute("COMMIT")
            processed_profile_count += batch_processed_profile_count
            inserted_profile_count += batch_inserted_profile_count
            reused_profile_count += batch_reused_profile_count
            rematerialized_profile_count += batch_rematerialized_profile_count
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




__all__ = [name for name in globals() if not name.startswith("__")]
