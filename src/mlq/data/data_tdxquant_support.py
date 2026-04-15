"""`TdxQuant none` raw sync 的 checkpoint / audit / digest 辅助。"""

from __future__ import annotations

from mlq.data.data_common import *
from mlq.data.data_shared import *


def build_tdxquant_request_nk(
    *,
    run_id: str,
    code: str,
    requested_dividend_type: str,
    requested_count: int,
    requested_end_time: str,
) -> str:
    """构建单次 TdxQuant 请求自然键。"""

    return "|".join([run_id, code, requested_dividend_type, str(requested_count), requested_end_time])


def build_tdxquant_checkpoint_nk(*, code: str, asset_type: str) -> str:
    """构建 TdxQuant instrument checkpoint 自然键。"""

    return "|".join([code, asset_type])


def fetch_tdxquant_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    code: str,
    asset_type: str,
) -> tuple[object, ...] | None:
    """读取 instrument checkpoint。"""

    return connection.execute(
        f"""
        SELECT
            checkpoint_nk,
            last_success_trade_date,
            last_observed_trade_date,
            last_success_run_id,
            last_response_digest
        FROM {RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE}
        WHERE checkpoint_nk = ?
        """,
        [build_tdxquant_checkpoint_nk(code=code, asset_type=asset_type)],
    ).fetchone()


def should_skip_tdxquant_request(
    *,
    checkpoint_row: tuple[object, ...],
    response_trade_date_max: date,
    response_digest: str,
) -> bool:
    """判断本次响应是否与 checkpoint 完全一致。"""

    last_observed = checkpoint_row[2]
    last_digest = checkpoint_row[4]
    if last_observed is None or last_digest is None:
        return False
    return _coerce_date(last_observed) == response_trade_date_max and str(last_digest) == response_digest


def build_tdxquant_response_digest(
    *,
    instrument_info: TdxQuantInstrumentInfo,
    response_bars: tuple[TdxQuantDailyBar, ...],
) -> str:
    """构建响应指纹，判断是否需要重写 raw bars。"""

    payload = {
        "code": instrument_info.code,
        "name": instrument_info.name,
        "asset_type": instrument_info.asset_type,
        "bars": [
            {
                "trade_date": row.trade_date.isoformat(),
                "open": _normalize_float(row.open),
                "high": _normalize_float(row.high),
                "low": _normalize_float(row.low),
                "close": _normalize_float(row.close),
                "volume": _normalize_float(row.volume),
                "amount": _normalize_float(row.amount),
            }
            for row in response_bars
        ],
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def record_raw_tdxquant_request(
    connection: duckdb.DuckDBPyConnection,
    *,
    request_nk: str,
    run_id: str,
    asset_type: str,
    code: str,
    name: str,
    requested_dividend_type: str,
    requested_count: int,
    requested_end_time: str,
    response_trade_date_min: date | None,
    response_trade_date_max: date | None,
    response_row_count: int,
    response_digest: str | None,
    inserted_bar_count: int,
    reused_bar_count: int,
    rematerialized_bar_count: int,
    status: str,
    error_message: str | None,
) -> None:
    """回写单次请求审计账本。"""

    connection.execute(
        f"""
        INSERT OR REPLACE INTO {RAW_TDXQUANT_REQUEST_TABLE} (
            request_nk,
            run_id,
            asset_type,
            code,
            name,
            requested_dividend_type,
            requested_count,
            requested_end_time,
            response_trade_date_min,
            response_trade_date_max,
            response_row_count,
            response_digest,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            status,
            error_message,
            recorded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            request_nk,
            run_id,
            asset_type,
            code,
            name,
            requested_dividend_type,
            requested_count,
            requested_end_time,
            response_trade_date_min,
            response_trade_date_max,
            response_row_count,
            response_digest,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            status,
            error_message,
        ],
    )


def upsert_tdxquant_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    code: str,
    asset_type: str,
    last_success_trade_date: date | None,
    last_observed_trade_date: date | None,
    last_success_run_id: str,
    last_response_digest: str | None,
) -> None:
    """回写 instrument checkpoint。"""

    checkpoint_nk = build_tdxquant_checkpoint_nk(code=code, asset_type=asset_type)
    existing = connection.execute(
        f"""
        SELECT checkpoint_nk
        FROM {RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE}
        WHERE checkpoint_nk = ?
        """,
        [checkpoint_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE} (
                checkpoint_nk,
                code,
                asset_type,
                last_success_trade_date,
                last_observed_trade_date,
                last_success_run_id,
                last_response_digest
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                checkpoint_nk,
                code,
                asset_type,
                last_success_trade_date,
                last_observed_trade_date,
                last_success_run_id,
                last_response_digest,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {RAW_TDXQUANT_INSTRUMENT_CHECKPOINT_TABLE}
        SET
            last_success_trade_date = ?,
            last_observed_trade_date = ?,
            last_success_run_id = ?,
            last_response_digest = ?,
            updated_at_utc = CURRENT_TIMESTAMP
        WHERE checkpoint_nk = ?
        """,
        [
            last_success_trade_date,
            last_observed_trade_date,
            last_success_run_id,
            last_response_digest,
            checkpoint_nk,
        ],
    )


def insert_raw_tdxquant_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    strategy_path: Path,
    scope_source: str,
    requested_end_trade_date: date,
    requested_count: int,
    candidate_instrument_count: int,
) -> None:
    """登记 raw_tdxquant_run 起点。"""

    connection.execute(
        f"""
        INSERT OR REPLACE INTO {RAW_TDXQUANT_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            strategy_path,
            scope_source,
            requested_end_trade_date,
            requested_count,
            candidate_instrument_count,
            processed_instrument_count,
            successful_request_count,
            failed_request_count,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            dirty_mark_count,
            run_status,
            started_at_utc,
            finished_at_utc,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 'running', CURRENT_TIMESTAMP, NULL, NULL)
        """,
        [
            run_id,
            TDXQUANT_DAILY_RAW_SYNC_RUNNER_NAME,
            TDXQUANT_DAILY_RAW_SYNC_RUNNER_VERSION,
            str(strategy_path),
            scope_source,
            requested_end_trade_date,
            requested_count,
            candidate_instrument_count,
        ],
    )


def update_raw_tdxquant_run_success(
    connection: duckdb.DuckDBPyConnection,
    *,
    summary: TdxQuantDailyRawSyncSummary,
) -> None:
    """把 raw_tdxquant_run 标记为 completed。"""

    connection.execute(
        f"""
        UPDATE {RAW_TDXQUANT_RUN_TABLE}
        SET
            processed_instrument_count = ?,
            successful_request_count = ?,
            failed_request_count = ?,
            inserted_bar_count = ?,
            reused_bar_count = ?,
            rematerialized_bar_count = ?,
            dirty_mark_count = ?,
            run_status = 'completed',
            finished_at_utc = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            summary.processed_instrument_count,
            summary.successful_request_count,
            summary.failed_request_count,
            summary.inserted_bar_count,
            summary.reused_bar_count,
            summary.rematerialized_bar_count,
            summary.dirty_mark_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            summary.run_id,
        ],
    )


def update_raw_tdxquant_run_failure(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    strategy_path: Path,
    scope_source: str,
    requested_end_trade_date: date,
    requested_count: int,
    candidate_instrument_count: int,
    processed_instrument_count: int,
    successful_request_count: int,
    failed_request_count: int,
    inserted_bar_count: int,
    reused_bar_count: int,
    rematerialized_bar_count: int,
    dirty_mark_count: int,
    error_message: str,
) -> None:
    """把 raw_tdxquant_run 标记为 failed。"""

    summary_payload = {
        "run_id": run_id,
        "strategy_path": str(strategy_path),
        "scope_source": scope_source,
        "requested_end_trade_date": requested_end_trade_date.isoformat(),
        "requested_count": requested_count,
        "candidate_instrument_count": candidate_instrument_count,
        "processed_instrument_count": processed_instrument_count,
        "successful_request_count": successful_request_count,
        "failed_request_count": failed_request_count,
        "inserted_bar_count": inserted_bar_count,
        "reused_bar_count": reused_bar_count,
        "rematerialized_bar_count": rematerialized_bar_count,
        "dirty_mark_count": dirty_mark_count,
        "error_message": error_message,
    }
    connection.execute(
        f"""
        UPDATE {RAW_TDXQUANT_RUN_TABLE}
        SET
            strategy_path = ?,
            scope_source = ?,
            requested_end_trade_date = ?,
            requested_count = ?,
            candidate_instrument_count = ?,
            processed_instrument_count = ?,
            successful_request_count = ?,
            failed_request_count = ?,
            inserted_bar_count = ?,
            reused_bar_count = ?,
            rematerialized_bar_count = ?,
            dirty_mark_count = ?,
            run_status = 'failed',
            finished_at_utc = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            str(strategy_path),
            scope_source,
            requested_end_trade_date,
            requested_count,
            candidate_instrument_count,
            processed_instrument_count,
            successful_request_count,
            failed_request_count,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            dirty_mark_count,
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )
