"""raw ingest 共享辅助逻辑。"""

from __future__ import annotations

from mlq.data.data_common import *
from mlq.data.data_shared import *

def _normalize_raw_run_mode(run_mode: str) -> str:
    normalized = str(run_mode).strip().lower()
    if normalized not in {"incremental", "full"}:
        raise ValueError(f"Unsupported raw run mode: {run_mode}")
    return normalized


def _resolve_raw_candidate_files(
    connection: duckdb.DuckDBPyConnection,
    *,
    adjust_method: str,
    source_root: Path,
    candidate_files: list[Path],
    continue_from_last_run: bool,
) -> list[Path]:
    if not continue_from_last_run:
        return candidate_files
    last_failed_run = connection.execute(
        f"""
        SELECT run_id
        FROM {RAW_INGEST_RUN_TABLE}
        WHERE adjust_method = ?
          AND source_root = ?
          AND run_status = 'failed'
        ORDER BY started_at DESC NULLS LAST, run_id DESC
        LIMIT 1
        """,
        [adjust_method, str(source_root)],
    ).fetchone()
    if last_failed_run is None:
        return candidate_files
    completed_source_paths = {
        str(row[0])
        for row in connection.execute(
            f"""
            SELECT source_path
            FROM {RAW_INGEST_FILE_TABLE}
            WHERE run_id = ?
              AND action <> 'failed'
            """,
            [str(last_failed_run[0])],
        ).fetchall()
    }
    if not completed_source_paths:
        return candidate_files
    return [path for path in candidate_files if str(path) not in completed_source_paths]


def _resolve_raw_candidate_files_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    adjust_method: str,
    source_root: Path,
    candidate_files: list[Path],
    continue_from_last_run: bool,
) -> list[Path]:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        return _resolve_raw_candidate_files(
            connection,
            adjust_method=adjust_method,
            source_root=source_root,
            candidate_files=candidate_files,
            continue_from_last_run=continue_from_last_run,
        )
    if not continue_from_last_run:
        return candidate_files
    last_failed_run = connection.execute(
        f"""
        SELECT run_id
        FROM {RAW_INGEST_RUN_TABLE}
        WHERE COALESCE(asset_type, ?) = ?
          AND adjust_method = ?
          AND source_root = ?
          AND run_status = 'failed'
        ORDER BY started_at DESC NULLS LAST, run_id DESC
        LIMIT 1
        """,
        [DEFAULT_ASSET_TYPE, normalized_asset_type, adjust_method, str(source_root)],
    ).fetchone()
    if last_failed_run is None:
        return candidate_files
    completed_source_paths = {
        str(row[0])
        for row in connection.execute(
            f"""
            SELECT source_path
            FROM {RAW_INGEST_FILE_TABLE}
            WHERE run_id = ?
              AND COALESCE(asset_type, ?) = ?
              AND action <> 'failed'
            """,
            [str(last_failed_run[0]), DEFAULT_ASSET_TYPE, normalized_asset_type],
        ).fetchall()
    }
    if not completed_source_paths:
        return candidate_files
    return [path for path in candidate_files if str(path) not in completed_source_paths]

def _should_skip_raw_file(
    *,
    path: Path,
    source_size_bytes: int,
    source_mtime_utc: datetime,
    registry_row: tuple[object, ...],
    force_hash: bool,
) -> bool:
    registry_size_bytes = int(registry_row[0])
    registry_mtime_utc = _normalize_timestamp(registry_row[1])
    same_size = registry_size_bytes == source_size_bytes
    same_mtime = registry_mtime_utc == source_mtime_utc
    if same_size and same_mtime and not force_hash:
        return True
    if not same_size and not force_hash:
        return False
    if same_size and not same_mtime or force_hash:
        stored_hash = None if registry_row[5] is None else str(registry_row[5])
        if not stored_hash:
            return False
        return stored_hash == _compute_file_content_hash(path)
    return False


def _compute_file_content_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _insert_raw_ingest_run_start(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    adjust_method: str,
    run_mode: str,
    source_root: Path,
    candidate_file_count: int,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {RAW_INGEST_RUN_TABLE} (
            run_id,
            asset_type,
            runner_name,
            runner_version,
            adjust_method,
            run_mode,
            source_root,
            candidate_file_count,
            run_status,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running', NULL)
        """,
        [
            run_id,
            DEFAULT_ASSET_TYPE,
            RAW_INGEST_RUNNER_NAME,
            RAW_INGEST_RUNNER_VERSION,
            adjust_method,
            run_mode,
            str(source_root),
            candidate_file_count,
        ],
    )


def _insert_raw_ingest_run_start_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    adjust_method: str,
    run_mode: str,
    source_root: Path,
    candidate_file_count: int,
) -> None:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        _insert_raw_ingest_run_start(
            connection,
            run_id=run_id,
            adjust_method=adjust_method,
            run_mode=run_mode,
            source_root=source_root,
            candidate_file_count=candidate_file_count,
        )
        return
    connection.execute(
        f"""
        INSERT INTO {RAW_INGEST_RUN_TABLE} (
            run_id,
            asset_type,
            runner_name,
            runner_version,
            adjust_method,
            run_mode,
            source_root,
            candidate_file_count,
            run_status,
            summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running', NULL)
        """,
        [
            run_id,
            normalized_asset_type,
            RAW_INGEST_RUNNER_NAME_BY_ASSET_TYPE[normalized_asset_type],
            RAW_INGEST_RUNNER_VERSION,
            adjust_method,
            run_mode,
            str(source_root),
            candidate_file_count,
        ],
    )


def _record_raw_ingest_file(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    file_nk: str,
    code: str,
    name: str,
    adjust_method: str,
    source_path: Path,
    fingerprint_mode: str,
    action: str,
    row_count: int,
    error_message: str | None,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {RAW_INGEST_FILE_TABLE} (
            run_id,
            asset_type,
            file_nk,
            code,
            name,
            adjust_method,
            source_path,
            fingerprint_mode,
            action,
            row_count,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            DEFAULT_ASSET_TYPE,
            file_nk,
            code,
            name,
            adjust_method,
            str(source_path),
            fingerprint_mode,
            action,
            row_count,
            error_message,
        ],
    )


def _record_raw_ingest_file_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    file_nk: str,
    code: str,
    name: str,
    adjust_method: str,
    source_path: Path,
    fingerprint_mode: str,
    action: str,
    row_count: int,
    error_message: str | None,
) -> None:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        _record_raw_ingest_file(
            connection,
            run_id=run_id,
            file_nk=file_nk,
            code=code,
            name=name,
            adjust_method=adjust_method,
            source_path=source_path,
            fingerprint_mode=fingerprint_mode,
            action=action,
            row_count=row_count,
            error_message=error_message,
        )
        return
    connection.execute(
        f"""
        INSERT INTO {RAW_INGEST_FILE_TABLE} (
            run_id,
            asset_type,
            file_nk,
            code,
            name,
            adjust_method,
            source_path,
            fingerprint_mode,
            action,
            row_count,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            normalized_asset_type,
            file_nk,
            code,
            name,
            adjust_method,
            str(source_path),
            fingerprint_mode,
            action,
            row_count,
            error_message,
        ],
    )


def _resolve_raw_file_action(
    *,
    inserted_count: int,
    reused_count: int,
    rematerialized_count: int,
) -> str:
    if rematerialized_count > 0:
        return "rematerialized"
    if inserted_count > 0:
        return "inserted"
    if reused_count > 0:
        return "reused"
    return "reused"


def _update_raw_ingest_run_success(
    connection: duckdb.DuckDBPyConnection,
    *,
    summary: TdxStockRawIngestSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {RAW_INGEST_RUN_TABLE}
        SET
            processed_file_count = ?,
            skipped_file_count = ?,
            inserted_bar_count = ?,
            reused_bar_count = ?,
            rematerialized_bar_count = ?,
            run_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            summary.processed_file_count,
            summary.skipped_unchanged_file_count,
            summary.bar_inserted_count,
            summary.bar_reused_count,
            summary.bar_rematerialized_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            summary.run_id,
        ],
    )


def _update_raw_ingest_run_failure(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    adjust_method: str,
    run_mode: str,
    source_root: Path,
    candidate_file_count: int,
    processed_file_count: int,
    skipped_file_count: int,
    inserted_bar_count: int,
    reused_bar_count: int,
    rematerialized_bar_count: int,
    failed_file_count: int,
    error_message: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {RAW_INGEST_RUN_TABLE}
        SET
            asset_type = ?,
            adjust_method = ?,
            run_mode = ?,
            source_root = ?,
            candidate_file_count = ?,
            processed_file_count = ?,
            skipped_file_count = ?,
            inserted_bar_count = ?,
            reused_bar_count = ?,
            rematerialized_bar_count = ?,
            run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            DEFAULT_ASSET_TYPE,
            adjust_method,
            run_mode,
            str(source_root),
            candidate_file_count,
            processed_file_count,
            skipped_file_count,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            json.dumps(
                {
                    "error_message": error_message,
                    "failed_file_count": failed_file_count,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            run_id,
        ],
    )


def _update_raw_ingest_run_failure_by_asset(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    asset_type: str,
    adjust_method: str,
    run_mode: str,
    source_root: Path,
    candidate_file_count: int,
    processed_file_count: int,
    skipped_file_count: int,
    inserted_bar_count: int,
    reused_bar_count: int,
    rematerialized_bar_count: int,
    failed_file_count: int,
    error_message: str,
) -> None:
    normalized_asset_type = _normalize_asset_type(asset_type)
    if normalized_asset_type == DEFAULT_ASSET_TYPE:
        _update_raw_ingest_run_failure(
            connection,
            run_id=run_id,
            adjust_method=adjust_method,
            run_mode=run_mode,
            source_root=source_root,
            candidate_file_count=candidate_file_count,
            processed_file_count=processed_file_count,
            skipped_file_count=skipped_file_count,
            inserted_bar_count=inserted_bar_count,
            reused_bar_count=reused_bar_count,
            rematerialized_bar_count=rematerialized_bar_count,
            failed_file_count=failed_file_count,
            error_message=error_message,
        )
        return
    connection.execute(
        f"""
        UPDATE {RAW_INGEST_RUN_TABLE}
        SET
            asset_type = ?,
            adjust_method = ?,
            run_mode = ?,
            source_root = ?,
            candidate_file_count = ?,
            processed_file_count = ?,
            skipped_file_count = ?,
            inserted_bar_count = ?,
            reused_bar_count = ?,
            rematerialized_bar_count = ?,
            run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            normalized_asset_type,
            adjust_method,
            run_mode,
            str(source_root),
            candidate_file_count,
            processed_file_count,
            skipped_file_count,
            inserted_bar_count,
            reused_bar_count,
            rematerialized_bar_count,
            json.dumps(
                {
                    "asset_type": normalized_asset_type,
                    "error_message": error_message,
                    "failed_file_count": failed_file_count,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            run_id,
        ],
    )



__all__ = [name for name in globals() if not name.startswith("__")]

