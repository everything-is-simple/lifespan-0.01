"""执行 `filter snapshot` 官方 producer 的最小 bounded 运行时。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.filter.bootstrap import (
    FILTER_CHECKPOINT_TABLE,
    FILTER_WORK_QUEUE_TABLE,
    bootstrap_filter_snapshot_ledger,
    filter_ledger_path,
)
from mlq.filter.filter_materialization import (
    _insert_run_row,
    _mark_run_completed,
    _materialize_filter_rows,
    _update_run_summary,
)
from mlq.filter.filter_shared import (
    FilterSnapshotBuildSummary,
    _build_filter_run_id,
    _build_queue_nk,
    _build_scope_nk,
    _coerce_date,
    _to_python_date,
    _write_summary,
)
from mlq.filter.filter_source import (
    _ensure_database_exists,
    _load_context_presence,
    _load_structure_snapshot_rows,
)
from mlq.malf.bootstrap import MALF_STATE_SNAPSHOT_TABLE
from mlq.structure.bootstrap import STRUCTURE_CHECKPOINT_TABLE


DEFAULT_FILTER_STRUCTURE_TABLE: Final[str] = "structure_snapshot"
DEFAULT_FILTER_CONTEXT_TABLE: Final[str] = MALF_STATE_SNAPSHOT_TABLE
DEFAULT_FILTER_SOURCE_TIMEFRAME: Final[str] = "D"
DEFAULT_FILTER_CONTRACT_VERSION: Final[str] = "filter-snapshot-v2"


def run_filter_snapshot_build(
    *,
    settings: WorkspaceRoots | None = None,
    filter_path: Path | None = None,
    structure_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    run_id: str | None = None,
    source_structure_table: str = DEFAULT_FILTER_STRUCTURE_TABLE,
    source_context_table: str = DEFAULT_FILTER_CONTEXT_TABLE,
    source_timeframe: str = DEFAULT_FILTER_SOURCE_TIMEFRAME,
    filter_contract_version: str = DEFAULT_FILTER_CONTRACT_VERSION,
    runner_name: str = "filter_snapshot_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
    use_checkpoint_queue: bool | None = None,
) -> FilterSnapshotBuildSummary:
    """从官方 `structure snapshot` 物化 `filter snapshot`。"""

    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_timeframe = _normalize_timeframe(source_timeframe)
    _validate_filter_mainline_contract(
        source_structure_table=source_structure_table,
        source_context_table=source_context_table,
        source_timeframe=normalized_timeframe,
    )
    if _should_use_queue_execution(
        use_checkpoint_queue=use_checkpoint_queue,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
    ):
        return _run_filter_queue_build(
            settings=settings,
            filter_path=filter_path,
            structure_path=structure_path,
            malf_path=malf_path,
            limit=limit,
            batch_size=batch_size,
            run_id=run_id,
            source_structure_table=source_structure_table,
            source_context_table=source_context_table,
            source_timeframe=normalized_timeframe,
            filter_contract_version=filter_contract_version,
            runner_name=runner_name,
            runner_version=runner_version,
            summary_path=summary_path,
        )
    return _run_filter_bounded_build(
        settings=settings,
        filter_path=filter_path,
        structure_path=structure_path,
        malf_path=malf_path,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        limit=limit,
        batch_size=batch_size,
        run_id=run_id,
        source_structure_table=source_structure_table,
        source_context_table=source_context_table,
        source_timeframe=normalized_timeframe,
        filter_contract_version=filter_contract_version,
        runner_name=runner_name,
        runner_version=runner_version,
        summary_path=summary_path,
    )


def _validate_filter_mainline_contract(
    *,
    source_structure_table: str,
    source_context_table: str,
    source_timeframe: str,
) -> None:
    if source_structure_table != DEFAULT_FILTER_STRUCTURE_TABLE:
        raise ValueError(
            "filter mainline only accepts official `structure_snapshot` as source_structure_table."
        )
    if source_context_table != DEFAULT_FILTER_CONTEXT_TABLE:
        raise ValueError(
            "filter mainline only accepts canonical `malf_state_snapshot` as source_context_table."
        )
    if source_timeframe != DEFAULT_FILTER_SOURCE_TIMEFRAME:
        raise ValueError("filter mainline only accepts canonical timeframe `D`.")


def _run_filter_bounded_build(
    *,
    settings: WorkspaceRoots | None,
    filter_path: Path | None,
    structure_path: Path | None,
    malf_path: Path | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    batch_size: int,
    run_id: str | None,
    source_structure_table: str,
    source_context_table: str,
    source_timeframe: str,
    filter_contract_version: str,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> FilterSnapshotBuildSummary:
    """执行显式 bounded window 物化。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_filter_path = Path(filter_path or filter_ledger_path(workspace))
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    resolved_malf_path = Path(malf_path or workspace.databases.malf)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_timeframe = _normalize_timeframe(source_timeframe)
    materialization_run_id = run_id or _build_filter_run_id()

    _ensure_database_exists(resolved_structure_path, label="structure")
    structure_rows = _load_structure_snapshot_rows(
        structure_path=resolved_structure_path,
        table_name=source_structure_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=instruments,
        limit=normalized_limit,
    )
    context_presence = _load_context_presence(
        malf_path=resolved_malf_path,
        table_name=source_context_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=tuple(sorted({row.instrument for row in structure_rows})),
        timeframe=normalized_timeframe,
    )

    filter_connection = duckdb.connect(str(resolved_filter_path))
    try:
        bootstrap_filter_snapshot_ledger(workspace, connection=filter_connection)
        _insert_run_row(
            filter_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            bounded_instrument_count=len({row.instrument for row in structure_rows}),
            source_structure_table=source_structure_table,
            source_context_table=source_context_table,
            filter_contract_version=filter_contract_version,
        )
        summary = _materialize_filter_rows(
            connection=filter_connection,
            run_id=materialization_run_id,
            structure_rows=structure_rows,
            context_presence=context_presence,
            filter_contract_version=filter_contract_version,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            malf_path=resolved_malf_path,
            source_structure_table=source_structure_table,
            source_context_table=source_context_table,
            source_timeframe=normalized_timeframe,
            batch_size=normalized_batch_size,
        )
        _mark_run_completed(filter_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            filter_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        filter_connection.close()


def _should_use_queue_execution(
    *,
    use_checkpoint_queue: bool | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> bool:
    if use_checkpoint_queue is not None:
        return use_checkpoint_queue
    return signal_start_date is None and signal_end_date is None and not instruments


def _normalize_timeframe(value: str | None) -> str:
    candidate = str(value or DEFAULT_FILTER_SOURCE_TIMEFRAME).strip().upper()
    return candidate or DEFAULT_FILTER_SOURCE_TIMEFRAME


def _run_filter_queue_build(
    *,
    settings: WorkspaceRoots | None,
    filter_path: Path | None,
    structure_path: Path | None,
    malf_path: Path | None,
    limit: int,
    batch_size: int,
    run_id: str | None,
    source_structure_table: str,
    source_context_table: str,
    source_timeframe: str,
    filter_contract_version: str,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> FilterSnapshotBuildSummary:
    """执行默认 data-grade queue/checkpoint 续跑。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_filter_path = Path(filter_path or filter_ledger_path(workspace))
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    resolved_malf_path = Path(malf_path or workspace.databases.malf)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_timeframe = _normalize_timeframe(source_timeframe)
    materialization_run_id = run_id or _build_filter_run_id()

    _ensure_database_exists(resolved_structure_path, label="structure")
    filter_connection = duckdb.connect(str(resolved_filter_path))
    try:
        bootstrap_filter_snapshot_ledger(workspace, connection=filter_connection)
        dirty_scopes = _load_filter_dirty_scopes(
            structure_path=resolved_structure_path,
            limit=normalized_limit,
            timeframe=normalized_timeframe,
        )
        enqueue_counts = _enqueue_filter_dirty_scopes(
            connection=filter_connection,
            scope_rows=dirty_scopes,
            run_id=materialization_run_id,
        )
        claimed_scope_rows = _claim_filter_scopes(
            connection=filter_connection,
            run_id=materialization_run_id,
            timeframe=normalized_timeframe,
        )
        _insert_run_row(
            filter_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            source_structure_table=source_structure_table,
            source_context_table=source_context_table,
            filter_contract_version=filter_contract_version,
        )

        summary_counts = {
            "candidate_structure_count": 0,
            "materialized_snapshot_count": 0,
            "inserted_count": 0,
            "reused_count": 0,
            "rematerialized_count": 0,
            "missing_context_count": 0,
            "admissible_count": 0,
            "blocked_count": 0,
        }
        checkpoint_upserted_count = 0
        for scope_row in claimed_scope_rows:
            try:
                scope_summary = _materialize_filter_scope(
                    connection=filter_connection,
                    run_id=materialization_run_id,
                    structure_path=resolved_structure_path,
                    malf_path=resolved_malf_path,
                    instrument=str(scope_row["code"]),
                    signal_start_date=_to_python_date(scope_row["replay_start_bar_dt"]),
                    signal_end_date=_to_python_date(scope_row["replay_confirm_until_dt"]),
                    limit=normalized_limit,
                    batch_size=normalized_batch_size,
                    source_structure_table=source_structure_table,
                    source_context_table=source_context_table,
                    source_timeframe=normalized_timeframe,
                    filter_contract_version=filter_contract_version,
                    runner_name=runner_name,
                    runner_version=runner_version,
                    filter_path=resolved_filter_path,
                )
                for key in summary_counts:
                    summary_counts[key] += int(getattr(scope_summary, key))
                _upsert_filter_checkpoint(
                    filter_connection,
                    asset_type=str(scope_row["asset_type"]),
                    code=str(scope_row["code"]),
                    timeframe=str(scope_row["timeframe"]),
                    last_completed_bar_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
                    tail_start_bar_dt=_to_python_date(scope_row["replay_start_bar_dt"]),
                    tail_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
                    source_fingerprint=str(scope_row["source_fingerprint"]),
                    last_run_id=materialization_run_id,
                )
                checkpoint_upserted_count += 1
                _mark_filter_queue_completed(
                    filter_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
            except Exception:
                _mark_filter_queue_failed(
                    filter_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
                raise

        summary = FilterSnapshotBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode="checkpoint_queue",
            filter_contract_version=filter_contract_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            claimed_scope_count=len(claimed_scope_rows),
            candidate_structure_count=summary_counts["candidate_structure_count"],
            materialized_snapshot_count=summary_counts["materialized_snapshot_count"],
            inserted_count=summary_counts["inserted_count"],
            reused_count=summary_counts["reused_count"],
            rematerialized_count=summary_counts["rematerialized_count"],
            missing_context_count=summary_counts["missing_context_count"],
            admissible_count=summary_counts["admissible_count"],
            blocked_count=summary_counts["blocked_count"],
            queue_enqueued_count=enqueue_counts["queue_enqueued_count"],
            queue_claimed_count=len(claimed_scope_rows),
            checkpoint_upserted_count=checkpoint_upserted_count,
            filter_ledger_path=str(resolved_filter_path),
            structure_ledger_path=str(resolved_structure_path),
            malf_ledger_path=str(resolved_malf_path),
            source_structure_table=source_structure_table,
            source_context_table=source_context_table,
            source_timeframe=normalized_timeframe,
        )
        _mark_run_completed(filter_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            filter_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        filter_connection.close()


def _load_filter_dirty_scopes(
    *,
    structure_path: Path,
    limit: int,
    timeframe: str,
) -> list[dict[str, object]]:
    connection = duckdb.connect(str(structure_path), read_only=True)
    try:
        rows = connection.execute(
            f"""
            SELECT asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt,
                   tail_confirm_until_dt, source_fingerprint, last_run_id
            FROM {STRUCTURE_CHECKPOINT_TABLE}
            WHERE timeframe = ?
            ORDER BY code
            LIMIT ?
            """,
            [timeframe, limit],
        ).fetchall()
    finally:
        connection.close()
    return [
        {
            "scope_nk": _build_scope_nk(asset_type=str(row[0]), code=str(row[1]), timeframe=str(row[2])),
            "asset_type": str(row[0]),
            "code": str(row[1]),
            "timeframe": str(row[2]),
            "replay_start_bar_dt": _to_python_date(row[4]) or _to_python_date(row[3]),
            "replay_confirm_until_dt": _to_python_date(row[3]),
            "source_fingerprint": json.dumps(
                {
                    "last_completed_bar_dt": None if _to_python_date(row[3]) is None else _to_python_date(row[3]).isoformat(),
                    "tail_start_bar_dt": None if _to_python_date(row[4]) is None else _to_python_date(row[4]).isoformat(),
                    "tail_confirm_until_dt": None if _to_python_date(row[5]) is None else _to_python_date(row[5]).isoformat(),
                    "source_fingerprint": str(row[6]),
                    "last_run_id": None if row[7] is None else str(row[7]),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        }
        for row in rows
        if _to_python_date(row[3]) is not None
    ]


def _enqueue_filter_dirty_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    scope_rows: list[dict[str, object]],
    run_id: str,
) -> dict[str, int]:
    queue_enqueued_count = 0
    for scope_row in scope_rows:
        checkpoint_row = connection.execute(
            f"""
            SELECT last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint
            FROM {FILTER_CHECKPOINT_TABLE}
            WHERE asset_type = ?
              AND code = ?
              AND timeframe = ?
            """,
            [scope_row["asset_type"], scope_row["code"], scope_row["timeframe"]],
        ).fetchone()
        replay_start = _to_python_date(scope_row["replay_start_bar_dt"])
        replay_end = _to_python_date(scope_row["replay_confirm_until_dt"])
        dirty_reason = _derive_filter_dirty_reason(
            checkpoint_row=checkpoint_row,
            replay_start_bar_dt=replay_start,
            replay_confirm_until_dt=replay_end,
            source_fingerprint=str(scope_row["source_fingerprint"]),
        )
        if dirty_reason is None:
            continue
        queue_nk = _build_queue_nk(scope_nk=str(scope_row["scope_nk"]), dirty_reason=dirty_reason)
        existing = connection.execute(
            f"SELECT queue_nk FROM {FILTER_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
            [queue_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {FILTER_WORK_QUEUE_TABLE} (
                    queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
                    replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint,
                    queue_status, first_seen_run_id, last_materialized_run_id, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    queue_nk,
                    scope_row["scope_nk"],
                    scope_row["asset_type"],
                    scope_row["code"],
                    scope_row["timeframe"],
                    dirty_reason,
                    replay_start,
                    replay_end,
                    scope_row["source_fingerprint"],
                    run_id,
                    run_id,
                ],
            )
            queue_enqueued_count += 1
            continue
        connection.execute(
            f"""
            UPDATE {FILTER_WORK_QUEUE_TABLE}
            SET replay_start_bar_dt = ?,
                replay_confirm_until_dt = ?,
                source_fingerprint = ?,
                queue_status = 'pending',
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [replay_start, replay_end, scope_row["source_fingerprint"], queue_nk],
        )
    return {"queue_enqueued_count": queue_enqueued_count}


def _derive_filter_dirty_reason(
    *,
    checkpoint_row: tuple[object, ...] | None,
    replay_start_bar_dt: date | None,
    replay_confirm_until_dt: date | None,
    source_fingerprint: str,
) -> str | None:
    if checkpoint_row is None:
        return "bootstrap_missing_checkpoint"
    last_completed_bar_dt = _to_python_date(checkpoint_row[0])
    tail_start_bar_dt = _to_python_date(checkpoint_row[1])
    tail_confirm_until_dt = _to_python_date(checkpoint_row[2])
    checkpoint_fingerprint = "" if checkpoint_row[3] is None else str(checkpoint_row[3])
    if checkpoint_fingerprint != source_fingerprint:
        return "source_fingerprint_changed"
    if last_completed_bar_dt is None or (
        replay_confirm_until_dt is not None and replay_confirm_until_dt > last_completed_bar_dt
    ):
        return "source_advanced"
    if tail_start_bar_dt is None or (replay_start_bar_dt is not None and replay_start_bar_dt < tail_start_bar_dt):
        return "source_replayed"
    if tail_confirm_until_dt is None or (
        replay_confirm_until_dt is not None and replay_confirm_until_dt > tail_confirm_until_dt
    ):
        return "tail_confirm_advanced"
    return None


def _claim_filter_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    timeframe: str,
) -> list[dict[str, object]]:
    rows = connection.execute(
        f"""
        SELECT queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
               replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint
        FROM {FILTER_WORK_QUEUE_TABLE}
        WHERE timeframe = ?
          AND queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY code, enqueued_at
        """,
        [timeframe],
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {FILTER_WORK_QUEUE_TABLE}
            SET queue_status = 'claimed',
                claimed_at = CURRENT_TIMESTAMP,
                last_claimed_run_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [run_id, str(row[0])],
        )
        claimed_rows.append(
            {
                "queue_nk": str(row[0]),
                "scope_nk": str(row[1]),
                "asset_type": str(row[2]),
                "code": str(row[3]),
                "timeframe": str(row[4]),
                "dirty_reason": str(row[5]),
                "replay_start_bar_dt": _to_python_date(row[6]),
                "replay_confirm_until_dt": _to_python_date(row[7]),
                "source_fingerprint": str(row[8]),
            }
        )
    return claimed_rows


def _mark_filter_queue_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {FILTER_WORK_QUEUE_TABLE}
        SET queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _mark_filter_queue_failed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {FILTER_WORK_QUEUE_TABLE}
        SET queue_status = 'failed',
            last_claimed_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _upsert_filter_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    code: str,
    timeframe: str,
    last_completed_bar_dt: date | None,
    tail_start_bar_dt: date | None,
    tail_confirm_until_dt: date | None,
    source_fingerprint: str,
    last_run_id: str,
) -> None:
    existing = connection.execute(
        f"""
        SELECT asset_type
        FROM {FILTER_CHECKPOINT_TABLE}
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [asset_type, code, timeframe],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {FILTER_CHECKPOINT_TABLE} (
                asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt,
                tail_confirm_until_dt, source_fingerprint, last_run_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                asset_type,
                code,
                timeframe,
                last_completed_bar_dt,
                tail_start_bar_dt,
                tail_confirm_until_dt,
                source_fingerprint,
                last_run_id,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {FILTER_CHECKPOINT_TABLE}
        SET last_completed_bar_dt = ?,
            tail_start_bar_dt = ?,
            tail_confirm_until_dt = ?,
            source_fingerprint = ?,
            last_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [
            last_completed_bar_dt,
            tail_start_bar_dt,
            tail_confirm_until_dt,
            source_fingerprint,
            last_run_id,
            asset_type,
            code,
            timeframe,
        ],
    )


def _materialize_filter_scope(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    structure_path: Path,
    malf_path: Path,
    instrument: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    limit: int,
    batch_size: int,
    source_structure_table: str,
    source_context_table: str,
    source_timeframe: str,
    filter_contract_version: str,
    runner_name: str,
    runner_version: str,
    filter_path: Path,
) -> FilterSnapshotBuildSummary:
    structure_rows = _load_structure_snapshot_rows(
        structure_path=structure_path,
        table_name=source_structure_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=(instrument,),
        limit=limit,
    )
    context_presence = _load_context_presence(
        malf_path=malf_path,
        table_name=source_context_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=tuple(sorted({row.instrument for row in structure_rows})),
        timeframe=source_timeframe,
    )
    return _materialize_filter_rows(
        connection=connection,
        run_id=run_id,
        structure_rows=structure_rows,
        context_presence=context_presence,
        filter_contract_version=filter_contract_version,
        runner_name=runner_name,
        runner_version=runner_version,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        filter_path=filter_path,
        structure_path=structure_path,
        malf_path=malf_path,
        source_structure_table=source_structure_table,
        source_context_table=source_context_table,
        source_timeframe=source_timeframe,
        batch_size=batch_size,
    )
