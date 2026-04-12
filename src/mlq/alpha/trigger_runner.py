"""执行 `alpha trigger ledger` 官方 bounded materialization。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from mlq.alpha.bootstrap import (
    ALPHA_TRIGGER_CHECKPOINT_TABLE,
    ALPHA_TRIGGER_WORK_QUEUE_TABLE,
    alpha_ledger_path,
    bootstrap_alpha_trigger_ledger,
)
from mlq.alpha.trigger_materialization import (
    _insert_run_row,
    _mark_run_completed,
    _materialize_trigger_rows,
    _update_run_summary,
    _write_summary,
)
from mlq.alpha.trigger_shared import (
    AlphaTriggerBuildSummary,
    _build_alpha_trigger_run_id,
    _build_queue_nk,
    _build_scope_nk,
    _coerce_date,
    _ensure_database_exists,
    _to_python_date,
)
from mlq.alpha.trigger_sources import (
    _load_official_context_rows,
    _load_trigger_input_rows,
)
from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.filter.bootstrap import FILTER_CHECKPOINT_TABLE


DEFAULT_ALPHA_TRIGGER_INPUT_TABLE: Final[str] = "alpha_trigger_candidate"
DEFAULT_ALPHA_TRIGGER_FILTER_TABLE: Final[str] = "filter_snapshot"
DEFAULT_ALPHA_TRIGGER_STRUCTURE_TABLE: Final[str] = "structure_snapshot"
DEFAULT_ALPHA_TRIGGER_CONTRACT_VERSION: Final[str] = "alpha-trigger-v2"


def run_alpha_trigger_build(
    *,
    settings: WorkspaceRoots | None = None,
    alpha_path: Path | None = None,
    filter_path: Path | None = None,
    structure_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    run_id: str | None = None,
    source_trigger_input_table: str = DEFAULT_ALPHA_TRIGGER_INPUT_TABLE,
    source_filter_table: str = DEFAULT_ALPHA_TRIGGER_FILTER_TABLE,
    source_structure_table: str = DEFAULT_ALPHA_TRIGGER_STRUCTURE_TABLE,
    trigger_contract_version: str = DEFAULT_ALPHA_TRIGGER_CONTRACT_VERSION,
    runner_name: str = "alpha_trigger_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
    use_checkpoint_queue: bool | None = None,
) -> AlphaTriggerBuildSummary:
    """从 bounded detector 输入与官方 `filter/structure snapshot` 物化 trigger ledger。"""

    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    if _should_use_queue_execution(
        use_checkpoint_queue=use_checkpoint_queue,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
    ):
        return _run_alpha_trigger_queue_build(
            settings=settings,
            alpha_path=alpha_path,
            filter_path=filter_path,
            structure_path=structure_path,
            limit=limit,
            batch_size=batch_size,
            run_id=run_id,
            source_trigger_input_table=source_trigger_input_table,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            trigger_contract_version=trigger_contract_version,
            runner_name=runner_name,
            runner_version=runner_version,
            summary_path=summary_path,
        )
    return _run_alpha_trigger_bounded_build(
        settings=settings,
        alpha_path=alpha_path,
        filter_path=filter_path,
        structure_path=structure_path,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        limit=limit,
        batch_size=batch_size,
        run_id=run_id,
        source_trigger_input_table=source_trigger_input_table,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        trigger_contract_version=trigger_contract_version,
        runner_name=runner_name,
        runner_version=runner_version,
        summary_path=summary_path,
    )


def _run_alpha_trigger_bounded_build(
    *,
    settings: WorkspaceRoots | None,
    alpha_path: Path | None,
    filter_path: Path | None,
    structure_path: Path | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    batch_size: int,
    run_id: str | None,
    source_trigger_input_table: str,
    source_filter_table: str,
    source_structure_table: str,
    trigger_contract_version: str,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> AlphaTriggerBuildSummary:
    """执行显式 bounded trigger 物化。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    resolved_filter_path = Path(filter_path or workspace.databases.filter)
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    materialization_run_id = run_id or _build_alpha_trigger_run_id()

    _ensure_database_exists(resolved_filter_path, label="filter")
    _ensure_database_exists(resolved_structure_path, label="structure")

    alpha_connection = duckdb.connect(str(resolved_alpha_path))
    try:
        bootstrap_alpha_trigger_ledger(workspace, connection=alpha_connection)
        input_rows = _load_trigger_input_rows(
            connection=alpha_connection,
            table_name=source_trigger_input_table,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=instruments,
            limit=normalized_limit,
        )
        context_rows = _load_official_context_rows(
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            filter_table_name=source_filter_table,
            structure_table_name=source_structure_table,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=tuple(sorted({row.instrument for row in input_rows})),
        )
        context_map = {
            (row.instrument, row.signal_date, row.asof_date): row
            for row in context_rows
        }

        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            bounded_instrument_count=len({row.instrument for row in input_rows}),
            candidate_trigger_count=len(input_rows),
            source_trigger_input_table=source_trigger_input_table,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            trigger_contract_version=trigger_contract_version,
        )

        summary = _materialize_trigger_rows(
            connection=alpha_connection,
            run_id=materialization_run_id,
            input_rows=input_rows,
            context_map=context_map,
            trigger_contract_version=trigger_contract_version,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            alpha_path=resolved_alpha_path,
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            source_trigger_input_table=source_trigger_input_table,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            batch_size=normalized_batch_size,
        )
        _mark_run_completed(alpha_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            alpha_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        alpha_connection.close()


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


def _run_alpha_trigger_queue_build(
    *,
    settings: WorkspaceRoots | None,
    alpha_path: Path | None,
    filter_path: Path | None,
    structure_path: Path | None,
    limit: int,
    batch_size: int,
    run_id: str | None,
    source_trigger_input_table: str,
    source_filter_table: str,
    source_structure_table: str,
    trigger_contract_version: str,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> AlphaTriggerBuildSummary:
    """执行默认 data-grade queue/checkpoint 续跑。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    resolved_filter_path = Path(filter_path or workspace.databases.filter)
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    materialization_run_id = run_id or _build_alpha_trigger_run_id()

    _ensure_database_exists(resolved_filter_path, label="filter")
    _ensure_database_exists(resolved_structure_path, label="structure")
    alpha_connection = duckdb.connect(str(resolved_alpha_path))
    try:
        bootstrap_alpha_trigger_ledger(workspace, connection=alpha_connection)
        dirty_scopes = _load_alpha_trigger_dirty_scopes(
            connection=alpha_connection,
            filter_path=resolved_filter_path,
            source_trigger_input_table=source_trigger_input_table,
            limit=normalized_limit,
        )
        enqueue_counts = _enqueue_alpha_trigger_dirty_scopes(
            connection=alpha_connection,
            scope_rows=dirty_scopes,
            run_id=materialization_run_id,
        )
        claimed_scope_rows = _claim_alpha_trigger_scopes(
            connection=alpha_connection,
            run_id=materialization_run_id,
        )
        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            candidate_trigger_count=0,
            source_trigger_input_table=source_trigger_input_table,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            trigger_contract_version=trigger_contract_version,
        )

        summary_counts = {
            "candidate_trigger_count": 0,
            "materialized_trigger_count": 0,
            "inserted_count": 0,
            "reused_count": 0,
            "rematerialized_count": 0,
            "missing_context_count": 0,
        }
        checkpoint_upserted_count = 0

        for scope_row in claimed_scope_rows:
            try:
                scope_summary = _materialize_alpha_trigger_scope(
                    connection=alpha_connection,
                    run_id=materialization_run_id,
                    alpha_path=resolved_alpha_path,
                    filter_path=resolved_filter_path,
                    structure_path=resolved_structure_path,
                    instrument=str(scope_row["code"]),
                    signal_start_date=_to_python_date(scope_row["replay_start_bar_dt"]),
                    signal_end_date=_to_python_date(scope_row["replay_confirm_until_dt"]),
                    limit=normalized_limit,
                    batch_size=normalized_batch_size,
                    source_trigger_input_table=source_trigger_input_table,
                    source_filter_table=source_filter_table,
                    source_structure_table=source_structure_table,
                    trigger_contract_version=trigger_contract_version,
                    runner_name=runner_name,
                    runner_version=runner_version,
                )
                for key in summary_counts:
                    summary_counts[key] += int(getattr(scope_summary, key))
                _upsert_alpha_trigger_checkpoint(
                    alpha_connection,
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
                _mark_alpha_trigger_queue_completed(
                    alpha_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
            except Exception:
                _mark_alpha_trigger_queue_failed(
                    alpha_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
                raise

        summary = AlphaTriggerBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode="checkpoint_queue",
            trigger_contract_version=trigger_contract_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            claimed_scope_count=len(claimed_scope_rows),
            candidate_trigger_count=summary_counts["candidate_trigger_count"],
            materialized_trigger_count=summary_counts["materialized_trigger_count"],
            inserted_count=summary_counts["inserted_count"],
            reused_count=summary_counts["reused_count"],
            rematerialized_count=summary_counts["rematerialized_count"],
            missing_context_count=summary_counts["missing_context_count"],
            queue_enqueued_count=enqueue_counts["queue_enqueued_count"],
            queue_claimed_count=len(claimed_scope_rows),
            checkpoint_upserted_count=checkpoint_upserted_count,
            alpha_ledger_path=str(resolved_alpha_path),
            filter_ledger_path=str(resolved_filter_path),
            structure_ledger_path=str(resolved_structure_path),
            source_trigger_input_table=source_trigger_input_table,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
        )
        _mark_run_completed(alpha_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            alpha_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        alpha_connection.close()


def _load_alpha_trigger_dirty_scopes(
    *,
    connection: duckdb.DuckDBPyConnection,
    filter_path: Path,
    source_trigger_input_table: str,
    limit: int,
) -> list[dict[str, object]]:
    filter_connection = duckdb.connect(str(filter_path), read_only=True)
    try:
        candidate_rows = connection.execute(
            f"""
            SELECT instrument, MIN(asof_date), MAX(asof_date), COUNT(*)
            FROM {source_trigger_input_table}
            GROUP BY instrument
            ORDER BY instrument
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        filter_rows = filter_connection.execute(
            f"""
            SELECT code, last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id
            FROM {FILTER_CHECKPOINT_TABLE}
            WHERE timeframe = 'D'
            """
        ).fetchall()
    finally:
        filter_connection.close()
    filter_map = {str(row[0]): row for row in filter_rows}
    scope_rows: list[dict[str, object]] = []
    for instrument, min_asof, max_asof, candidate_count in candidate_rows:
        filter_row = filter_map.get(str(instrument))
        source_payload = {
            "candidate_min_asof_date": None if _to_python_date(min_asof) is None else _to_python_date(min_asof).isoformat(),
            "candidate_max_asof_date": None if _to_python_date(max_asof) is None else _to_python_date(max_asof).isoformat(),
            "candidate_count": int(candidate_count),
            "filter_last_completed_bar_dt": None if filter_row is None or _to_python_date(filter_row[1]) is None else _to_python_date(filter_row[1]).isoformat(),
            "filter_tail_start_bar_dt": None if filter_row is None or _to_python_date(filter_row[2]) is None else _to_python_date(filter_row[2]).isoformat(),
            "filter_tail_confirm_until_dt": None if filter_row is None or _to_python_date(filter_row[3]) is None else _to_python_date(filter_row[3]).isoformat(),
            "filter_source_fingerprint": None if filter_row is None else str(filter_row[4]),
            "filter_last_run_id": None if filter_row is None or filter_row[5] is None else str(filter_row[5]),
        }
        replay_start = min(
            [candidate for candidate in (_to_python_date(min_asof), None if filter_row is None else _to_python_date(filter_row[2])) if candidate is not None],
            default=_to_python_date(max_asof),
        )
        replay_end = _to_python_date(max_asof)
        if replay_end is None:
            continue
        scope_rows.append(
            {
                "scope_nk": _build_scope_nk(asset_type="stock", code=str(instrument), timeframe="D"),
                "asset_type": "stock",
                "code": str(instrument),
                "timeframe": "D",
                "replay_start_bar_dt": replay_start,
                "replay_confirm_until_dt": replay_end,
                "source_fingerprint": json.dumps(source_payload, ensure_ascii=False, sort_keys=True),
            }
        )
    return scope_rows


def _enqueue_alpha_trigger_dirty_scopes(
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
            FROM {ALPHA_TRIGGER_CHECKPOINT_TABLE}
            WHERE asset_type = ?
              AND code = ?
              AND timeframe = ?
            """,
            [scope_row["asset_type"], scope_row["code"], scope_row["timeframe"]],
        ).fetchone()
        dirty_reason = _derive_alpha_trigger_dirty_reason(
            checkpoint_row=checkpoint_row,
            replay_start_bar_dt=_to_python_date(scope_row["replay_start_bar_dt"]),
            replay_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
            source_fingerprint=str(scope_row["source_fingerprint"]),
        )
        if dirty_reason is None:
            continue
        queue_nk = _build_queue_nk(scope_nk=str(scope_row["scope_nk"]), dirty_reason=dirty_reason)
        existing = connection.execute(
            f"SELECT queue_nk FROM {ALPHA_TRIGGER_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
            [queue_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {ALPHA_TRIGGER_WORK_QUEUE_TABLE} (
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
                    _to_python_date(scope_row["replay_start_bar_dt"]),
                    _to_python_date(scope_row["replay_confirm_until_dt"]),
                    scope_row["source_fingerprint"],
                    run_id,
                    run_id,
                ],
            )
            queue_enqueued_count += 1
            continue
        connection.execute(
            f"""
            UPDATE {ALPHA_TRIGGER_WORK_QUEUE_TABLE}
            SET replay_start_bar_dt = ?,
                replay_confirm_until_dt = ?,
                source_fingerprint = ?,
                queue_status = 'pending',
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [
                _to_python_date(scope_row["replay_start_bar_dt"]),
                _to_python_date(scope_row["replay_confirm_until_dt"]),
                scope_row["source_fingerprint"],
                queue_nk,
            ],
        )
    return {"queue_enqueued_count": queue_enqueued_count}


def _derive_alpha_trigger_dirty_reason(
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
    if last_completed_bar_dt is None or (replay_confirm_until_dt is not None and replay_confirm_until_dt > last_completed_bar_dt):
        return "source_advanced"
    if tail_start_bar_dt is None or (replay_start_bar_dt is not None and replay_start_bar_dt < tail_start_bar_dt):
        return "source_replayed"
    if tail_confirm_until_dt is None or (replay_confirm_until_dt is not None and replay_confirm_until_dt > tail_confirm_until_dt):
        return "tail_confirm_advanced"
    return None


def _claim_alpha_trigger_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
) -> list[dict[str, object]]:
    rows = connection.execute(
        f"""
        SELECT queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
               replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint
        FROM {ALPHA_TRIGGER_WORK_QUEUE_TABLE}
        WHERE queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY code, enqueued_at
        """
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {ALPHA_TRIGGER_WORK_QUEUE_TABLE}
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


def _mark_alpha_trigger_queue_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_TRIGGER_WORK_QUEUE_TABLE}
        SET queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _mark_alpha_trigger_queue_failed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nk: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_TRIGGER_WORK_QUEUE_TABLE}
        SET queue_status = 'failed',
            last_claimed_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _upsert_alpha_trigger_checkpoint(
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
        FROM {ALPHA_TRIGGER_CHECKPOINT_TABLE}
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [asset_type, code, timeframe],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {ALPHA_TRIGGER_CHECKPOINT_TABLE} (
                asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt,
                tail_confirm_until_dt, source_fingerprint, last_run_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id],
        )
        return
    connection.execute(
        f"""
        UPDATE {ALPHA_TRIGGER_CHECKPOINT_TABLE}
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
        [last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id, asset_type, code, timeframe],
    )


def _materialize_alpha_trigger_scope(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    alpha_path: Path,
    filter_path: Path,
    structure_path: Path,
    instrument: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    limit: int,
    batch_size: int,
    source_trigger_input_table: str,
    source_filter_table: str,
    source_structure_table: str,
    trigger_contract_version: str,
    runner_name: str,
    runner_version: str,
) -> AlphaTriggerBuildSummary:
    input_rows = _load_trigger_input_rows(
        connection=connection,
        table_name=source_trigger_input_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=(instrument,),
        limit=limit,
    )
    context_rows = _load_official_context_rows(
        filter_path=filter_path,
        structure_path=structure_path,
        filter_table_name=source_filter_table,
        structure_table_name=source_structure_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=tuple(sorted({row.instrument for row in input_rows})),
    )
    context_map = {
        (row.instrument, row.signal_date, row.asof_date): row
        for row in context_rows
    }
    return _materialize_trigger_rows(
        connection=connection,
        run_id=run_id,
        input_rows=input_rows,
        context_map=context_map,
        trigger_contract_version=trigger_contract_version,
        runner_name=runner_name,
        runner_version=runner_version,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        alpha_path=alpha_path,
        filter_path=filter_path,
        structure_path=structure_path,
        source_trigger_input_table=source_trigger_input_table,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        batch_size=batch_size,
    )
