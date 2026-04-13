"""alpha PAS 五触发 detector runner。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from mlq.alpha.bootstrap import (
    ALPHA_PAS_TRIGGER_CHECKPOINT_TABLE,
    ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE,
    alpha_ledger_path,
    bootstrap_alpha_pas_trigger_ledger,
)
from mlq.alpha.pas_materialization import (
    _insert_run_row,
    _mark_run_completed,
    _materialize_pas_detector_rows,
    _update_run_summary,
)
from mlq.alpha.pas_shared import (
    DEFAULT_ALPHA_PAS_TRIGGER_ADJUST_METHOD,
    DEFAULT_ALPHA_PAS_TRIGGER_CONTRACT_VERSION,
    DEFAULT_ALPHA_PAS_TRIGGER_FILTER_TABLE,
    DEFAULT_ALPHA_PAS_TRIGGER_PRICE_TABLE,
    DEFAULT_ALPHA_PAS_TRIGGER_STRUCTURE_TABLE,
    DEFAULT_ALPHA_PAS_TRIGGER_TIMEFRAME,
    AlphaPasTriggerBuildSummary,
    _build_alpha_pas_trigger_run_id,
    _build_queue_nk,
    _build_scope_nk,
    _coerce_date,
    _ensure_database_exists,
    _price_history_window_start,
    _to_python_date,
    _write_summary,
)
from mlq.alpha.pas_source import _load_detector_scope_rows, _load_price_history
from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.filter.bootstrap import FILTER_CHECKPOINT_TABLE


DEFAULT_ALPHA_PAS_RUNNER_NAME: Final[str] = "alpha_pas_five_trigger_builder"
DEFAULT_ALPHA_PAS_RUNNER_VERSION: Final[str] = "v1"


def run_alpha_pas_five_trigger_build(
    *,
    settings: WorkspaceRoots | None = None,
    alpha_path: Path | None = None,
    filter_path: Path | None = None,
    structure_path: Path | None = None,
    market_base_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    source_filter_table: str = DEFAULT_ALPHA_PAS_TRIGGER_FILTER_TABLE,
    source_structure_table: str = DEFAULT_ALPHA_PAS_TRIGGER_STRUCTURE_TABLE,
    source_price_table: str = DEFAULT_ALPHA_PAS_TRIGGER_PRICE_TABLE,
    source_adjust_method: str = DEFAULT_ALPHA_PAS_TRIGGER_ADJUST_METHOD,
    source_timeframe: str = DEFAULT_ALPHA_PAS_TRIGGER_TIMEFRAME,
    detector_contract_version: str = DEFAULT_ALPHA_PAS_TRIGGER_CONTRACT_VERSION,
    runner_name: str = DEFAULT_ALPHA_PAS_RUNNER_NAME,
    runner_version: str = DEFAULT_ALPHA_PAS_RUNNER_VERSION,
    summary_path: Path | None = None,
    use_checkpoint_queue: bool | None = None,
) -> AlphaPasTriggerBuildSummary:
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_limit = max(int(limit), 1)
    _validate_mainline_contract(
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        source_adjust_method=source_adjust_method,
        source_timeframe=source_timeframe,
    )
    if _should_use_queue_execution(
        use_checkpoint_queue=use_checkpoint_queue,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
    ):
        return _run_queue_build(
            settings=settings,
            alpha_path=alpha_path,
            filter_path=filter_path,
            structure_path=structure_path,
            market_base_path=market_base_path,
            limit=normalized_limit,
            run_id=run_id,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            source_price_table=source_price_table,
            source_adjust_method=source_adjust_method,
            detector_contract_version=detector_contract_version,
            runner_name=runner_name,
            runner_version=runner_version,
            summary_path=summary_path,
        )
    return _run_bounded_build(
        settings=settings,
        alpha_path=alpha_path,
        filter_path=filter_path,
        structure_path=structure_path,
        market_base_path=market_base_path,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        limit=normalized_limit,
        run_id=run_id,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        source_price_table=source_price_table,
        source_adjust_method=source_adjust_method,
        detector_contract_version=detector_contract_version,
        runner_name=runner_name,
        runner_version=runner_version,
        summary_path=summary_path,
    )


def _validate_mainline_contract(
    *,
    source_filter_table: str,
    source_structure_table: str,
    source_adjust_method: str,
    source_timeframe: str,
) -> None:
    if source_filter_table != DEFAULT_ALPHA_PAS_TRIGGER_FILTER_TABLE:
        raise ValueError("alpha PAS mainline only accepts official `filter_snapshot` as source_filter_table.")
    if source_structure_table != DEFAULT_ALPHA_PAS_TRIGGER_STRUCTURE_TABLE:
        raise ValueError("alpha PAS mainline only accepts official `structure_snapshot` as source_structure_table.")
    if source_adjust_method != DEFAULT_ALPHA_PAS_TRIGGER_ADJUST_METHOD:
        raise ValueError("alpha PAS mainline only accepts canonical `adjust_method='backward'`.")
    if source_timeframe != DEFAULT_ALPHA_PAS_TRIGGER_TIMEFRAME:
        raise ValueError("alpha PAS mainline only accepts canonical timeframe `D`.")


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


def _run_bounded_build(
    *,
    settings: WorkspaceRoots | None,
    alpha_path: Path | None,
    filter_path: Path | None,
    structure_path: Path | None,
    market_base_path: Path | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    run_id: str | None,
    source_filter_table: str,
    source_structure_table: str,
    source_price_table: str,
    source_adjust_method: str,
    detector_contract_version: str,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> AlphaPasTriggerBuildSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    resolved_filter_path = Path(filter_path or workspace.databases.filter)
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    resolved_market_base_path = Path(market_base_path or workspace.databases.market_base)
    materialization_run_id = run_id or _build_alpha_pas_trigger_run_id()

    _ensure_database_exists(resolved_filter_path, label="filter")
    _ensure_database_exists(resolved_structure_path, label="structure")
    _ensure_database_exists(resolved_market_base_path, label="market_base")

    alpha_connection = duckdb.connect(str(resolved_alpha_path))
    try:
        bootstrap_alpha_pas_trigger_ledger(workspace, connection=alpha_connection)
        scope_rows = _load_detector_scope_rows(
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            filter_table_name=source_filter_table,
            structure_table_name=source_structure_table,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=instruments,
            limit=limit,
        )
        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            bounded_instrument_count=len({row.instrument for row in scope_rows}),
            candidate_scope_count=len(scope_rows),
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            source_price_table=source_price_table,
            source_adjust_method=source_adjust_method,
            detector_contract_version=detector_contract_version,
        )
        summary = _materialize_scope_window(
            alpha_path=resolved_alpha_path,
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            market_base_path=resolved_market_base_path,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            source_price_table=source_price_table,
            source_adjust_method=source_adjust_method,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=instruments,
            limit=limit,
            connection=alpha_connection,
            run_id=materialization_run_id,
            detector_contract_version=detector_contract_version,
            runner_name=runner_name,
            runner_version=runner_version,
        )
        _mark_run_completed(alpha_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            alpha_connection,
            run_id=materialization_run_id,
            run_status="failed",
            materialized_candidate_count=0,
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        alpha_connection.close()


def _run_queue_build(
    *,
    settings: WorkspaceRoots | None,
    alpha_path: Path | None,
    filter_path: Path | None,
    structure_path: Path | None,
    market_base_path: Path | None,
    limit: int,
    run_id: str | None,
    source_filter_table: str,
    source_structure_table: str,
    source_price_table: str,
    source_adjust_method: str,
    detector_contract_version: str,
    runner_name: str,
    runner_version: str,
    summary_path: Path | None,
) -> AlphaPasTriggerBuildSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    resolved_filter_path = Path(filter_path or workspace.databases.filter)
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    resolved_market_base_path = Path(market_base_path or workspace.databases.market_base)
    materialization_run_id = run_id or _build_alpha_pas_trigger_run_id()

    _ensure_database_exists(resolved_filter_path, label="filter")
    _ensure_database_exists(resolved_structure_path, label="structure")
    _ensure_database_exists(resolved_market_base_path, label="market_base")

    alpha_connection = duckdb.connect(str(resolved_alpha_path))
    try:
        bootstrap_alpha_pas_trigger_ledger(workspace, connection=alpha_connection)
        dirty_scopes = _load_dirty_scopes(filter_path=resolved_filter_path, limit=limit)
        enqueue_counts = _enqueue_dirty_scopes(connection=alpha_connection, scope_rows=dirty_scopes, run_id=materialization_run_id)
        claimed_scope_rows = _claim_scopes(connection=alpha_connection, run_id=materialization_run_id)
        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            candidate_scope_count=0,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            source_price_table=source_price_table,
            source_adjust_method=source_adjust_method,
            detector_contract_version=detector_contract_version,
        )
        summary_counts = {
            "evaluated_snapshot_count": 0,
            "materialized_candidate_count": 0,
            "skipped_pattern_count": 0,
            "inserted_count": 0,
            "reused_count": 0,
            "rematerialized_count": 0,
        }
        family_counts = {"bof": 0, "tst": 0, "pb": 0, "cpb": 0, "bpb": 0}
        checkpoint_upserted_count = 0
        for scope_row in claimed_scope_rows:
            try:
                scope_summary = _materialize_scope_window(
                    alpha_path=resolved_alpha_path,
                    filter_path=resolved_filter_path,
                    structure_path=resolved_structure_path,
                    market_base_path=resolved_market_base_path,
                    source_filter_table=source_filter_table,
                    source_structure_table=source_structure_table,
                    source_price_table=source_price_table,
                    source_adjust_method=source_adjust_method,
                    signal_start_date=_to_python_date(scope_row["replay_start_bar_dt"]),
                    signal_end_date=_to_python_date(scope_row["replay_confirm_until_dt"]),
                    instruments=(str(scope_row["code"]),),
                    limit=limit,
                    connection=alpha_connection,
                    run_id=materialization_run_id,
                    detector_contract_version=detector_contract_version,
                    runner_name=runner_name,
                    runner_version=runner_version,
                )
                for key in summary_counts:
                    summary_counts[key] += int(getattr(scope_summary, key))
                for trigger_type, count in scope_summary.family_counts.items():
                    family_counts[trigger_type] = family_counts.get(trigger_type, 0) + int(count)
                _upsert_checkpoint(
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
                _mark_queue_completed(alpha_connection, queue_nk=str(scope_row["queue_nk"]), run_id=materialization_run_id)
            except Exception:
                _mark_queue_failed(alpha_connection, queue_nk=str(scope_row["queue_nk"]), run_id=materialization_run_id)
                raise
        summary = AlphaPasTriggerBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode="checkpoint_queue",
            detector_contract_version=detector_contract_version,
            signal_start_date=None,
            signal_end_date=None,
            bounded_instrument_count=len({str(row["code"]) for row in claimed_scope_rows}),
            claimed_scope_count=len(claimed_scope_rows),
            evaluated_snapshot_count=summary_counts["evaluated_snapshot_count"],
            materialized_candidate_count=summary_counts["materialized_candidate_count"],
            skipped_pattern_count=summary_counts["skipped_pattern_count"],
            inserted_count=summary_counts["inserted_count"],
            reused_count=summary_counts["reused_count"],
            rematerialized_count=summary_counts["rematerialized_count"],
            queue_enqueued_count=enqueue_counts["queue_enqueued_count"],
            queue_claimed_count=len(claimed_scope_rows),
            checkpoint_upserted_count=checkpoint_upserted_count,
            family_counts=family_counts,
            alpha_ledger_path=str(resolved_alpha_path),
            filter_ledger_path=str(resolved_filter_path),
            structure_ledger_path=str(resolved_structure_path),
            market_base_ledger_path=str(resolved_market_base_path),
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            source_price_table=source_price_table,
            source_adjust_method=source_adjust_method,
        )
        _mark_run_completed(alpha_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            alpha_connection,
            run_id=materialization_run_id,
            run_status="failed",
            materialized_candidate_count=0,
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        alpha_connection.close()


def _load_dirty_scopes(*, filter_path: Path, limit: int) -> list[dict[str, object]]:
    connection = duckdb.connect(str(filter_path), read_only=True)
    try:
        rows = connection.execute(
            f"""
            SELECT asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt,
                   tail_confirm_until_dt, source_fingerprint, last_run_id
            FROM {FILTER_CHECKPOINT_TABLE}
            WHERE timeframe = 'D'
            ORDER BY code
            LIMIT ?
            """,
            [limit],
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


def _enqueue_dirty_scopes(
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
            FROM {ALPHA_PAS_TRIGGER_CHECKPOINT_TABLE}
            WHERE asset_type = ? AND code = ? AND timeframe = ?
            """,
            [scope_row["asset_type"], scope_row["code"], scope_row["timeframe"]],
        ).fetchone()
        dirty_reason = _derive_dirty_reason(
            checkpoint_row=checkpoint_row,
            replay_start_bar_dt=_to_python_date(scope_row["replay_start_bar_dt"]),
            replay_confirm_until_dt=_to_python_date(scope_row["replay_confirm_until_dt"]),
            source_fingerprint=str(scope_row["source_fingerprint"]),
        )
        if dirty_reason is None:
            continue
        queue_nk = _build_queue_nk(scope_nk=str(scope_row["scope_nk"]), dirty_reason=dirty_reason)
        existing = connection.execute(
            f"SELECT queue_nk FROM {ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
            [queue_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE} (
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
            UPDATE {ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE}
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


def _derive_dirty_reason(
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


def _claim_scopes(connection: duckdb.DuckDBPyConnection, *, run_id: str) -> list[dict[str, object]]:
    rows = connection.execute(
        f"""
        SELECT queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
               replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint
        FROM {ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE}
        WHERE queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY code, enqueued_at
        """
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE}
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


def _mark_queue_completed(connection: duckdb.DuckDBPyConnection, *, queue_nk: str, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE}
        SET queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _mark_queue_failed(connection: duckdb.DuckDBPyConnection, *, queue_nk: str, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_PAS_TRIGGER_WORK_QUEUE_TABLE}
        SET queue_status = 'failed',
            last_claimed_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _upsert_checkpoint(
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
        SELECT asset_type FROM {ALPHA_PAS_TRIGGER_CHECKPOINT_TABLE}
        WHERE asset_type = ? AND code = ? AND timeframe = ?
        """,
        [asset_type, code, timeframe],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {ALPHA_PAS_TRIGGER_CHECKPOINT_TABLE} (
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
        UPDATE {ALPHA_PAS_TRIGGER_CHECKPOINT_TABLE}
        SET last_completed_bar_dt = ?,
            tail_start_bar_dt = ?,
            tail_confirm_until_dt = ?,
            source_fingerprint = ?,
            last_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE asset_type = ? AND code = ? AND timeframe = ?
        """,
        [last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id, asset_type, code, timeframe],
    )


def _materialize_scope_window(
    *,
    alpha_path: Path,
    filter_path: Path,
    structure_path: Path,
    market_base_path: Path,
    source_filter_table: str,
    source_structure_table: str,
    source_price_table: str,
    source_adjust_method: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    detector_contract_version: str,
    runner_name: str,
    runner_version: str,
) -> AlphaPasTriggerBuildSummary:
    scope_rows = _load_detector_scope_rows(
        filter_path=filter_path,
        structure_path=structure_path,
        filter_table_name=source_filter_table,
        structure_table_name=source_structure_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=instruments,
        limit=limit,
    )
    price_history = _load_price_history(
        market_base_path=market_base_path,
        table_name=source_price_table,
        adjust_method=source_adjust_method,
        history_start_date=_price_history_window_start(signal_start_date),
        signal_end_date=signal_end_date,
        instruments=tuple(sorted({row.instrument for row in scope_rows})),
    )
    return _materialize_pas_detector_rows(
        connection=connection,
        run_id=run_id,
        scope_rows=scope_rows,
        price_history=price_history,
        detector_contract_version=detector_contract_version,
        runner_name=runner_name,
        runner_version=runner_version,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        alpha_path=alpha_path,
        filter_path=filter_path,
        structure_path=structure_path,
        market_base_path=market_base_path,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        source_price_table=source_price_table,
        source_adjust_method=source_adjust_method,
    )
