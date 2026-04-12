"""`malf snapshot` bridge v1 runner 的落表与审计 helper。"""

from __future__ import annotations

import json

import duckdb

from mlq.malf.bootstrap import (
    MALF_RUN_CONTEXT_SNAPSHOT_TABLE,
    MALF_RUN_STRUCTURE_SNAPSHOT_TABLE,
    MALF_RUN_TABLE,
    PAS_CONTEXT_SNAPSHOT_TABLE,
    STRUCTURE_CANDIDATE_SNAPSHOT_TABLE,
)
from mlq.malf.snapshot_shared import MalfSnapshotBuildSummary, _coerce_date


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    signal_start_date,
    signal_end_date,
    bounded_instrument_count: int,
    source_price_table: str,
    adjust_method: str,
    malf_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {MALF_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_price_table,
            adjust_method,
            malf_contract_version
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_price_table,
            adjust_method,
            malf_contract_version,
        ],
    )


def _materialize_snapshot_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    context_rows: list[dict[str, object]],
    structure_rows: list[dict[str, object]],
) -> dict[str, int]:
    counts = {
        "context_inserted_count": 0,
        "context_reused_count": 0,
        "context_rematerialized_count": 0,
        "structure_inserted_count": 0,
        "structure_reused_count": 0,
        "structure_rematerialized_count": 0,
    }
    for row in context_rows:
        action = _upsert_context_snapshot(connection, row=row)
        _record_context_bridge(connection, run_id=run_id, context_nk=str(row["context_nk"]), action=action)
        counts[f"context_{action}_count"] += 1
    for row in structure_rows:
        action = _upsert_structure_candidate_snapshot(connection, row=row)
        _record_structure_bridge(connection, run_id=run_id, candidate_nk=str(row["candidate_nk"]), action=action)
        counts[f"structure_{action}_count"] += 1
    return counts


def _upsert_context_snapshot(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT
            context_nk,
            entity_name,
            source_context_nk,
            malf_context_4,
            lifecycle_rank_high,
            lifecycle_rank_total,
            calc_date,
            adjust_method,
            first_seen_run_id
        FROM {PAS_CONTEXT_SNAPSHOT_TABLE}
        WHERE entity_code = ?
          AND signal_date = ?
          AND asof_date = ?
        """,
        [row["entity_code"], row["signal_date"], row["asof_date"]],
    ).fetchone()
    fingerprint = (
        str(row["context_nk"]),
        str(row["entity_name"]),
        str(row["source_context_nk"]),
        str(row["malf_context_4"]),
        int(row["lifecycle_rank_high"]),
        int(row["lifecycle_rank_total"]),
        row["calc_date"],
        str(row["adjust_method"]),
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {PAS_CONTEXT_SNAPSHOT_TABLE} (
                context_nk,
                entity_code,
                entity_name,
                signal_date,
                asof_date,
                source_context_nk,
                malf_context_4,
                lifecycle_rank_high,
                lifecycle_rank_total,
                calc_date,
                adjust_method,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["context_nk"],
                row["entity_code"],
                row["entity_name"],
                row["signal_date"],
                row["asof_date"],
                row["source_context_nk"],
                row["malf_context_4"],
                row["lifecycle_rank_high"],
                row["lifecycle_rank_total"],
                row["calc_date"],
                row["adjust_method"],
                row["first_seen_run_id"],
                row["last_materialized_run_id"],
            ],
        )
        return "inserted"
    existing_fingerprint = (
        str(existing[0]) if existing[0] is not None else "",
        str(existing[1]) if existing[1] is not None else "",
        str(existing[2]) if existing[2] is not None else "",
        str(existing[3]) if existing[3] is not None else "",
        int(existing[4]) if existing[4] is not None else 0,
        int(existing[5]) if existing[5] is not None else 0,
        _coerce_date(existing[6]),
        str(existing[7]) if existing[7] is not None else "",
    )
    first_seen_run_id = str(existing[8]) if existing[8] is not None else str(row["first_seen_run_id"])
    connection.execute(
        f"""
        UPDATE {PAS_CONTEXT_SNAPSHOT_TABLE}
        SET
            context_nk = ?,
            entity_name = ?,
            source_context_nk = ?,
            malf_context_4 = ?,
            lifecycle_rank_high = ?,
            lifecycle_rank_total = ?,
            calc_date = ?,
            adjust_method = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE entity_code = ?
          AND signal_date = ?
          AND asof_date = ?
        """,
        [
            row["context_nk"],
            row["entity_name"],
            row["source_context_nk"],
            row["malf_context_4"],
            row["lifecycle_rank_high"],
            row["lifecycle_rank_total"],
            row["calc_date"],
            row["adjust_method"],
            first_seen_run_id,
            row["last_materialized_run_id"],
            row["entity_code"],
            row["signal_date"],
            row["asof_date"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _upsert_structure_candidate_snapshot(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT
            candidate_nk,
            instrument_name,
            new_high_count,
            new_low_count,
            refresh_density,
            advancement_density,
            is_failed_extreme,
            failure_type,
            adjust_method,
            first_seen_run_id
        FROM {STRUCTURE_CANDIDATE_SNAPSHOT_TABLE}
        WHERE instrument = ?
          AND signal_date = ?
          AND asof_date = ?
        """,
        [row["instrument"], row["signal_date"], row["asof_date"]],
    ).fetchone()
    fingerprint = (
        str(row["candidate_nk"]),
        str(row["instrument_name"]),
        int(row["new_high_count"]),
        int(row["new_low_count"]),
        float(row["refresh_density"]),
        float(row["advancement_density"]),
        bool(row["is_failed_extreme"]),
        None if row["failure_type"] is None else str(row["failure_type"]),
        str(row["adjust_method"]),
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {STRUCTURE_CANDIDATE_SNAPSHOT_TABLE} (
                candidate_nk,
                instrument,
                instrument_name,
                signal_date,
                asof_date,
                new_high_count,
                new_low_count,
                refresh_density,
                advancement_density,
                is_failed_extreme,
                failure_type,
                adjust_method,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["candidate_nk"],
                row["instrument"],
                row["instrument_name"],
                row["signal_date"],
                row["asof_date"],
                row["new_high_count"],
                row["new_low_count"],
                row["refresh_density"],
                row["advancement_density"],
                row["is_failed_extreme"],
                row["failure_type"],
                row["adjust_method"],
                row["first_seen_run_id"],
                row["last_materialized_run_id"],
            ],
        )
        return "inserted"
    existing_fingerprint = (
        str(existing[0]) if existing[0] is not None else "",
        str(existing[1]) if existing[1] is not None else "",
        int(existing[2]) if existing[2] is not None else 0,
        int(existing[3]) if existing[3] is not None else 0,
        float(existing[4]) if existing[4] is not None else 0.0,
        float(existing[5]) if existing[5] is not None else 0.0,
        bool(existing[6]),
        None if existing[7] is None else str(existing[7]),
        str(existing[8]) if existing[8] is not None else "",
    )
    first_seen_run_id = str(existing[9]) if existing[9] is not None else str(row["first_seen_run_id"])
    connection.execute(
        f"""
        UPDATE {STRUCTURE_CANDIDATE_SNAPSHOT_TABLE}
        SET
            candidate_nk = ?,
            instrument_name = ?,
            new_high_count = ?,
            new_low_count = ?,
            refresh_density = ?,
            advancement_density = ?,
            is_failed_extreme = ?,
            failure_type = ?,
            adjust_method = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE instrument = ?
          AND signal_date = ?
          AND asof_date = ?
        """,
        [
            row["candidate_nk"],
            row["instrument_name"],
            row["new_high_count"],
            row["new_low_count"],
            row["refresh_density"],
            row["advancement_density"],
            row["is_failed_extreme"],
            row["failure_type"],
            row["adjust_method"],
            first_seen_run_id,
            row["last_materialized_run_id"],
            row["instrument"],
            row["signal_date"],
            row["asof_date"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _record_context_bridge(connection: duckdb.DuckDBPyConnection, *, run_id: str, context_nk: str, action: str) -> None:
    existing = connection.execute(
        f"SELECT run_id FROM {MALF_RUN_CONTEXT_SNAPSHOT_TABLE} WHERE run_id = ? AND context_nk = ?",
        [run_id, context_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"INSERT INTO {MALF_RUN_CONTEXT_SNAPSHOT_TABLE} (run_id, context_nk, materialization_action) VALUES (?, ?, ?)",
            [run_id, context_nk, action],
        )
        return
    connection.execute(
        f"""
        UPDATE {MALF_RUN_CONTEXT_SNAPSHOT_TABLE}
        SET materialization_action = ?, recorded_at = CURRENT_TIMESTAMP
        WHERE run_id = ? AND context_nk = ?
        """,
        [action, run_id, context_nk],
    )


def _record_structure_bridge(connection: duckdb.DuckDBPyConnection, *, run_id: str, candidate_nk: str, action: str) -> None:
    existing = connection.execute(
        f"SELECT run_id FROM {MALF_RUN_STRUCTURE_SNAPSHOT_TABLE} WHERE run_id = ? AND candidate_nk = ?",
        [run_id, candidate_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"INSERT INTO {MALF_RUN_STRUCTURE_SNAPSHOT_TABLE} (run_id, candidate_nk, materialization_action) VALUES (?, ?, ?)",
            [run_id, candidate_nk, action],
        )
        return
    connection.execute(
        f"""
        UPDATE {MALF_RUN_STRUCTURE_SNAPSHOT_TABLE}
        SET materialization_action = ?, recorded_at = CURRENT_TIMESTAMP
        WHERE run_id = ? AND candidate_nk = ?
        """,
        [action, run_id, candidate_nk],
    )


def _mark_run_completed(connection: duckdb.DuckDBPyConnection, *, run_id: str, summary: MalfSnapshotBuildSummary) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_RUN_TABLE}
        SET
            run_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True), run_id],
    )


def _mark_run_failed(connection: duckdb.DuckDBPyConnection, *, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_RUN_TABLE}
        SET run_status = 'failed', completed_at = CURRENT_TIMESTAMP
        WHERE run_id = ?
        """,
        [run_id],
    )
