"""主线本地正式库增量同步、断点续跑与 freshness audit。"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.data_mainline_standardization import (
    MAINLINE_LEDGER_SPEC_BY_NAME,
    _load_table_row_counts,
    _normalize_selected_ledgers,
    _normalize_source_map,
    _path_size_bytes,
)


MAINLINE_LOCAL_LEDGER_SYNC_RUN_TABLE = "mainline_local_ledger_sync_run"
MAINLINE_LOCAL_LEDGER_SYNC_CHECKPOINT_TABLE = "mainline_local_ledger_sync_checkpoint"
MAINLINE_LOCAL_LEDGER_SYNC_DIRTY_QUEUE_TABLE = "mainline_local_ledger_sync_dirty_queue"
MAINLINE_LOCAL_LEDGER_FRESHNESS_READOUT_TABLE = "mainline_local_ledger_freshness_readout"
MAINLINE_LOCAL_LEDGER_SYNC_CONTROL_FILENAME = "mainline_local_ledger_sync.duckdb"

MAINLINE_LEDGER_LATEST_BAR_DATE_CANDIDATES: dict[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "raw_market": (
        ("stock_daily_bar", ("trade_date",)),
        ("index_daily_bar", ("trade_date",)),
        ("block_daily_bar", ("trade_date",)),
    ),
    "market_base": (
        ("stock_daily_adjusted", ("trade_date",)),
        ("index_daily_adjusted", ("trade_date",)),
        ("block_daily_adjusted", ("trade_date",)),
    ),
    "malf": (
        ("malf_state_snapshot", ("asof_bar_dt",)),
        ("malf_wave_life_snapshot", ("asof_bar_dt",)),
        ("malf_wave_ledger", ("end_bar_dt", "start_bar_dt")),
    ),
    "structure": (
        ("structure_snapshot", ("signal_date", "asof_date")),
        ("structure_run", ("signal_end_date", "signal_start_date")),
    ),
    "filter": (
        ("filter_snapshot", ("signal_date", "asof_date")),
        ("filter_run", ("signal_end_date", "signal_start_date")),
    ),
    "alpha": (
        ("alpha_formal_signal_event", ("signal_date", "asof_date")),
        ("alpha_family_event", ("signal_date", "asof_date")),
        ("alpha_trigger_event", ("signal_date", "asof_date")),
    ),
    "position": (
        ("position_candidate_audit", ("reference_trade_date",)),
        ("position_sizing_snapshot", ("reference_trade_date",)),
    ),
    "portfolio_plan": (("portfolio_plan_snapshot", ("reference_trade_date",)),),
    "trade_runtime": (
        ("trade_execution_plan", ("planned_entry_trade_date", "signal_date")),
        ("trade_position_leg", ("entry_trade_date",)),
        ("trade_carry_snapshot", ("snapshot_date",)),
    ),
    "system": (
        ("system_mainline_snapshot", ("snapshot_date",)),
        ("system_run", ("snapshot_date",)),
    ),
}

CONTROL_DDL: dict[str, str] = {
    MAINLINE_LOCAL_LEDGER_SYNC_RUN_TABLE: """
        CREATE TABLE IF NOT EXISTS mainline_local_ledger_sync_run (
            run_id TEXT PRIMARY KEY,
            runner_name TEXT NOT NULL,
            runner_version TEXT NOT NULL,
            run_status TEXT NOT NULL,
            selected_ledger_count BIGINT NOT NULL DEFAULT 0,
            queue_enqueued_count BIGINT NOT NULL DEFAULT 0,
            queue_claimed_count BIGINT NOT NULL DEFAULT 0,
            synced_ledger_count BIGINT NOT NULL DEFAULT 0,
            missing_source_count BIGINT NOT NULL DEFAULT 0,
            replay_requested_count BIGINT NOT NULL DEFAULT 0,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            summary_json TEXT
        )
    """,
    MAINLINE_LOCAL_LEDGER_SYNC_CHECKPOINT_TABLE: """
        CREATE TABLE IF NOT EXISTS mainline_local_ledger_sync_checkpoint (
            ledger_name TEXT PRIMARY KEY,
            module_name TEXT NOT NULL,
            source_path TEXT NOT NULL,
            target_path TEXT NOT NULL,
            last_completed_bar_dt DATE,
            tail_start_bar_dt DATE,
            tail_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MAINLINE_LOCAL_LEDGER_SYNC_DIRTY_QUEUE_TABLE: """
        CREATE TABLE IF NOT EXISTS mainline_local_ledger_sync_dirty_queue (
            queue_nk TEXT PRIMARY KEY,
            scope_nk TEXT NOT NULL,
            ledger_name TEXT NOT NULL,
            module_name TEXT NOT NULL,
            source_path TEXT NOT NULL,
            target_path TEXT NOT NULL,
            dirty_reason TEXT NOT NULL,
            source_latest_bar_dt DATE,
            replay_start_bar_dt DATE,
            replay_confirm_until_dt DATE,
            source_fingerprint TEXT NOT NULL,
            queue_status TEXT NOT NULL,
            enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            claimed_at TIMESTAMP,
            completed_at TIMESTAMP,
            first_seen_run_id TEXT,
            last_claimed_run_id TEXT,
            last_materialized_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MAINLINE_LOCAL_LEDGER_FRESHNESS_READOUT_TABLE: """
        CREATE TABLE IF NOT EXISTS mainline_local_ledger_freshness_readout (
            ledger_name TEXT PRIMARY KEY,
            module_name TEXT NOT NULL,
            source_path TEXT NOT NULL,
            target_path TEXT NOT NULL,
            source_exists BOOLEAN NOT NULL,
            target_exists BOOLEAN NOT NULL,
            source_latest_bar_dt DATE,
            last_completed_bar_dt DATE,
            freshness_lag_days BIGINT,
            freshness_status TEXT NOT NULL,
            last_dirty_reason TEXT,
            source_fingerprint TEXT NOT NULL,
            last_run_id TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
}


@dataclass(frozen=True)
class MainlineLocalLedgerSyncResult:
    ledger_name: str
    module_name: str
    dirty_reason: str
    sync_action: str
    source_path: Path
    target_path: Path
    source_latest_bar_dt: date | None
    last_completed_bar_dt: date | None
    tail_start_bar_dt: date | None
    tail_confirm_until_dt: date | None
    source_fingerprint: str
    table_count_after: int
    total_row_count_after: int

    def as_dict(self) -> dict[str, object]:
        return {
            "ledger_name": self.ledger_name,
            "module_name": self.module_name,
            "dirty_reason": self.dirty_reason,
            "sync_action": self.sync_action,
            "source_path": str(self.source_path),
            "target_path": str(self.target_path),
            "source_latest_bar_dt": _dump_date(self.source_latest_bar_dt),
            "last_completed_bar_dt": _dump_date(self.last_completed_bar_dt),
            "tail_start_bar_dt": _dump_date(self.tail_start_bar_dt),
            "tail_confirm_until_dt": _dump_date(self.tail_confirm_until_dt),
            "source_fingerprint": self.source_fingerprint,
            "table_count_after": self.table_count_after,
            "total_row_count_after": self.total_row_count_after,
        }


@dataclass(frozen=True)
class MainlineLocalLedgerFreshnessRow:
    ledger_name: str
    module_name: str
    source_path: Path
    target_path: Path
    source_exists: bool
    target_exists: bool
    source_latest_bar_dt: date | None
    last_completed_bar_dt: date | None
    freshness_lag_days: int | None
    freshness_status: str
    last_dirty_reason: str | None
    source_fingerprint: str

    def as_dict(self) -> dict[str, object]:
        return {
            "ledger_name": self.ledger_name,
            "module_name": self.module_name,
            "source_path": str(self.source_path),
            "target_path": str(self.target_path),
            "source_exists": self.source_exists,
            "target_exists": self.target_exists,
            "source_latest_bar_dt": _dump_date(self.source_latest_bar_dt),
            "last_completed_bar_dt": _dump_date(self.last_completed_bar_dt),
            "freshness_lag_days": self.freshness_lag_days,
            "freshness_status": self.freshness_status,
            "last_dirty_reason": self.last_dirty_reason,
            "source_fingerprint": self.source_fingerprint,
        }


@dataclass(frozen=True)
class MainlineLocalLedgerIncrementalSyncSummary:
    run_id: str
    selected_ledger_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    synced_ledger_count: int
    copied_from_source_count: int
    observed_in_place_count: int
    bootstrapped_empty_target_count: int
    checkpoint_upserted_count: int
    freshness_upserted_count: int
    missing_source_count: int
    replay_requested_count: int
    fresh_count: int
    stale_count: int
    unknown_freshness_count: int
    missing_target_count: int
    sync_results: tuple[MainlineLocalLedgerSyncResult, ...]
    freshness_rows: tuple[MainlineLocalLedgerFreshnessRow, ...]
    report_json_path: Path
    report_markdown_path: Path

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "selected_ledger_count": self.selected_ledger_count,
            "queue_enqueued_count": self.queue_enqueued_count,
            "queue_claimed_count": self.queue_claimed_count,
            "synced_ledger_count": self.synced_ledger_count,
            "copied_from_source_count": self.copied_from_source_count,
            "observed_in_place_count": self.observed_in_place_count,
            "bootstrapped_empty_target_count": self.bootstrapped_empty_target_count,
            "checkpoint_upserted_count": self.checkpoint_upserted_count,
            "freshness_upserted_count": self.freshness_upserted_count,
            "missing_source_count": self.missing_source_count,
            "replay_requested_count": self.replay_requested_count,
            "fresh_count": self.fresh_count,
            "stale_count": self.stale_count,
            "unknown_freshness_count": self.unknown_freshness_count,
            "missing_target_count": self.missing_target_count,
            "sync_results": [row.as_dict() for row in self.sync_results],
            "freshness_rows": [row.as_dict() for row in self.freshness_rows],
            "report_json_path": str(self.report_json_path),
            "report_markdown_path": str(self.report_markdown_path),
        }


def mainline_local_ledger_sync_control_path(settings: WorkspaceRoots | None = None) -> Path:
    workspace = settings or default_settings()
    return (workspace.data_root / "data" / MAINLINE_LOCAL_LEDGER_SYNC_CONTROL_FILENAME).resolve()


def connect_mainline_local_ledger_sync_control(
    settings: WorkspaceRoots | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    workspace = settings or default_settings()
    if not read_only:
        workspace.ensure_directories()
        mainline_local_ledger_sync_control_path(workspace).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(mainline_local_ledger_sync_control_path(workspace)), read_only=read_only)


def bootstrap_mainline_local_ledger_sync_control(
    settings: WorkspaceRoots | None = None,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[str, ...]:
    workspace = settings or default_settings()
    owns_connection = connection is None
    conn = connection or connect_mainline_local_ledger_sync_control(workspace)
    try:
        for ddl in CONTROL_DDL.values():
            conn.execute(ddl)
        return tuple(CONTROL_DDL.keys())
    finally:
        if owns_connection:
            conn.close()


def run_mainline_local_ledger_incremental_sync(
    *,
    settings: WorkspaceRoots | None = None,
    ledgers: list[str] | tuple[str, ...] | None = None,
    source_ledger_paths: dict[str, str | Path] | None = None,
    source_latest_bar_dates: dict[str, str | date] | None = None,
    replay_start_dates: dict[str, str | date] | None = None,
    replay_confirm_until_dates: dict[str, str | date] | None = None,
    run_id: str | None = None,
    summary_path: Path | None = None,
    runner_name: str = "mainline_local_ledger_incremental_sync",
    runner_version: str = "v1",
) -> MainlineLocalLedgerIncrementalSyncSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    selected_ledgers = _normalize_selected_ledgers(ledgers)
    source_map = _normalize_source_map(source_ledger_paths)
    source_latest_map = _normalize_date_map(source_latest_bar_dates)
    replay_start_map = _normalize_date_map(replay_start_dates)
    replay_confirm_map = _normalize_date_map(replay_confirm_until_dates)
    effective_run_id = run_id or f"mainline-local-ledger-sync-{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
    replay_requested_count = sum(
        1
        for ledger_name in selected_ledgers
        if ledger_name in replay_start_map or ledger_name in replay_confirm_map
    )

    connection = connect_mainline_local_ledger_sync_control(workspace)
    try:
        bootstrap_mainline_local_ledger_sync_control(workspace, connection=connection)
        connection.execute(
            f"""
            INSERT INTO {MAINLINE_LOCAL_LEDGER_SYNC_RUN_TABLE} (
                run_id, runner_name, runner_version, run_status, selected_ledger_count, replay_requested_count
            )
            VALUES (?, ?, ?, 'running', ?, ?)
            """,
            [effective_run_id, runner_name, runner_version, len(selected_ledgers), replay_requested_count],
        )
        scope_rows = _collect_scope_rows(
            connection=connection,
            settings=workspace,
            selected_ledgers=selected_ledgers,
            source_map=source_map,
            source_latest_map=source_latest_map,
            replay_start_map=replay_start_map,
            replay_confirm_map=replay_confirm_map,
        )
        queue_enqueued_count = _enqueue_dirty_scopes(connection=connection, scope_rows=scope_rows, run_id=effective_run_id)
        claimed_scope_rows = _claim_scopes(connection=connection, selected_ledgers=selected_ledgers, run_id=effective_run_id)
        sync_results: list[MainlineLocalLedgerSyncResult] = []
        copied_from_source_count = 0
        observed_in_place_count = 0
        bootstrapped_empty_target_count = 0
        checkpoint_upserted_count = 0
        for scope_row in claimed_scope_rows:
            try:
                spec = MAINLINE_LEDGER_SPEC_BY_NAME[str(scope_row["ledger_name"])]
                action = _sync_one_ledger(
                    spec=spec,
                    settings=workspace,
                    source_path=Path(str(scope_row["source_path"])),
                    target_path=Path(str(scope_row["target_path"])),
                )
                state = _resolve_source_state(
                    ledger_name=str(scope_row["ledger_name"]),
                    source_path=Path(str(scope_row["source_path"])),
                    explicit_latest_bar_dt=source_latest_map.get(str(scope_row["ledger_name"])),
                )
                last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt = _resolve_tail_dates(
                    source_latest_bar_dt=state["source_latest_bar_dt"],
                    replay_start_bar_dt=_to_date(scope_row["replay_start_bar_dt"]),
                    replay_confirm_until_dt=_to_date(scope_row["replay_confirm_until_dt"]),
                )
                _upsert_checkpoint(
                    connection=connection,
                    ledger_name=str(scope_row["ledger_name"]),
                    module_name=str(scope_row["module_name"]),
                    source_path=Path(str(scope_row["source_path"])),
                    target_path=Path(str(scope_row["target_path"])),
                    last_completed_bar_dt=last_completed_bar_dt,
                    tail_start_bar_dt=tail_start_bar_dt,
                    tail_confirm_until_dt=tail_confirm_until_dt,
                    source_fingerprint=str(state["source_fingerprint"]),
                    last_run_id=effective_run_id,
                )
                connection.execute(
                    f"""
                    UPDATE {MAINLINE_LOCAL_LEDGER_SYNC_DIRTY_QUEUE_TABLE}
                    SET queue_status = 'completed',
                        completed_at = CURRENT_TIMESTAMP,
                        last_materialized_run_id = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE queue_nk = ?
                    """,
                    [effective_run_id, str(scope_row["queue_nk"])],
                )
                row_counts = _load_table_row_counts(Path(str(scope_row["target_path"])))
                sync_results.append(
                    MainlineLocalLedgerSyncResult(
                        ledger_name=str(scope_row["ledger_name"]),
                        module_name=str(scope_row["module_name"]),
                        dirty_reason=str(scope_row["dirty_reason"]),
                        sync_action=action,
                        source_path=Path(str(scope_row["source_path"])),
                        target_path=Path(str(scope_row["target_path"])),
                        source_latest_bar_dt=state["source_latest_bar_dt"],
                        last_completed_bar_dt=last_completed_bar_dt,
                        tail_start_bar_dt=tail_start_bar_dt,
                        tail_confirm_until_dt=tail_confirm_until_dt,
                        source_fingerprint=str(state["source_fingerprint"]),
                        table_count_after=len(row_counts),
                        total_row_count_after=sum(row_counts.values()),
                    )
                )
                checkpoint_upserted_count += 1
                if action == "copied_from_source":
                    copied_from_source_count += 1
                elif action == "observed_in_place":
                    observed_in_place_count += 1
                else:
                    bootstrapped_empty_target_count += 1
            except Exception:
                connection.execute(
                    f"""
                    UPDATE {MAINLINE_LOCAL_LEDGER_SYNC_DIRTY_QUEUE_TABLE}
                    SET queue_status = 'failed',
                        last_claimed_run_id = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE queue_nk = ?
                    """,
                    [effective_run_id, str(scope_row["queue_nk"])],
                )
                raise

        freshness_rows = _write_freshness_readout(
            connection=connection,
            settings=workspace,
            selected_ledgers=selected_ledgers,
            source_map=source_map,
            source_latest_map=source_latest_map,
            dirty_reason_map={str(row["ledger_name"]): row["dirty_reason"] for row in scope_rows},
            run_id=effective_run_id,
        )
        report_json_path, report_markdown_path = _write_reports(
            settings=workspace,
            run_id=effective_run_id,
            sync_results=sync_results,
            freshness_rows=freshness_rows,
            summary_path=summary_path,
        )
        summary = MainlineLocalLedgerIncrementalSyncSummary(
            run_id=effective_run_id,
            selected_ledger_count=len(selected_ledgers),
            queue_enqueued_count=queue_enqueued_count,
            queue_claimed_count=len(claimed_scope_rows),
            synced_ledger_count=len(sync_results),
            copied_from_source_count=copied_from_source_count,
            observed_in_place_count=observed_in_place_count,
            bootstrapped_empty_target_count=bootstrapped_empty_target_count,
            checkpoint_upserted_count=checkpoint_upserted_count,
            freshness_upserted_count=len(freshness_rows),
            missing_source_count=sum(1 for row in freshness_rows if row.freshness_status == "missing_source"),
            replay_requested_count=replay_requested_count,
            fresh_count=sum(1 for row in freshness_rows if row.freshness_status == "fresh"),
            stale_count=sum(1 for row in freshness_rows if row.freshness_status == "stale"),
            unknown_freshness_count=sum(1 for row in freshness_rows if row.freshness_status == "unknown"),
            missing_target_count=sum(1 for row in freshness_rows if row.freshness_status == "missing_target"),
            sync_results=tuple(sync_results),
            freshness_rows=tuple(freshness_rows),
            report_json_path=report_json_path,
            report_markdown_path=report_markdown_path,
        )
        connection.execute(
            f"""
            UPDATE {MAINLINE_LOCAL_LEDGER_SYNC_RUN_TABLE}
            SET run_status = 'completed',
                queue_enqueued_count = ?,
                queue_claimed_count = ?,
                synced_ledger_count = ?,
                missing_source_count = ?,
                completed_at = CURRENT_TIMESTAMP,
                summary_json = ?
            WHERE run_id = ?
            """,
            [
                summary.queue_enqueued_count,
                summary.queue_claimed_count,
                summary.synced_ledger_count,
                summary.missing_source_count,
                json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
                effective_run_id,
            ],
        )
        return summary
    except Exception:
        connection.execute(
            f"""
            UPDATE {MAINLINE_LOCAL_LEDGER_SYNC_RUN_TABLE}
            SET run_status = 'failed',
                completed_at = CURRENT_TIMESTAMP,
                summary_json = ?
            WHERE run_id = ?
            """,
            [json.dumps({"run_status": "failed"}, ensure_ascii=False), effective_run_id],
        )
        raise
    finally:
        connection.close()


def _collect_scope_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    settings: WorkspaceRoots,
    selected_ledgers: tuple[str, ...],
    source_map: dict[str, Path],
    source_latest_map: dict[str, date],
    replay_start_map: dict[str, date],
    replay_confirm_map: dict[str, date],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for ledger_name in selected_ledgers:
        spec = MAINLINE_LEDGER_SPEC_BY_NAME[ledger_name]
        target_path = spec.target_path(settings)
        source_path = source_map.get(ledger_name, target_path)
        state = _resolve_source_state(
            ledger_name=ledger_name,
            source_path=source_path,
            explicit_latest_bar_dt=source_latest_map.get(ledger_name),
        )
        checkpoint = _load_checkpoint(connection=connection, ledger_name=ledger_name)
        dirty_reason = _derive_dirty_reason(
            checkpoint=checkpoint,
            same_path=source_path.resolve() == target_path.resolve(),
            source_exists=bool(state["source_exists"]),
            target_exists=target_path.exists(),
            source_latest_bar_dt=state["source_latest_bar_dt"],
            replay_start_bar_dt=replay_start_map.get(ledger_name),
            replay_confirm_until_dt=replay_confirm_map.get(ledger_name),
            source_fingerprint=str(state["source_fingerprint"]),
        )
        rows.append(
            {
                "queue_nk": f"mainline-ledger::{ledger_name}::{dirty_reason}",
                "ledger_name": ledger_name,
                "module_name": spec.module_name,
                "source_path": source_path,
                "target_path": target_path,
                "dirty_reason": dirty_reason,
                "source_latest_bar_dt": state["source_latest_bar_dt"],
                "replay_start_bar_dt": replay_start_map.get(ledger_name),
                "replay_confirm_until_dt": replay_confirm_map.get(ledger_name),
                "source_fingerprint": state["source_fingerprint"],
            }
        )
    return rows


def _enqueue_dirty_scopes(
    *,
    connection: duckdb.DuckDBPyConnection,
    scope_rows: list[dict[str, object]],
    run_id: str,
) -> int:
    count = 0
    for row in scope_rows:
        if row["dirty_reason"] is None:
            continue
        existing = connection.execute(
            f"SELECT queue_nk FROM {MAINLINE_LOCAL_LEDGER_SYNC_DIRTY_QUEUE_TABLE} WHERE queue_nk = ?",
            [str(row["queue_nk"])],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {MAINLINE_LOCAL_LEDGER_SYNC_DIRTY_QUEUE_TABLE} (
                    queue_nk, scope_nk, ledger_name, module_name, source_path, target_path,
                    dirty_reason, source_latest_bar_dt, replay_start_bar_dt, replay_confirm_until_dt,
                    source_fingerprint, queue_status, first_seen_run_id, last_materialized_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                [
                    str(row["queue_nk"]),
                    f"mainline-ledger::{row['ledger_name']}",
                    str(row["ledger_name"]),
                    str(row["module_name"]),
                    str(row["source_path"]),
                    str(row["target_path"]),
                    str(row["dirty_reason"]),
                    _to_date(row["source_latest_bar_dt"]),
                    _to_date(row["replay_start_bar_dt"]),
                    _to_date(row["replay_confirm_until_dt"]),
                    str(row["source_fingerprint"]),
                    run_id,
                    run_id,
                ],
            )
            count += 1
            continue
        connection.execute(
            f"""
            UPDATE {MAINLINE_LOCAL_LEDGER_SYNC_DIRTY_QUEUE_TABLE}
            SET source_path = ?,
                target_path = ?,
                source_latest_bar_dt = ?,
                replay_start_bar_dt = ?,
                replay_confirm_until_dt = ?,
                source_fingerprint = ?,
                queue_status = 'pending',
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [
                str(row["source_path"]),
                str(row["target_path"]),
                _to_date(row["source_latest_bar_dt"]),
                _to_date(row["replay_start_bar_dt"]),
                _to_date(row["replay_confirm_until_dt"]),
                str(row["source_fingerprint"]),
                str(row["queue_nk"]),
            ],
        )
    return count


def _claim_scopes(
    *,
    connection: duckdb.DuckDBPyConnection,
    selected_ledgers: tuple[str, ...],
    run_id: str,
) -> list[dict[str, object]]:
    if not selected_ledgers:
        return []
    placeholders = ", ".join("?" for _ in selected_ledgers)
    rows = connection.execute(
        f"""
        SELECT queue_nk, ledger_name, module_name, source_path, target_path, dirty_reason,
               source_latest_bar_dt, replay_start_bar_dt, replay_confirm_until_dt, source_fingerprint
        FROM {MAINLINE_LOCAL_LEDGER_SYNC_DIRTY_QUEUE_TABLE}
        WHERE queue_status IN ('pending', 'claimed', 'failed')
          AND ledger_name IN ({placeholders})
        ORDER BY ledger_name, enqueued_at
        """,
        list(selected_ledgers),
    ).fetchall()
    claimed: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {MAINLINE_LOCAL_LEDGER_SYNC_DIRTY_QUEUE_TABLE}
            SET queue_status = 'claimed',
                claimed_at = CURRENT_TIMESTAMP,
                last_claimed_run_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [run_id, str(row[0])],
        )
        claimed.append(
            {
                "queue_nk": str(row[0]),
                "ledger_name": str(row[1]),
                "module_name": str(row[2]),
                "source_path": Path(str(row[3])).resolve(),
                "target_path": Path(str(row[4])).resolve(),
                "dirty_reason": str(row[5]),
                "source_latest_bar_dt": _to_date(row[6]),
                "replay_start_bar_dt": _to_date(row[7]),
                "replay_confirm_until_dt": _to_date(row[8]),
                "source_fingerprint": str(row[9]),
            }
        )
    return claimed


def _sync_one_ledger(*, spec, settings: WorkspaceRoots, source_path: Path, target_path: Path) -> str:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path.resolve() == target_path.resolve():
        existed_before = target_path.exists()
        spec.bootstrap(settings)
        return "observed_in_place" if existed_before else "bootstrapped_empty_target"
    shutil.copy2(source_path, target_path)
    spec.bootstrap(settings)
    return "copied_from_source"


def _write_freshness_readout(
    *,
    connection: duckdb.DuckDBPyConnection,
    settings: WorkspaceRoots,
    selected_ledgers: tuple[str, ...],
    source_map: dict[str, Path],
    source_latest_map: dict[str, date],
    dirty_reason_map: dict[str, str | None],
    run_id: str,
) -> list[MainlineLocalLedgerFreshnessRow]:
    rows: list[MainlineLocalLedgerFreshnessRow] = []
    for ledger_name in selected_ledgers:
        spec = MAINLINE_LEDGER_SPEC_BY_NAME[ledger_name]
        target_path = spec.target_path(settings)
        source_path = source_map.get(ledger_name, target_path)
        state = _resolve_source_state(
            ledger_name=ledger_name,
            source_path=source_path,
            explicit_latest_bar_dt=source_latest_map.get(ledger_name),
        )
        checkpoint = _load_checkpoint(connection=connection, ledger_name=ledger_name)
        last_completed_bar_dt = None if checkpoint is None else checkpoint["last_completed_bar_dt"]
        lag_days = None
        if state["source_latest_bar_dt"] is not None and last_completed_bar_dt is not None:
            lag_days = int((state["source_latest_bar_dt"] - last_completed_bar_dt).days)
        same_path = source_path.resolve() == target_path.resolve()
        if not state["source_exists"] and not same_path:
            status = "missing_source"
        elif not target_path.exists():
            status = "missing_target"
        elif lag_days is None:
            status = "unknown"
        elif lag_days <= 0:
            status = "fresh"
        else:
            status = "stale"
        freshness_row = MainlineLocalLedgerFreshnessRow(
            ledger_name=ledger_name,
            module_name=spec.module_name,
            source_path=source_path,
            target_path=target_path,
            source_exists=bool(state["source_exists"]),
            target_exists=target_path.exists(),
            source_latest_bar_dt=state["source_latest_bar_dt"],
            last_completed_bar_dt=last_completed_bar_dt,
            freshness_lag_days=lag_days,
            freshness_status=status,
            last_dirty_reason=None if dirty_reason_map.get(ledger_name) is None else str(dirty_reason_map[ledger_name]),
            source_fingerprint=str(state["source_fingerprint"]),
        )
        connection.execute(
            f"""
            INSERT OR REPLACE INTO {MAINLINE_LOCAL_LEDGER_FRESHNESS_READOUT_TABLE} (
                ledger_name, module_name, source_path, target_path, source_exists, target_exists,
                source_latest_bar_dt, last_completed_bar_dt, freshness_lag_days, freshness_status,
                last_dirty_reason, source_fingerprint, last_run_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                freshness_row.ledger_name,
                freshness_row.module_name,
                str(freshness_row.source_path),
                str(freshness_row.target_path),
                freshness_row.source_exists,
                freshness_row.target_exists,
                freshness_row.source_latest_bar_dt,
                freshness_row.last_completed_bar_dt,
                freshness_row.freshness_lag_days,
                freshness_row.freshness_status,
                freshness_row.last_dirty_reason,
                freshness_row.source_fingerprint,
                run_id,
            ],
        )
        rows.append(freshness_row)
    return rows


def _load_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    ledger_name: str,
) -> dict[str, object] | None:
    row = connection.execute(
        f"""
        SELECT last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint
        FROM {MAINLINE_LOCAL_LEDGER_SYNC_CHECKPOINT_TABLE}
        WHERE ledger_name = ?
        """,
        [ledger_name],
    ).fetchone()
    if row is None:
        return None
    return {
        "last_completed_bar_dt": _to_date(row[0]),
        "tail_start_bar_dt": _to_date(row[1]),
        "tail_confirm_until_dt": _to_date(row[2]),
        "source_fingerprint": "" if row[3] is None else str(row[3]),
    }


def _derive_dirty_reason(
    *,
    checkpoint: dict[str, object] | None,
    same_path: bool,
    source_exists: bool,
    target_exists: bool,
    source_latest_bar_dt: date | None,
    replay_start_bar_dt: date | None,
    replay_confirm_until_dt: date | None,
    source_fingerprint: str,
) -> str | None:
    if not source_exists:
        return "bootstrap_missing_target" if same_path and not target_exists else None
    if not target_exists:
        return "target_missing"
    if checkpoint is None:
        return "bootstrap_missing_checkpoint"
    if str(checkpoint["source_fingerprint"]) != source_fingerprint:
        return "source_fingerprint_changed"
    last_completed_bar_dt = _to_date(checkpoint["last_completed_bar_dt"])
    if last_completed_bar_dt is None:
        return "checkpoint_incomplete"
    if source_latest_bar_dt is not None and source_latest_bar_dt > last_completed_bar_dt:
        return "source_advanced"
    tail_start_bar_dt = _to_date(checkpoint["tail_start_bar_dt"])
    if replay_start_bar_dt is not None and (tail_start_bar_dt is None or replay_start_bar_dt < tail_start_bar_dt):
        return "source_replayed"
    tail_confirm_until_dt = _to_date(checkpoint["tail_confirm_until_dt"])
    if replay_confirm_until_dt is not None and (
        tail_confirm_until_dt is None or replay_confirm_until_dt > tail_confirm_until_dt
    ):
        return "tail_confirm_advanced"
    return None


def _upsert_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    ledger_name: str,
    module_name: str,
    source_path: Path,
    target_path: Path,
    last_completed_bar_dt: date | None,
    tail_start_bar_dt: date | None,
    tail_confirm_until_dt: date | None,
    source_fingerprint: str,
    last_run_id: str,
) -> None:
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {MAINLINE_LOCAL_LEDGER_SYNC_CHECKPOINT_TABLE} (
            ledger_name, module_name, source_path, target_path, last_completed_bar_dt,
            tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            ledger_name,
            module_name,
            str(source_path),
            str(target_path),
            last_completed_bar_dt,
            tail_start_bar_dt,
            tail_confirm_until_dt,
            source_fingerprint,
            last_run_id,
        ],
    )


def _resolve_tail_dates(
    *,
    source_latest_bar_dt: date | None,
    replay_start_bar_dt: date | None,
    replay_confirm_until_dt: date | None,
) -> tuple[date | None, date | None, date | None]:
    last_completed_bar_dt = replay_confirm_until_dt or source_latest_bar_dt
    tail_start_bar_dt = replay_start_bar_dt or last_completed_bar_dt
    tail_confirm_until_dt = replay_confirm_until_dt or last_completed_bar_dt
    return last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt


def _resolve_source_state(
    *,
    ledger_name: str,
    source_path: Path,
    explicit_latest_bar_dt: date | None,
) -> dict[str, object]:
    source_exists = source_path.exists()
    latest_bar_dt = explicit_latest_bar_dt
    if source_exists and latest_bar_dt is None:
        latest_bar_dt = _detect_latest_bar_date(ledger_name=ledger_name, database_path=source_path)
    return {
        "source_exists": source_exists,
        "source_latest_bar_dt": latest_bar_dt,
        "source_fingerprint": _build_source_fingerprint(source_path=source_path, source_latest_bar_dt=latest_bar_dt),
    }


def _detect_latest_bar_date(*, ledger_name: str, database_path: Path) -> date | None:
    if not database_path.exists():
        return None
    candidates = MAINLINE_LEDGER_LATEST_BAR_DATE_CANDIDATES.get(ledger_name, ())
    if not candidates:
        return None
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        latest_values: list[date] = []
        for table_name, column_names in candidates:
            available_columns = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'main'
                      AND table_name = ?
                    """,
                    [table_name],
                ).fetchall()
            }
            if not available_columns:
                continue
            for column_name in column_names:
                if column_name not in available_columns:
                    continue
                value = connection.execute(
                    f'SELECT MAX(CAST("{column_name}" AS DATE)) FROM "{table_name}"'
                ).fetchone()
                candidate = _to_date(None if value is None else value[0])
                if candidate is not None:
                    latest_values.append(candidate)
        return max(latest_values) if latest_values else None
    finally:
        connection.close()


def _build_source_fingerprint(*, source_path: Path, source_latest_bar_dt: date | None) -> str:
    if not source_path.exists():
        return json.dumps(
            {"exists": False, "path": str(source_path), "source_latest_bar_dt": _dump_date(source_latest_bar_dt)},
            ensure_ascii=False,
            sort_keys=True,
        )
    stat = source_path.stat()
    return json.dumps(
        {
            "exists": True,
            "path": str(source_path),
            "size_bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
            "source_latest_bar_dt": _dump_date(source_latest_bar_dt),
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def _normalize_date_map(raw_map: dict[str, str | date] | None) -> dict[str, date]:
    normalized: dict[str, date] = {}
    for ledger_name, raw_value in (raw_map or {}).items():
        normalized[ledger_name] = _to_date(raw_value)  # type: ignore[assignment]
    return {key: value for key, value in normalized.items() if value is not None}


def _write_reports(
    *,
    settings: WorkspaceRoots,
    run_id: str,
    sync_results: list[MainlineLocalLedgerSyncResult],
    freshness_rows: list[MainlineLocalLedgerFreshnessRow],
    summary_path: Path | None,
) -> tuple[Path, Path]:
    report_root = settings.module_report_root("data") / "mainline_local_ledger_incremental_sync"
    report_root.mkdir(parents=True, exist_ok=True)
    report_json_path = (summary_path or (report_root / f"{run_id}.json")).resolve()
    report_json_path.parent.mkdir(parents=True, exist_ok=True)
    report_markdown_path = report_json_path.with_suffix(".md")
    payload = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "sync_results": [row.as_dict() for row in sync_results],
        "freshness_rows": [row.as_dict() for row in freshness_rows],
    }
    report_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# mainline local ledger incremental sync",
        "",
        f"- run_id: `{run_id}`",
        f"- generated_at_utc: `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "## sync_results",
    ]
    for row in sync_results:
        lines.append(
            "- "
            + f"`{row.ledger_name}` dirty_reason={row.dirty_reason} | sync_action={row.sync_action} | "
            + f"last_completed_bar_dt={_dump_date(row.last_completed_bar_dt)} | "
            + f"tail_start_bar_dt={_dump_date(row.tail_start_bar_dt)} | "
            + f"tail_confirm_until_dt={_dump_date(row.tail_confirm_until_dt)}"
        )
    lines.extend(["", "## freshness"])
    for row in freshness_rows:
        lines.append(
            "- "
            + f"`{row.ledger_name}` freshness_status={row.freshness_status} | "
            + f"source_latest_bar_dt={_dump_date(row.source_latest_bar_dt)} | "
            + f"last_completed_bar_dt={_dump_date(row.last_completed_bar_dt)} | "
            + f"freshness_lag_days={row.freshness_lag_days}"
        )
    report_markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_json_path, report_markdown_path


def _to_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _dump_date(value: date | None) -> str | None:
    return None if value is None else value.isoformat()
