"""执行正式 data-grade 的 `position -> portfolio_plan` v2 物化。"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.portfolio_plan.bootstrap import (
    PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE,
    PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_CHECKPOINT_TABLE,
    PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE,
    PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_RUN_TABLE,
    PORTFOLIO_PLAN_SNAPSHOT_TABLE,
    PORTFOLIO_PLAN_WORK_QUEUE_TABLE,
    bootstrap_portfolio_plan_ledger,
    portfolio_plan_ledger_path,
)
from mlq.portfolio_plan.materialization import (
    DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
    _CandidateLedgerBundle,
    _PositionBridgeRow,
    _PortfolioCapacitySnapshotRow,
    _aggregate_materialization_action,
    _build_capacity_reason_summary,
    _build_candidate_bundle,
    _build_capacity_snapshot_nk,
    _decision_sort_key,
    _evaluate_candidate_decision,
    _resolve_binding_constraint_code,
    _resolve_capacity_decision_reason_code,
    _upsert_materialized_row,
)

DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION: Final[str] = "portfolio-plan-v2"
DEFAULT_SOURCE_POSITION_TABLE: Final[str] = (
    "position_candidate_audit+position_capacity_snapshot+position_sizing_snapshot"
)
DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE: Final[str] = "position_candidate_audit"
DEFAULT_POSITION_CAPACITY_TABLE: Final[str] = "position_capacity_snapshot"
DEFAULT_POSITION_SIZING_TABLE: Final[str] = "position_sizing_snapshot"
DEFAULT_PORTFOLIO_PLAN_RUNNER_NAME: Final[str] = "portfolio_plan_builder"
DEFAULT_PORTFOLIO_PLAN_RUNNER_VERSION: Final[str] = "v3"
DEFAULT_CANDIDATE_CHECKPOINT_SCOPE: Final[str] = "candidate"


@dataclass(frozen=True)
class PortfolioPlanBuildSummary:
    """汇总一次 data-grade `portfolio_plan` 运行。"""

    run_id: str
    runner_name: str
    runner_version: str
    execution_mode: str
    portfolio_id: str
    portfolio_plan_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_candidate_count: int
    processed_candidate_count: int
    admitted_count: int
    blocked_count: int
    trimmed_count: int
    deferred_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    checkpoint_upserted_count: int
    freshness_updated_count: int
    requested_total_weight: float
    admitted_total_weight: float
    trimmed_total_weight: float
    blocked_total_weight: float
    deferred_total_weight: float
    decision_reason_counts: dict[str, int]
    trade_readiness_counts: dict[str, int]
    portfolio_gross_cap_weight: float
    portfolio_gross_used_weight: float
    portfolio_gross_remaining_weight: float
    latest_reference_trade_date: str | None
    expected_reference_trade_date: str | None
    freshness_status: str
    source_position_table: str
    position_ledger_path: str
    portfolio_plan_ledger_path: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 `summary_json` 的稳定字典。"""

        return asdict(self)


def run_portfolio_plan_build(
    *,
    portfolio_id: str,
    portfolio_gross_cap_weight: float,
    settings: WorkspaceRoots | None = None,
    position_path: Path | None = None,
    portfolio_plan_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    candidate_nks: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    source_position_table: str = DEFAULT_SOURCE_POSITION_TABLE,
    portfolio_plan_contract_version: str = DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION,
    runner_name: str = DEFAULT_PORTFOLIO_PLAN_RUNNER_NAME,
    runner_version: str = DEFAULT_PORTFOLIO_PLAN_RUNNER_VERSION,
    bootstrap_mode: bool = False,
    incremental_mode: bool | None = None,
    replay_mode: bool = False,
    summary_path: Path | None = None,
) -> PortfolioPlanBuildSummary:
    """Read official `position` outputs and materialize `portfolio_plan` v2 ledgers."""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_candidate_nks = tuple(
        sorted({item for item in candidate_nks or () if item})
    )
    normalized_limit = max(int(limit), 1)
    normalized_cap_weight = max(float(portfolio_gross_cap_weight), 0.0)
    execution_mode = _resolve_execution_mode(
        bootstrap_mode=bootstrap_mode,
        incremental_mode=incremental_mode,
        replay_mode=replay_mode,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        candidate_nks=normalized_candidate_nks,
    )
    planning_run_id = run_id or _build_portfolio_plan_run_id()
    resolved_position_path = Path(position_path or workspace.databases.position)
    resolved_portfolio_plan_path = Path(
        portfolio_plan_path or portfolio_plan_ledger_path(workspace)
    )
    _ensure_database_exists(resolved_position_path, label="position")
    portfolio_plan_connection = duckdb.connect(str(resolved_portfolio_plan_path))
    claimed_queue_rows: list[dict[str, object]] = []
    try:
        bootstrap_portfolio_plan_ledger(workspace, connection=portfolio_plan_connection)
        source_bridge_rows = _load_position_bridge_rows(
            position_path=resolved_position_path,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            reference_trade_dates=(),
            instruments=normalized_instruments,
            candidate_nks=normalized_candidate_nks,
            limit=normalized_limit if execution_mode == "bootstrap" else None,
        )
        _insert_run_row(
            portfolio_plan_connection,
            run_id=planning_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode=execution_mode,
            portfolio_id=portfolio_id,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_candidate_count=len(source_bridge_rows),
            source_position_table=source_position_table,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
        )

        queue_enqueued_count = 0
        processed_bridge_rows = source_bridge_rows
        if execution_mode in {"incremental", "replay"}:
            queue_enqueued_count = _enqueue_portfolio_plan_dirty_candidates(
                portfolio_plan_connection,
                bridge_rows=source_bridge_rows,
                run_id=planning_run_id,
                portfolio_id=portfolio_id,
                portfolio_plan_contract_version=portfolio_plan_contract_version,
                portfolio_gross_cap_weight=normalized_cap_weight,
                force_replay=execution_mode == "replay",
            )
            claimed_queue_rows = _claim_portfolio_plan_queue_rows(
                portfolio_plan_connection,
                run_id=planning_run_id,
                portfolio_id=portfolio_id,
                candidate_nks={row.candidate_nk for row in source_bridge_rows},
                limit=normalized_limit,
            )
            processed_bridge_rows = _load_bridge_rows_for_claimed_dates(
                position_path=resolved_position_path,
                claimed_queue_rows=claimed_queue_rows,
                signal_start_date=normalized_start_date,
                signal_end_date=normalized_end_date,
            )

        counts = _materialize_portfolio_plan_rows(
            connection=portfolio_plan_connection,
            bridge_rows=processed_bridge_rows,
            run_id=planning_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode=execution_mode,
            portfolio_id=portfolio_id,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            portfolio_gross_cap_weight=normalized_cap_weight,
            source_position_table=source_position_table,
            position_path=resolved_position_path,
            portfolio_plan_path=resolved_portfolio_plan_path,
            queue_rows=claimed_queue_rows,
        )
        latest_reference_trade_date, expected_reference_trade_date, freshness_status = (
            _upsert_portfolio_plan_freshness_audit(
                portfolio_plan_connection,
                portfolio_id=portfolio_id,
                expected_reference_trade_date=_max_reference_trade_date(source_bridge_rows),
                last_success_run_id=planning_run_id,
            )
        )
        summary = PortfolioPlanBuildSummary(
            run_id=planning_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            execution_mode=execution_mode,
            portfolio_id=portfolio_id,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
            signal_start_date=(
                None if normalized_start_date is None else normalized_start_date.isoformat()
            ),
            signal_end_date=(
                None if normalized_end_date is None else normalized_end_date.isoformat()
            ),
            bounded_candidate_count=len(source_bridge_rows),
            processed_candidate_count=counts["candidate_count"],
            admitted_count=counts["admitted_count"],
            blocked_count=counts["blocked_count"],
            trimmed_count=counts["trimmed_count"],
            deferred_count=counts["deferred_count"],
            inserted_count=counts["inserted_count"],
            reused_count=counts["reused_count"],
            rematerialized_count=counts["rematerialized_count"],
            queue_enqueued_count=queue_enqueued_count,
            queue_claimed_count=len(claimed_queue_rows),
            checkpoint_upserted_count=counts["checkpoint_upserted_count"],
            freshness_updated_count=1,
            requested_total_weight=counts["requested_total_weight"],
            admitted_total_weight=counts["admitted_total_weight"],
            trimmed_total_weight=counts["trimmed_total_weight"],
            blocked_total_weight=counts["blocked_total_weight"],
            deferred_total_weight=counts["deferred_total_weight"],
            decision_reason_counts=dict(sorted(counts["decision_reason_counts"].items())),
            trade_readiness_counts=dict(sorted(counts["trade_readiness_counts"].items())),
            portfolio_gross_cap_weight=normalized_cap_weight,
            portfolio_gross_used_weight=counts["latest_used_weight"],
            portfolio_gross_remaining_weight=counts["latest_remaining_weight"],
            latest_reference_trade_date=(
                None
                if latest_reference_trade_date is None
                else latest_reference_trade_date.isoformat()
            ),
            expected_reference_trade_date=(
                None
                if expected_reference_trade_date is None
                else expected_reference_trade_date.isoformat()
            ),
            freshness_status=freshness_status,
            source_position_table=source_position_table,
            position_ledger_path=str(resolved_position_path),
            portfolio_plan_ledger_path=str(resolved_portfolio_plan_path),
        )
        _mark_run_completed(portfolio_plan_connection, run_id=planning_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        if claimed_queue_rows:
            _mark_queue_rows_failed(
                portfolio_plan_connection,
                queue_nks=[str(row["queue_nk"]) for row in claimed_queue_rows],
                run_id=planning_run_id,
                error_text="portfolio_plan_build_failed",
            )
        _mark_run_failed(portfolio_plan_connection, run_id=planning_run_id)
        raise
    finally:
        portfolio_plan_connection.close()


def _resolve_execution_mode(
    *,
    bootstrap_mode: bool,
    incremental_mode: bool | None,
    replay_mode: bool,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    candidate_nks: tuple[str, ...],
) -> str:
    explicit_mode_count = int(bootstrap_mode) + int(replay_mode)
    if incremental_mode is True:
        explicit_mode_count += 1
    if explicit_mode_count > 1:
        raise ValueError(
            "bootstrap_mode / incremental_mode / replay_mode 只能显式选择一种。"
        )
    if replay_mode:
        return "replay"
    if bootstrap_mode:
        return "bootstrap"
    if incremental_mode is True:
        return "incremental"
    if incremental_mode is False:
        return "bootstrap"
    has_bounded_scope = bool(
        signal_start_date is not None
        or signal_end_date is not None
        or instruments
        or candidate_nks
    )
    return "bootstrap" if has_bounded_scope else "incremental"


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_portfolio_plan_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"portfolio-plan-{timestamp}"


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _load_position_bridge_rows(
    *,
    position_path: Path,
    signal_start_date: date | None,
    signal_end_date: date | None,
    reference_trade_dates: tuple[date, ...],
    instruments: tuple[str, ...],
    candidate_nks: tuple[str, ...],
    limit: int | None,
) -> list[_PositionBridgeRow]:
    connection = duckdb.connect(str(position_path), read_only=True)
    try:
        _ensure_bridge_tables_exist(connection)
        parameters: list[object] = []
        where_clauses: list[str] = []
        if signal_start_date is not None:
            where_clauses.append("s.reference_trade_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("s.reference_trade_date <= ?")
            parameters.append(signal_end_date)
        if reference_trade_dates:
            placeholders = ", ".join("?" for _ in reference_trade_dates)
            where_clauses.append(f"s.reference_trade_date IN ({placeholders})")
            parameters.extend(reference_trade_dates)
        if instruments:
            placeholders = ", ".join("?" for _ in instruments)
            where_clauses.append(f"a.instrument IN ({placeholders})")
            parameters.extend(instruments)
        if candidate_nks:
            placeholders = ", ".join("?" for _ in candidate_nks)
            where_clauses.append(f"a.candidate_nk IN ({placeholders})")
            parameters.extend(candidate_nks)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        limit_sql = "LIMIT ?" if limit is not None else ""
        if limit is not None:
            parameters.append(limit)
        rows = connection.execute(
            f"""
            SELECT
                a.candidate_nk,
                a.instrument,
                a.policy_id,
                s.reference_trade_date,
                a.candidate_status,
                a.blocked_reason_code,
                s.position_action_decision,
                COALESCE(s.schedule_stage, 't+1'),
                COALESCE(s.schedule_lag_days, 1),
                c.final_allowed_position_weight,
                COALESCE(s.required_reduction_weight, c.required_reduction_weight, 0),
                COALESCE(c.remaining_single_name_capacity_weight, 0),
                COALESCE(c.remaining_portfolio_capacity_weight, 0),
                COALESCE(c.binding_cap_code, 'no_binding_cap'),
                COALESCE(c.capacity_source_code, 'unknown')
            FROM {DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE} AS a
            INNER JOIN {DEFAULT_POSITION_CAPACITY_TABLE} AS c
                ON c.candidate_nk = a.candidate_nk
            INNER JOIN {DEFAULT_POSITION_SIZING_TABLE} AS s
                ON s.candidate_nk = a.candidate_nk
            {where_sql}
            ORDER BY s.reference_trade_date, a.instrument, a.candidate_nk
            {limit_sql}
            """,
            parameters,
        ).fetchall()
        return [
            _PositionBridgeRow(
                candidate_nk=str(row[0]),
                instrument=str(row[1]),
                policy_id=str(row[2]),
                reference_trade_date=_normalize_date_value(
                    row[3],
                    field_name="reference_trade_date",
                ),
                candidate_status=_normalize_optional_str(row[4]).lower(),
                blocked_reason_code=_normalize_optional_str(row[5]) or None,
                position_action_decision=_normalize_optional_str(row[6]),
                schedule_stage=_normalize_optional_str(row[7]) or "t+1",
                schedule_lag_days=max(int(row[8] or 1), 0),
                final_allowed_position_weight=float(row[9] or 0.0),
                required_reduction_weight=float(row[10] or 0.0),
                remaining_single_name_capacity_weight=float(row[11] or 0.0),
                remaining_portfolio_capacity_weight=float(row[12] or 0.0),
                binding_cap_code=_normalize_optional_str(row[13]) or "no_binding_cap",
                capacity_source_code=_normalize_optional_str(row[14]) or "unknown",
            )
            for row in rows
        ]
    finally:
        connection.close()


def _load_bridge_rows_for_claimed_dates(
    *,
    position_path: Path,
    claimed_queue_rows: list[dict[str, object]],
    signal_start_date: date | None,
    signal_end_date: date | None,
) -> list[_PositionBridgeRow]:
    if not claimed_queue_rows:
        return []
    claimed_dates = tuple(
        sorted(
            {
                _normalize_date_value(
                    row["reference_trade_date"],
                    field_name="queue.reference_trade_date",
                )
                for row in claimed_queue_rows
            }
        )
    )
    return _load_position_bridge_rows(
        position_path=position_path,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        reference_trade_dates=claimed_dates,
        instruments=(),
        candidate_nks=(),
        limit=None,
    )


def _ensure_bridge_tables_exist(connection: duckdb.DuckDBPyConnection) -> None:
    rows = connection.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        """
    ).fetchall()
    existing_tables = {str(row[0]) for row in rows}
    required_tables = {
        DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE,
        DEFAULT_POSITION_CAPACITY_TABLE,
        DEFAULT_POSITION_SIZING_TABLE,
    }
    missing_tables = sorted(required_tables - existing_tables)
    if missing_tables:
        raise ValueError(
            "Missing required position bridge tables: " + ", ".join(missing_tables)
        )


def _build_queue_nk(
    *,
    portfolio_id: str,
    candidate_nk: str,
    reference_trade_date: date,
) -> str:
    return f"{portfolio_id}|{candidate_nk}|{reference_trade_date.isoformat()}"


def _build_candidate_checkpoint_nk(*, portfolio_id: str, candidate_nk: str) -> str:
    return f"{portfolio_id}|candidate|{candidate_nk}"


def _build_portfolio_checkpoint_nk(
    *,
    portfolio_id: str,
    checkpoint_scope: str,
) -> str:
    return f"{portfolio_id}|{checkpoint_scope}"


def _build_source_fingerprint(
    *,
    bridge_row: _PositionBridgeRow,
    portfolio_gross_cap_weight: float,
    portfolio_plan_contract_version: str,
) -> str:
    payload = {
        "binding_cap_code": bridge_row.binding_cap_code,
        "blocked_reason_code": bridge_row.blocked_reason_code,
        "candidate_nk": bridge_row.candidate_nk,
        "candidate_status": bridge_row.candidate_status,
        "capacity_source_code": bridge_row.capacity_source_code,
        "contract_version": portfolio_plan_contract_version,
        "final_allowed_position_weight": round(
            float(bridge_row.final_allowed_position_weight),
            12,
        ),
        "instrument": bridge_row.instrument,
        "policy_id": bridge_row.policy_id,
        "portfolio_gross_cap_weight": round(float(portfolio_gross_cap_weight), 12),
        "position_action_decision": bridge_row.position_action_decision,
        "reference_trade_date": bridge_row.reference_trade_date.isoformat(),
        "remaining_portfolio_capacity_weight": round(
            float(bridge_row.remaining_portfolio_capacity_weight),
            12,
        ),
        "remaining_single_name_capacity_weight": round(
            float(bridge_row.remaining_single_name_capacity_weight),
            12,
        ),
        "required_reduction_weight": round(
            float(bridge_row.required_reduction_weight),
            12,
        ),
        "schedule_lag_days": int(bridge_row.schedule_lag_days),
        "schedule_stage": bridge_row.schedule_stage,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _load_portfolio_plan_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    checkpoint_nk: str,
) -> dict[str, object] | None:
    row = connection.execute(
        f"""
        SELECT
            checkpoint_nk,
            portfolio_id,
            checkpoint_scope,
            COALESCE(last_completed_reference_trade_date, latest_reference_trade_date),
            COALESCE(last_completed_candidate_nk, last_candidate_nk),
            COALESCE(last_run_id, last_success_run_id),
            checkpoint_payload_json
        FROM {PORTFOLIO_PLAN_CHECKPOINT_TABLE}
        WHERE checkpoint_nk = ?
        """,
        [checkpoint_nk],
    ).fetchone()
    if row is None:
        return None
    return {
        "checkpoint_nk": str(row[0]),
        "portfolio_id": str(row[1]),
        "checkpoint_scope": str(row[2]),
        "last_completed_reference_trade_date": (
            None
            if row[3] is None
            else _normalize_date_value(row[3], field_name="checkpoint.reference_trade_date")
        ),
        "last_completed_candidate_nk": None if row[4] is None else str(row[4]),
        "last_run_id": None if row[5] is None else str(row[5]),
        "checkpoint_payload_json": None if row[6] is None else str(row[6]),
    }


def _load_candidate_scope_presence(
    connection: duckdb.DuckDBPyConnection,
    *,
    portfolio_id: str,
    candidate_nk: str,
    reference_trade_date: date,
    portfolio_plan_contract_version: str,
) -> dict[str, bool]:
    row = connection.execute(
        f"""
        SELECT
            EXISTS(
                SELECT 1
                FROM {PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE}
                WHERE portfolio_id = ?
                  AND candidate_nk = ?
                  AND reference_trade_date = ?
                  AND portfolio_plan_contract_version = ?
            ),
            EXISTS(
                SELECT 1
                FROM {PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE}
                WHERE portfolio_id = ?
                  AND candidate_nk = ?
                  AND reference_trade_date = ?
                  AND portfolio_plan_contract_version = ?
            ),
            EXISTS(
                SELECT 1
                FROM {PORTFOLIO_PLAN_SNAPSHOT_TABLE}
                WHERE portfolio_id = ?
                  AND candidate_nk = ?
                  AND reference_trade_date = ?
                  AND portfolio_plan_contract_version = ?
            ),
            EXISTS(
                SELECT 1
                FROM {PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE}
                WHERE portfolio_id = ?
                  AND reference_trade_date = ?
                  AND portfolio_plan_contract_version = ?
            )
        """,
        [
            portfolio_id,
            candidate_nk,
            reference_trade_date,
            portfolio_plan_contract_version,
            portfolio_id,
            candidate_nk,
            reference_trade_date,
            portfolio_plan_contract_version,
            portfolio_id,
            candidate_nk,
            reference_trade_date,
            portfolio_plan_contract_version,
            portfolio_id,
            reference_trade_date,
            portfolio_plan_contract_version,
        ],
    ).fetchone()
    return {
        "decision_present": bool(row[0]),
        "allocation_present": bool(row[1]),
        "snapshot_present": bool(row[2]),
        "capacity_present": bool(row[3]),
    }


def _derive_queue_reason(
    *,
    checkpoint_row: dict[str, object] | None,
    candidate_scope_presence: dict[str, bool],
    source_fingerprint: str,
    force_replay: bool,
) -> str | None:
    if force_replay:
        return "replay_request"
    if checkpoint_row is None:
        return "bootstrap_missing_checkpoint"
    if not all(candidate_scope_presence.values()):
        return "missing_materialization"
    checkpoint_payload_json = checkpoint_row.get("checkpoint_payload_json")
    checkpoint_payload = (
        {}
        if not checkpoint_payload_json
        else json.loads(str(checkpoint_payload_json))
    )
    if str(checkpoint_payload.get("source_fingerprint", "")) != source_fingerprint:
        return "source_fingerprint_changed"
    return None


def _enqueue_portfolio_plan_dirty_candidates(
    connection: duckdb.DuckDBPyConnection,
    *,
    bridge_rows: list[_PositionBridgeRow],
    run_id: str,
    portfolio_id: str,
    portfolio_plan_contract_version: str,
    portfolio_gross_cap_weight: float,
    force_replay: bool,
) -> int:
    queue_enqueued_count = 0
    for bridge_row in bridge_rows:
        checkpoint_nk = _build_candidate_checkpoint_nk(
            portfolio_id=portfolio_id,
            candidate_nk=bridge_row.candidate_nk,
        )
        checkpoint_row = _load_portfolio_plan_checkpoint(
            connection,
            checkpoint_nk=checkpoint_nk,
        )
        source_fingerprint = _build_source_fingerprint(
            bridge_row=bridge_row,
            portfolio_gross_cap_weight=portfolio_gross_cap_weight,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
        )
        presence = _load_candidate_scope_presence(
            connection,
            portfolio_id=portfolio_id,
            candidate_nk=bridge_row.candidate_nk,
            reference_trade_date=bridge_row.reference_trade_date,
            portfolio_plan_contract_version=portfolio_plan_contract_version,
        )
        queue_reason = _derive_queue_reason(
            checkpoint_row=checkpoint_row,
            candidate_scope_presence=presence,
            source_fingerprint=source_fingerprint,
            force_replay=force_replay,
        )
        if queue_reason is None:
            continue
        queue_nk = _build_queue_nk(
            portfolio_id=portfolio_id,
            candidate_nk=bridge_row.candidate_nk,
            reference_trade_date=bridge_row.reference_trade_date,
        )
        existing = connection.execute(
            f"SELECT queue_nk FROM {PORTFOLIO_PLAN_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
            [queue_nk],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {PORTFOLIO_PLAN_WORK_QUEUE_TABLE} (
                    queue_nk,
                    portfolio_id,
                    candidate_nk,
                    reference_trade_date,
                    checkpoint_nk,
                    queue_reason,
                    queued_at,
                    queue_status,
                    source_fingerprint,
                    source_run_id,
                    source_candidate_nk,
                    first_enqueued_at,
                    last_enqueued_at
                )
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'pending', ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [
                    queue_nk,
                    portfolio_id,
                    bridge_row.candidate_nk,
                    bridge_row.reference_trade_date,
                    checkpoint_nk,
                    queue_reason,
                    source_fingerprint,
                    run_id,
                    bridge_row.candidate_nk,
                ],
            )
        else:
            connection.execute(
                f"""
                UPDATE {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
                SET
                    reference_trade_date = ?,
                    checkpoint_nk = ?,
                    queue_reason = ?,
                    queued_at = CURRENT_TIMESTAMP,
                    queue_status = 'pending',
                    source_fingerprint = ?,
                    source_run_id = ?,
                    source_candidate_nk = ?,
                    last_enqueued_at = CURRENT_TIMESTAMP,
                    last_error_text = NULL
                WHERE queue_nk = ?
                """,
                [
                    bridge_row.reference_trade_date,
                    checkpoint_nk,
                    queue_reason,
                    source_fingerprint,
                    run_id,
                    bridge_row.candidate_nk,
                    queue_nk,
                ],
            )
        queue_enqueued_count += 1
    return queue_enqueued_count


def _claim_portfolio_plan_queue_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    portfolio_id: str,
    candidate_nks: set[str],
    limit: int,
) -> list[dict[str, object]]:
    if not candidate_nks:
        return []
    placeholders = ", ".join("?" for _ in candidate_nks)
    rows = connection.execute(
        f"""
        SELECT
            queue_nk,
            candidate_nk,
            reference_trade_date,
            checkpoint_nk,
            queue_reason,
            source_fingerprint
        FROM {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
        WHERE portfolio_id = ?
          AND candidate_nk IN ({placeholders})
          AND queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY reference_trade_date, candidate_nk, COALESCE(queued_at, last_enqueued_at, first_enqueued_at)
        LIMIT ?
        """,
        [portfolio_id, *sorted(candidate_nks), limit],
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
            SET
                queue_status = 'claimed',
                claimed_at = CURRENT_TIMESTAMP,
                last_claimed_run_id = ?,
                last_error_text = NULL
            WHERE queue_nk = ?
            """,
            [run_id, str(row[0])],
        )
        claimed_rows.append(
            {
                "queue_nk": str(row[0]),
                "candidate_nk": str(row[1]),
                "reference_trade_date": _normalize_date_value(
                    row[2],
                    field_name="queue.reference_trade_date",
                ),
                "checkpoint_nk": str(row[3]),
                "queue_reason": str(row[4]),
                "source_fingerprint": None if row[5] is None else str(row[5]),
            }
        )
    return claimed_rows


def _mark_queue_rows_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nks: list[str],
    run_id: str,
) -> None:
    if not queue_nks:
        return
    placeholders = ", ".join("?" for _ in queue_nks)
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
        SET
            queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_success_run_id = ?,
            last_error_text = NULL
        WHERE queue_nk IN ({placeholders})
        """,
        [run_id, *queue_nks],
    )


def _mark_queue_rows_failed(
    connection: duckdb.DuckDBPyConnection,
    *,
    queue_nks: list[str],
    run_id: str,
    error_text: str,
) -> None:
    if not queue_nks:
        return
    placeholders = ", ".join("?" for _ in queue_nks)
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_WORK_QUEUE_TABLE}
        SET
            queue_status = 'failed',
            last_claimed_run_id = ?,
            last_error_text = ?
        WHERE queue_nk IN ({placeholders})
        """,
        [run_id, error_text, *queue_nks],
    )


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    execution_mode: str,
    portfolio_id: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_candidate_count: int,
    source_position_table: str,
    portfolio_plan_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {PORTFOLIO_PLAN_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            execution_mode,
            portfolio_id,
            signal_start_date,
            signal_end_date,
            bounded_candidate_count,
            source_position_table,
            portfolio_plan_contract_version
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            execution_mode,
            portfolio_id,
            signal_start_date,
            signal_end_date,
            bounded_candidate_count,
            source_position_table,
            portfolio_plan_contract_version,
        ],
    )


def _materialize_portfolio_plan_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    bridge_rows: list[_PositionBridgeRow],
    run_id: str,
    runner_name: str,
    runner_version: str,
    execution_mode: str,
    portfolio_id: str,
    portfolio_plan_contract_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    portfolio_gross_cap_weight: float,
    source_position_table: str,
    position_path: Path,
    portfolio_plan_path: Path,
    queue_rows: list[dict[str, object]],
) -> dict[str, object]:
    del runner_name, runner_version, execution_mode, signal_start_date, signal_end_date
    del source_position_table, position_path, portfolio_plan_path
    counts: dict[str, object] = {
        "candidate_count": 0,
        "admitted_count": 0,
        "blocked_count": 0,
        "trimmed_count": 0,
        "deferred_count": 0,
        "inserted_count": 0,
        "reused_count": 0,
        "rematerialized_count": 0,
        "checkpoint_upserted_count": 0,
        "requested_total_weight": 0.0,
        "admitted_total_weight": 0.0,
        "trimmed_total_weight": 0.0,
        "blocked_total_weight": 0.0,
        "deferred_total_weight": 0.0,
        "decision_reason_counts": defaultdict(int),
        "trade_readiness_counts": defaultdict(int),
        "latest_used_weight": 0.0,
        "latest_remaining_weight": portfolio_gross_cap_weight,
    }
    if not bridge_rows:
        return counts
    queue_rows_by_key = {
        (
            str(row["candidate_nk"]),
            _normalize_date_value(
                row["reference_trade_date"],
                field_name="queue.reference_trade_date",
            ),
        ): row
        for row in queue_rows
    }
    queue_rows_by_date: dict[date, list[dict[str, object]]] = defaultdict(list)
    for row in queue_rows:
        queue_rows_by_date[
            _normalize_date_value(
                row["reference_trade_date"],
                field_name="queue.reference_trade_date",
            )
        ].append(row)

    rows_by_trade_date: dict[date, list[_PositionBridgeRow]] = defaultdict(list)
    for bridge_row in bridge_rows:
        rows_by_trade_date[bridge_row.reference_trade_date].append(bridge_row)

    for reference_trade_date in sorted(rows_by_trade_date):
        date_rows = sorted(rows_by_trade_date[reference_trade_date], key=_decision_sort_key)
        date_queue_rows = queue_rows_by_date.get(reference_trade_date, [])
        try:
            _delete_stale_date_scope_rows(
                connection,
                portfolio_id=portfolio_id,
                reference_trade_date=reference_trade_date,
                current_candidate_nks={row.candidate_nk for row in date_rows},
                portfolio_plan_contract_version=portfolio_plan_contract_version,
            )
            capacity_snapshot_nk = _build_capacity_snapshot_nk(
                portfolio_id=portfolio_id,
                capacity_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
                reference_trade_date=reference_trade_date,
                portfolio_plan_contract_version=portfolio_plan_contract_version,
            )
            used_weight = 0.0
            remaining_weight = portfolio_gross_cap_weight
            date_requested_candidate_count = len(date_rows)
            date_admitted_count = 0
            date_blocked_count = 0
            date_trimmed_count = 0
            date_deferred_count = 0
            date_requested_total_weight = 0.0
            date_admitted_total_weight = 0.0
            date_trimmed_total_weight = 0.0
            date_blocked_total_weight = 0.0
            date_deferred_total_weight = 0.0
            candidate_bundles: list[_CandidateLedgerBundle] = []
            for decision_rank, bridge_row in enumerate(date_rows, start=1):
                evaluation = _evaluate_candidate_decision(
                    bridge_row=bridge_row,
                    portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                    portfolio_gross_used_weight=used_weight,
                    portfolio_gross_remaining_weight=remaining_weight,
                )
                bundle = _build_candidate_bundle(
                    run_id=run_id,
                    bridge_row=bridge_row,
                    evaluation=evaluation,
                    portfolio_id=portfolio_id,
                    portfolio_plan_contract_version=portfolio_plan_contract_version,
                    capacity_snapshot_nk=capacity_snapshot_nk,
                    portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                    decision_rank=decision_rank,
                )
                candidate_bundles.append(bundle)
                used_weight = evaluation.next_used_weight
                remaining_weight = evaluation.next_remaining_weight
                counts["requested_total_weight"] += evaluation.requested_weight
                counts["admitted_total_weight"] += evaluation.admitted_weight
                counts["trimmed_total_weight"] += evaluation.trimmed_weight
                date_requested_total_weight += evaluation.requested_weight
                date_admitted_total_weight += evaluation.admitted_weight
                date_trimmed_total_weight += evaluation.trimmed_weight
                counts["decision_reason_counts"][evaluation.decision_reason_code] += 1
                counts["trade_readiness_counts"][evaluation.trade_readiness_status] += 1
                if evaluation.decision_status == "admitted":
                    counts["admitted_count"] += 1
                    date_admitted_count += 1
                elif evaluation.decision_status == "trimmed":
                    counts["trimmed_count"] += 1
                    date_trimmed_count += 1
                elif evaluation.decision_status == "deferred":
                    counts["deferred_count"] += 1
                    date_deferred_count += 1
                    counts["deferred_total_weight"] += evaluation.requested_weight
                    date_deferred_total_weight += evaluation.requested_weight
                else:
                    counts["blocked_count"] += 1
                    date_blocked_count += 1
                    counts["blocked_total_weight"] += evaluation.requested_weight
                    date_blocked_total_weight += evaluation.requested_weight

            capacity_row = _PortfolioCapacitySnapshotRow(
                capacity_snapshot_nk=capacity_snapshot_nk,
                portfolio_id=portfolio_id,
                capacity_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
                reference_trade_date=reference_trade_date,
                portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                portfolio_gross_used_weight=used_weight,
                portfolio_gross_remaining_weight=remaining_weight,
                requested_candidate_count=date_requested_candidate_count,
                admitted_candidate_count=date_admitted_count,
                blocked_candidate_count=date_blocked_count,
                trimmed_candidate_count=date_trimmed_count,
                deferred_candidate_count=date_deferred_count,
                requested_total_weight=date_requested_total_weight,
                admitted_total_weight=date_admitted_total_weight,
                trimmed_total_weight=date_trimmed_total_weight,
                blocked_total_weight=date_blocked_total_weight,
                deferred_total_weight=date_deferred_total_weight,
                binding_constraint_code=_resolve_binding_constraint_code(candidate_bundles),
                capacity_decision_reason_code=_resolve_capacity_decision_reason_code(
                    candidate_bundles
                ),
                capacity_reason_summary_json=_build_capacity_reason_summary(candidate_bundles),
                portfolio_plan_contract_version=portfolio_plan_contract_version,
                first_seen_run_id=run_id,
                last_materialized_run_id=run_id,
            )
            capacity_action = _upsert_materialized_row(
                connection,
                table_name=PORTFOLIO_PLAN_CAPACITY_SNAPSHOT_TABLE,
                key_column="capacity_snapshot_nk",
                key_value=capacity_row.capacity_snapshot_nk,
                insert_payload=asdict(capacity_row),
                compare_columns=(
                    "portfolio_id",
                    "capacity_scope",
                    "reference_trade_date",
                    "portfolio_gross_cap_weight",
                    "portfolio_gross_used_weight",
                    "portfolio_gross_remaining_weight",
                    "requested_candidate_count",
                    "admitted_candidate_count",
                    "blocked_candidate_count",
                    "trimmed_candidate_count",
                    "deferred_candidate_count",
                    "requested_total_weight",
                    "admitted_total_weight",
                    "trimmed_total_weight",
                    "blocked_total_weight",
                    "deferred_total_weight",
                    "binding_constraint_code",
                    "capacity_decision_reason_code",
                    "capacity_reason_summary_json",
                    "portfolio_plan_contract_version",
                ),
            )

            for bridge_row, bundle in zip(date_rows, candidate_bundles, strict=True):
                decision_action = _upsert_materialized_row(
                    connection,
                    table_name=PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE,
                    key_column="candidate_decision_nk",
                    key_value=bundle.decision_row.candidate_decision_nk,
                    insert_payload=asdict(bundle.decision_row),
                    compare_columns=(
                        "candidate_nk",
                        "portfolio_id",
                        "instrument",
                        "policy_id",
                        "reference_trade_date",
                        "position_action_decision",
                        "decision_status",
                        "decision_reason_code",
                        "blocking_reason_code",
                        "decision_rank",
                        "decision_order_code",
                        "source_candidate_status",
                        "source_blocked_reason_code",
                        "source_binding_cap_code",
                        "source_capacity_source_code",
                        "source_required_reduction_weight",
                        "source_remaining_single_name_capacity_weight",
                        "source_remaining_portfolio_capacity_weight",
                        "capacity_before_weight",
                        "capacity_after_weight",
                        "trade_readiness_status",
                        "schedule_stage",
                        "schedule_lag_days",
                        "requested_weight",
                        "admitted_weight",
                        "trimmed_weight",
                        "capacity_snapshot_nk",
                        "portfolio_plan_contract_version",
                    ),
                )
                allocation_action = _upsert_materialized_row(
                    connection,
                    table_name=PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE,
                    key_column="allocation_snapshot_nk",
                    key_value=bundle.allocation_row.allocation_snapshot_nk,
                    insert_payload=asdict(bundle.allocation_row),
                    compare_columns=(
                        "candidate_nk",
                        "portfolio_id",
                        "instrument",
                        "allocation_scene",
                        "reference_trade_date",
                        "requested_weight",
                        "admitted_weight",
                        "trimmed_weight",
                        "final_allocated_weight",
                        "plan_status",
                        "decision_reason_code",
                        "blocking_reason_code",
                        "decision_rank",
                        "decision_order_code",
                        "trade_readiness_status",
                        "schedule_stage",
                        "schedule_lag_days",
                        "source_binding_cap_code",
                        "candidate_decision_nk",
                        "capacity_snapshot_nk",
                        "portfolio_plan_contract_version",
                    ),
                )
                snapshot_action = _upsert_materialized_row(
                    connection,
                    table_name=PORTFOLIO_PLAN_SNAPSHOT_TABLE,
                    key_column="plan_snapshot_nk",
                    key_value=bundle.snapshot_row.plan_snapshot_nk,
                    insert_payload=asdict(bundle.snapshot_row),
                    compare_columns=(
                        "candidate_nk",
                        "portfolio_id",
                        "instrument",
                        "reference_trade_date",
                        "position_action_decision",
                        "requested_weight",
                        "admitted_weight",
                        "trimmed_weight",
                        "plan_status",
                        "decision_reason_code",
                        "blocking_reason_code",
                        "decision_rank",
                        "decision_order_code",
                        "trade_readiness_status",
                        "schedule_stage",
                        "schedule_lag_days",
                        "source_binding_cap_code",
                        "portfolio_gross_cap_weight",
                        "portfolio_gross_used_weight",
                        "portfolio_gross_remaining_weight",
                        "candidate_decision_nk",
                        "capacity_snapshot_nk",
                        "allocation_snapshot_nk",
                        "portfolio_plan_contract_version",
                    ),
                )
                materialization_action = _aggregate_materialization_action(
                    capacity_action,
                    decision_action,
                    allocation_action,
                    snapshot_action,
                )
                queue_row = queue_rows_by_key.get(
                    (bridge_row.candidate_nk, bridge_row.reference_trade_date)
                )
                connection.execute(
                    f"""
                    INSERT OR REPLACE INTO {PORTFOLIO_PLAN_RUN_SNAPSHOT_TABLE} (
                        run_id,
                        plan_snapshot_nk,
                        candidate_decision_nk,
                        capacity_snapshot_nk,
                        allocation_snapshot_nk,
                        candidate_nk,
                        plan_status,
                        decision_reason_code,
                        trade_readiness_status,
                        queue_nk,
                        queue_reason,
                        materialization_action
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        bundle.snapshot_row.plan_snapshot_nk,
                        bundle.decision_row.candidate_decision_nk,
                        bundle.decision_row.capacity_snapshot_nk,
                        bundle.allocation_row.allocation_snapshot_nk,
                        bundle.decision_row.candidate_nk,
                        bundle.decision_row.decision_status,
                        bundle.decision_row.decision_reason_code,
                        bundle.decision_row.trade_readiness_status,
                        None if queue_row is None else str(queue_row["queue_nk"]),
                        None if queue_row is None else str(queue_row["queue_reason"]),
                        materialization_action,
                    ],
                )
                counts["candidate_count"] += 1
                counts[f"{materialization_action}_count"] += 1
                candidate_checkpoint_payload = {
                    "candidate_nk": bridge_row.candidate_nk,
                    "instrument": bridge_row.instrument,
                    "reference_trade_date": bridge_row.reference_trade_date.isoformat(),
                    "source_fingerprint": _build_source_fingerprint(
                        bridge_row=bridge_row,
                        portfolio_gross_cap_weight=portfolio_gross_cap_weight,
                        portfolio_plan_contract_version=portfolio_plan_contract_version,
                    ),
                }
                _upsert_portfolio_plan_checkpoint(
                    connection,
                    checkpoint_nk=_build_candidate_checkpoint_nk(
                        portfolio_id=portfolio_id,
                        candidate_nk=bridge_row.candidate_nk,
                    ),
                    portfolio_id=portfolio_id,
                    checkpoint_scope=DEFAULT_CANDIDATE_CHECKPOINT_SCOPE,
                    last_completed_reference_trade_date=bridge_row.reference_trade_date,
                    last_completed_candidate_nk=bridge_row.candidate_nk,
                    last_run_id=run_id,
                    checkpoint_payload=candidate_checkpoint_payload,
                )
                counts["checkpoint_upserted_count"] += 1

            portfolio_checkpoint_payload = {
                "capacity_snapshot_nk": capacity_snapshot_nk,
                "candidate_count": len(date_rows),
                "last_completed_reference_trade_date": reference_trade_date.isoformat(),
            }
            _upsert_portfolio_plan_checkpoint(
                connection,
                checkpoint_nk=_build_portfolio_checkpoint_nk(
                    portfolio_id=portfolio_id,
                    checkpoint_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
                ),
                portfolio_id=portfolio_id,
                checkpoint_scope=DEFAULT_PORTFOLIO_CAPACITY_SCOPE,
                last_completed_reference_trade_date=reference_trade_date,
                last_completed_candidate_nk=(
                    None if not date_rows else date_rows[-1].candidate_nk
                ),
                last_run_id=run_id,
                checkpoint_payload=portfolio_checkpoint_payload,
            )
            counts["checkpoint_upserted_count"] += 1
            if date_queue_rows:
                _mark_queue_rows_completed(
                    connection,
                    queue_nks=[str(row["queue_nk"]) for row in date_queue_rows],
                    run_id=run_id,
                )
            counts["latest_used_weight"] = used_weight
            counts["latest_remaining_weight"] = remaining_weight
        except Exception:
            if date_queue_rows:
                _mark_queue_rows_failed(
                    connection,
                    queue_nks=[str(row["queue_nk"]) for row in date_queue_rows],
                    run_id=run_id,
                    error_text=f"date_scope_failed:{reference_trade_date.isoformat()}",
                )
            raise
    return counts


def _delete_stale_date_scope_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    portfolio_id: str,
    reference_trade_date: date,
    current_candidate_nks: set[str],
    portfolio_plan_contract_version: str,
) -> None:
    rows = connection.execute(
        f"""
        SELECT candidate_nk
        FROM {PORTFOLIO_PLAN_SNAPSHOT_TABLE}
        WHERE portfolio_id = ?
          AND reference_trade_date = ?
          AND portfolio_plan_contract_version = ?
        """,
        [portfolio_id, reference_trade_date, portfolio_plan_contract_version],
    ).fetchall()
    stale_candidate_nks = sorted(
        {
            str(row[0])
            for row in rows
            if str(row[0]) not in current_candidate_nks
        }
    )
    if not stale_candidate_nks:
        return
    placeholders = ", ".join("?" for _ in stale_candidate_nks)
    parameters = [
        portfolio_id,
        reference_trade_date,
        portfolio_plan_contract_version,
        *stale_candidate_nks,
    ]
    for table_name in (
        PORTFOLIO_PLAN_CANDIDATE_DECISION_TABLE,
        PORTFOLIO_PLAN_ALLOCATION_SNAPSHOT_TABLE,
        PORTFOLIO_PLAN_SNAPSHOT_TABLE,
    ):
        connection.execute(
            f"""
            DELETE FROM {table_name}
            WHERE portfolio_id = ?
              AND reference_trade_date = ?
              AND portfolio_plan_contract_version = ?
              AND candidate_nk IN ({placeholders})
            """,
            parameters,
        )


def _upsert_portfolio_plan_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    checkpoint_nk: str,
    portfolio_id: str,
    checkpoint_scope: str,
    last_completed_reference_trade_date: date | None,
    last_completed_candidate_nk: str | None,
    last_run_id: str,
    checkpoint_payload: dict[str, object],
) -> None:
    existing = connection.execute(
        f"SELECT checkpoint_nk FROM {PORTFOLIO_PLAN_CHECKPOINT_TABLE} WHERE checkpoint_nk = ?",
        [checkpoint_nk],
    ).fetchone()
    payload_json = json.dumps(checkpoint_payload, ensure_ascii=False, sort_keys=True)
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {PORTFOLIO_PLAN_CHECKPOINT_TABLE} (
                checkpoint_nk,
                portfolio_id,
                checkpoint_scope,
                latest_reference_trade_date,
                last_candidate_nk,
                last_completed_reference_trade_date,
                last_completed_candidate_nk,
                last_success_run_id,
                last_run_id,
                checkpoint_payload_json,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                checkpoint_nk,
                portfolio_id,
                checkpoint_scope,
                last_completed_reference_trade_date,
                last_completed_candidate_nk,
                last_completed_reference_trade_date,
                last_completed_candidate_nk,
                last_run_id,
                last_run_id,
                payload_json,
            ],
        )
        return
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_CHECKPOINT_TABLE}
        SET
            portfolio_id = ?,
            checkpoint_scope = ?,
            latest_reference_trade_date = ?,
            last_candidate_nk = ?,
            last_completed_reference_trade_date = ?,
            last_completed_candidate_nk = ?,
            last_success_run_id = ?,
            last_run_id = ?,
            checkpoint_payload_json = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE checkpoint_nk = ?
        """,
        [
            portfolio_id,
            checkpoint_scope,
            last_completed_reference_trade_date,
            last_completed_candidate_nk,
            last_completed_reference_trade_date,
            last_completed_candidate_nk,
            last_run_id,
            last_run_id,
            payload_json,
            checkpoint_nk,
        ],
    )


def _load_latest_reference_trade_date(
    connection: duckdb.DuckDBPyConnection,
    *,
    portfolio_id: str,
) -> date | None:
    row = connection.execute(
        f"""
        SELECT MAX(reference_trade_date)
        FROM {PORTFOLIO_PLAN_SNAPSHOT_TABLE}
        WHERE portfolio_id = ?
        """,
        [portfolio_id],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return _normalize_date_value(row[0], field_name="snapshot.reference_trade_date")


def _derive_freshness_status(
    *,
    latest_reference_trade_date: date | None,
    expected_reference_trade_date: date | None,
) -> str:
    if latest_reference_trade_date is None and expected_reference_trade_date is None:
        return "no_source_data"
    if expected_reference_trade_date is None:
        return "fresh"
    if latest_reference_trade_date is None:
        return "stale"
    if latest_reference_trade_date < expected_reference_trade_date:
        return "stale"
    return "fresh"


def _upsert_portfolio_plan_freshness_audit(
    connection: duckdb.DuckDBPyConnection,
    *,
    portfolio_id: str,
    expected_reference_trade_date: date | None,
    last_success_run_id: str,
) -> tuple[date | None, date | None, str]:
    latest_reference_trade_date = _load_latest_reference_trade_date(
        connection,
        portfolio_id=portfolio_id,
    )
    freshness_status = _derive_freshness_status(
        latest_reference_trade_date=latest_reference_trade_date,
        expected_reference_trade_date=expected_reference_trade_date,
    )
    audit_date = datetime.now().date()
    existing = connection.execute(
        f"SELECT portfolio_id FROM {PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE} WHERE portfolio_id = ?",
        [portfolio_id],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE} (
                portfolio_id,
                audit_date,
                latest_reference_trade_date,
                expected_reference_trade_date,
                freshness_status,
                last_success_run_id,
                last_run_id,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                portfolio_id,
                audit_date,
                latest_reference_trade_date,
                expected_reference_trade_date,
                freshness_status,
                last_success_run_id,
                last_success_run_id,
            ],
        )
    else:
        connection.execute(
            f"""
            UPDATE {PORTFOLIO_PLAN_FRESHNESS_AUDIT_TABLE}
            SET
                audit_date = ?,
                latest_reference_trade_date = ?,
                expected_reference_trade_date = ?,
                freshness_status = ?,
                last_success_run_id = ?,
                last_run_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE portfolio_id = ?
            """,
            [
                audit_date,
                latest_reference_trade_date,
                expected_reference_trade_date,
                freshness_status,
                last_success_run_id,
                last_success_run_id,
                portfolio_id,
            ],
        )
    return latest_reference_trade_date, expected_reference_trade_date, freshness_status


def _max_reference_trade_date(bridge_rows: list[_PositionBridgeRow]) -> date | None:
    if not bridge_rows:
        return None
    return max(row.reference_trade_date for row in bridge_rows)


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: PortfolioPlanBuildSummary,
) -> None:
    _update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        summary=summary,
    )


def _mark_run_failed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_RUN_TABLE}
        SET
            run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            json.dumps({"run_status": "failed"}, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def _update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    summary: PortfolioPlanBuildSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {PORTFOLIO_PLAN_RUN_TABLE}
        SET
            run_status = ?,
            admitted_count = ?,
            blocked_count = ?,
            trimmed_count = ?,
            deferred_count = ?,
            inserted_count = ?,
            reused_count = ?,
            rematerialized_count = ?,
            queue_enqueued_count = ?,
            queue_claimed_count = ?,
            checkpoint_upserted_count = ?,
            freshness_updated_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            summary.admitted_count,
            summary.blocked_count,
            summary.trimmed_count,
            summary.deferred_count,
            summary.inserted_count,
            summary.reused_count,
            summary.rematerialized_count,
            summary.queue_enqueued_count,
            summary.queue_claimed_count,
            summary.checkpoint_upserted_count,
            summary.freshness_updated_count,
            json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def _normalize_date_value(value: object, *, field_name: str) -> date:
    if value is None:
        raise ValueError(f"Missing required date field: {field_name}")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_optional_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _write_summary(summary: PortfolioPlanBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
