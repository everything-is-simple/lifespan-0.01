"""执行 `filter snapshot` 官方 producer 的最小 bounded 运行时。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.malf.bootstrap import MALF_STATE_SNAPSHOT_TABLE
from mlq.filter.bootstrap import (
    FILTER_RUN_SNAPSHOT_TABLE,
    FILTER_RUN_TABLE,
    FILTER_SNAPSHOT_TABLE,
    bootstrap_filter_snapshot_ledger,
    filter_ledger_path,
)


DEFAULT_FILTER_STRUCTURE_TABLE: Final[str] = "structure_snapshot"
DEFAULT_FILTER_CONTEXT_TABLE: Final[str] = MALF_STATE_SNAPSHOT_TABLE
DEFAULT_FILTER_SOURCE_TIMEFRAME: Final[str] = "D"
DEFAULT_FILTER_CONTRACT_VERSION: Final[str] = "filter-snapshot-v2"


@dataclass(frozen=True)
class FilterSnapshotBuildSummary:
    """总结一次 `filter snapshot` producer 的 bounded 运行结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    filter_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    candidate_structure_count: int
    materialized_snapshot_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    missing_context_count: int
    admissible_count: int
    blocked_count: int
    filter_ledger_path: str
    structure_ledger_path: str
    malf_ledger_path: str
    source_structure_table: str
    source_context_table: str
    source_timeframe: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _StructureSnapshotInputRow:
    structure_snapshot_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    daily_major_state: str | None
    daily_trend_direction: str | None
    daily_reversal_stage: str | None
    daily_wave_id: int | None
    daily_current_hh_count: int | None
    daily_current_ll_count: int | None
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_wave_id: int | None
    weekly_current_hh_count: int | None
    weekly_current_ll_count: int | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_wave_id: int | None
    monthly_current_hh_count: int | None
    monthly_current_ll_count: int | None
    monthly_source_context_nk: str | None
    structure_progress_state: str
    break_confirmation_status: str | None
    break_confirmation_ref: str | None
    stats_snapshot_nk: str | None
    exhaustion_risk_bucket: str | None
    reversal_probability_bucket: str | None
    source_context_nk: str


@dataclass(frozen=True)
class _FilterSnapshotRow:
    filter_snapshot_nk: str
    structure_snapshot_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    major_state: str
    trend_direction: str
    reversal_stage: str
    wave_id: int
    current_hh_count: int
    current_ll_count: int
    daily_major_state: str | None
    daily_trend_direction: str | None
    daily_reversal_stage: str | None
    daily_wave_id: int | None
    daily_current_hh_count: int | None
    daily_current_ll_count: int | None
    daily_source_context_nk: str | None
    weekly_major_state: str | None
    weekly_trend_direction: str | None
    weekly_reversal_stage: str | None
    weekly_wave_id: int | None
    weekly_current_hh_count: int | None
    weekly_current_ll_count: int | None
    weekly_source_context_nk: str | None
    monthly_major_state: str | None
    monthly_trend_direction: str | None
    monthly_reversal_stage: str | None
    monthly_wave_id: int | None
    monthly_current_hh_count: int | None
    monthly_current_ll_count: int | None
    monthly_source_context_nk: str | None
    trigger_admissible: bool
    primary_blocking_condition: str | None
    blocking_conditions_json: str
    admission_notes: str | None
    break_confirmation_status: str | None
    break_confirmation_ref: str | None
    stats_snapshot_nk: str | None
    exhaustion_risk_bucket: str | None
    reversal_probability_bucket: str | None
    source_context_nk: str
    filter_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


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
) -> FilterSnapshotBuildSummary:
    """从官方 `structure snapshot` 物化 `filter snapshot`。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_filter_path = Path(filter_path or filter_ledger_path(workspace))
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    resolved_malf_path = Path(malf_path or workspace.databases.malf)
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_timeframe = _normalize_timeframe(source_timeframe)
    materialization_run_id = run_id or _build_filter_run_id()

    _ensure_database_exists(resolved_structure_path, label="structure")
    actual_source_context_table = source_context_table
    structure_rows = _load_structure_snapshot_rows(
        structure_path=resolved_structure_path,
        table_name=source_structure_table,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        limit=normalized_limit,
    )
    context_presence = _load_context_presence(
        malf_path=resolved_malf_path,
        table_name=actual_source_context_table,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=tuple(sorted({row.instrument for row in structure_rows})),
        timeframe=normalized_timeframe,
    )

    filter_connection = duckdb.connect(str(resolved_filter_path))
    try:
        bootstrap_filter_snapshot_ledger(workspace, connection=filter_connection)
        bounded_instrument_count = len({row.instrument for row in structure_rows})
        _insert_run_row(
            filter_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=bounded_instrument_count,
            source_structure_table=source_structure_table,
            source_context_table=actual_source_context_table,
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
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            malf_path=resolved_malf_path,
            source_structure_table=source_structure_table,
            source_context_table=actual_source_context_table,
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


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_timeframe(value: str | None) -> str:
    candidate = str(value or DEFAULT_FILTER_SOURCE_TIMEFRAME).strip().upper()
    return candidate or DEFAULT_FILTER_SOURCE_TIMEFRAME


def _build_filter_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"filter-snapshot-{timestamp}"


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _database_table_exists(path: Path, table_name: str | None) -> bool:
    if table_name is None or table_name == "" or not path.exists():
        return False
    connection = duckdb.connect(str(path), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            LIMIT 1
            """,
            [table_name],
        ).fetchall()
        return bool(rows)
    finally:
        connection.close()


def _load_structure_snapshot_rows(
    *,
    structure_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_StructureSnapshotInputRow]:
    connection = duckdb.connect(str(structure_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument",),
            field_name="instrument",
            table_name=table_name,
        )
        signal_date_column = _resolve_existing_column(
            available_columns,
            ("signal_date",),
            field_name="signal_date",
            table_name=table_name,
        )
        parameters: list[object] = []
        where_clauses: list[str] = []
        if signal_start_date is not None:
            where_clauses.append(f"{signal_date_column} >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append(f"{signal_date_column} <= ?")
            parameters.append(signal_end_date)
        if instruments:
            placeholders = ", ".join("?" for _ in instruments)
            where_clauses.append(f"{instrument_column} IN ({placeholders})")
            parameters.extend(instruments)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        rows = connection.execute(
            f"""
            SELECT
                {_resolve_existing_column(available_columns, ("structure_snapshot_nk",), field_name="structure_snapshot_nk", table_name=table_name)} AS structure_snapshot_nk,
                {instrument_column} AS instrument,
                {signal_date_column} AS signal_date,
                {_resolve_optional_column(available_columns, ("asof_date",)) or signal_date_column} AS asof_date,
                {_resolve_existing_column(available_columns, ("major_state",), field_name="major_state", table_name=table_name)} AS major_state,
                {_resolve_existing_column(available_columns, ("trend_direction",), field_name="trend_direction", table_name=table_name)} AS trend_direction,
                COALESCE({_resolve_optional_column(available_columns, ("reversal_stage",)) or "'none'"}, 'none') AS reversal_stage,
                COALESCE({_resolve_optional_column(available_columns, ("wave_id",)) or "0"}, 0) AS wave_id,
                COALESCE({_resolve_optional_column(available_columns, ("current_hh_count",)) or "0"}, 0) AS current_hh_count,
                COALESCE({_resolve_optional_column(available_columns, ("current_ll_count",)) or "0"}, 0) AS current_ll_count,
                {_resolve_optional_column(available_columns, ("daily_major_state",)) or "NULL"} AS daily_major_state,
                {_resolve_optional_column(available_columns, ("daily_trend_direction",)) or "NULL"} AS daily_trend_direction,
                {_resolve_optional_column(available_columns, ("daily_reversal_stage",)) or "NULL"} AS daily_reversal_stage,
                {_resolve_optional_column(available_columns, ("daily_wave_id",)) or "NULL"} AS daily_wave_id,
                {_resolve_optional_column(available_columns, ("daily_current_hh_count",)) or "NULL"} AS daily_current_hh_count,
                {_resolve_optional_column(available_columns, ("daily_current_ll_count",)) or "NULL"} AS daily_current_ll_count,
                {_resolve_optional_column(available_columns, ("daily_source_context_nk",)) or "NULL"} AS daily_source_context_nk,
                {_resolve_optional_column(available_columns, ("weekly_major_state",)) or "NULL"} AS weekly_major_state,
                {_resolve_optional_column(available_columns, ("weekly_trend_direction",)) or "NULL"} AS weekly_trend_direction,
                {_resolve_optional_column(available_columns, ("weekly_reversal_stage",)) or "NULL"} AS weekly_reversal_stage,
                {_resolve_optional_column(available_columns, ("weekly_wave_id",)) or "NULL"} AS weekly_wave_id,
                {_resolve_optional_column(available_columns, ("weekly_current_hh_count",)) or "NULL"} AS weekly_current_hh_count,
                {_resolve_optional_column(available_columns, ("weekly_current_ll_count",)) or "NULL"} AS weekly_current_ll_count,
                {_resolve_optional_column(available_columns, ("weekly_source_context_nk",)) or "NULL"} AS weekly_source_context_nk,
                {_resolve_optional_column(available_columns, ("monthly_major_state",)) or "NULL"} AS monthly_major_state,
                {_resolve_optional_column(available_columns, ("monthly_trend_direction",)) or "NULL"} AS monthly_trend_direction,
                {_resolve_optional_column(available_columns, ("monthly_reversal_stage",)) or "NULL"} AS monthly_reversal_stage,
                {_resolve_optional_column(available_columns, ("monthly_wave_id",)) or "NULL"} AS monthly_wave_id,
                {_resolve_optional_column(available_columns, ("monthly_current_hh_count",)) or "NULL"} AS monthly_current_hh_count,
                {_resolve_optional_column(available_columns, ("monthly_current_ll_count",)) or "NULL"} AS monthly_current_ll_count,
                {_resolve_optional_column(available_columns, ("monthly_source_context_nk",)) or "NULL"} AS monthly_source_context_nk,
                {_resolve_optional_column(available_columns, ("structure_progress_state",)) or "'unknown'"} AS structure_progress_state,
                {_resolve_optional_column(available_columns, ("break_confirmation_status",)) or "NULL"} AS break_confirmation_status,
                {_resolve_optional_column(available_columns, ("break_confirmation_ref",)) or "NULL"} AS break_confirmation_ref,
                {_resolve_optional_column(available_columns, ("stats_snapshot_nk",)) or "NULL"} AS stats_snapshot_nk,
                {_resolve_optional_column(available_columns, ("exhaustion_risk_bucket",)) or "NULL"} AS exhaustion_risk_bucket,
                {_resolve_optional_column(available_columns, ("reversal_probability_bucket",)) or "NULL"} AS reversal_probability_bucket,
                {_resolve_existing_column(available_columns, ("source_context_nk",), field_name="source_context_nk", table_name=table_name)} AS source_context_nk
            FROM {table_name}
            {where_sql}
            ORDER BY signal_date, instrument, structure_snapshot_nk
            LIMIT ?
            """,
            [*parameters, limit],
        ).fetchall()
        return [
            _StructureSnapshotInputRow(
                structure_snapshot_nk=str(row[0]),
                instrument=str(row[1]),
                signal_date=_normalize_date_value(row[2], field_name="signal_date"),
                asof_date=_normalize_date_value(row[3], field_name="asof_date"),
                major_state=_normalize_optional_str(row[4], default="??"),
                trend_direction=_normalize_optional_str(row[5], default="down").lower(),
                reversal_stage=_normalize_optional_str(row[6], default="none").lower(),
                wave_id=_normalize_optional_int(row[7]),
                current_hh_count=_normalize_optional_int(row[8]),
                current_ll_count=_normalize_optional_int(row[9]),
                daily_major_state=_normalize_optional_nullable_str(row[10]),
                daily_trend_direction=_normalize_optional_nullable_str(row[11]),
                daily_reversal_stage=_normalize_optional_nullable_str(row[12]),
                daily_wave_id=_normalize_optional_nullable_int(row[13]),
                daily_current_hh_count=_normalize_optional_nullable_int(row[14]),
                daily_current_ll_count=_normalize_optional_nullable_int(row[15]),
                daily_source_context_nk=_normalize_optional_nullable_str(row[16]),
                weekly_major_state=_normalize_optional_nullable_str(row[17]),
                weekly_trend_direction=_normalize_optional_nullable_str(row[18]),
                weekly_reversal_stage=_normalize_optional_nullable_str(row[19]),
                weekly_wave_id=_normalize_optional_nullable_int(row[20]),
                weekly_current_hh_count=_normalize_optional_nullable_int(row[21]),
                weekly_current_ll_count=_normalize_optional_nullable_int(row[22]),
                weekly_source_context_nk=_normalize_optional_nullable_str(row[23]),
                monthly_major_state=_normalize_optional_nullable_str(row[24]),
                monthly_trend_direction=_normalize_optional_nullable_str(row[25]),
                monthly_reversal_stage=_normalize_optional_nullable_str(row[26]),
                monthly_wave_id=_normalize_optional_nullable_int(row[27]),
                monthly_current_hh_count=_normalize_optional_nullable_int(row[28]),
                monthly_current_ll_count=_normalize_optional_nullable_int(row[29]),
                monthly_source_context_nk=_normalize_optional_nullable_str(row[30]),
                structure_progress_state=_normalize_progress_state(row[31]),
                break_confirmation_status=_normalize_optional_nullable_str(row[32]),
                break_confirmation_ref=_normalize_optional_nullable_str(row[33]),
                stats_snapshot_nk=_normalize_optional_nullable_str(row[34]),
                exhaustion_risk_bucket=_normalize_optional_nullable_str(row[35]),
                reversal_probability_bucket=_normalize_optional_nullable_str(row[36]),
                source_context_nk=str(row[37]),
            )
            for row in rows
        ]
    finally:
        connection.close()


def _load_context_presence(
    *,
    malf_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> set[tuple[str, date]]:
    if not malf_path.exists() or not instruments:
        return set()
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument", "entity_code", "code"),
            field_name="instrument",
            table_name=table_name,
        )
        signal_date_column = _resolve_existing_column(
            available_columns,
            ("signal_date", "asof_bar_dt"),
            field_name="signal_date/asof_bar_dt",
            table_name=table_name,
        )
        timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
        asset_type_column = _resolve_optional_column(available_columns, ("asset_type",))
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"{instrument_column} IN ({placeholders})"]
        if signal_start_date is not None:
            where_clauses.append(f"{signal_date_column} >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append(f"{signal_date_column} <= ?")
            parameters.append(signal_end_date)
        if timeframe_column is not None:
            where_clauses.append(f"{timeframe_column} = ?")
            parameters.append(timeframe)
        if asset_type_column is not None:
            where_clauses.append(f"{asset_type_column} = ?")
            parameters.append("stock")
        rows = connection.execute(
            f"""
            SELECT DISTINCT
                {instrument_column} AS instrument,
                {signal_date_column} AS signal_date
            FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            """,
            parameters,
        ).fetchall()
        return {
            (str(row[0]), _normalize_date_value(row[1], field_name="signal_date"))
            for row in rows
        }
    except ValueError:
        return set()
    finally:
        connection.close()


def _materialize_filter_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    structure_rows: list[_StructureSnapshotInputRow],
    context_presence: set[tuple[str, date]],
    filter_contract_version: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    filter_path: Path,
    structure_path: Path,
    malf_path: Path,
    source_structure_table: str,
    source_context_table: str,
    source_timeframe: str,
    batch_size: int,
) -> FilterSnapshotBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    missing_context_count = 0
    admissible_count = 0
    blocked_count = 0

    for structure_batch in _bounded_by_instrument_batches(structure_rows, batch_size=batch_size):
        for structure_row in structure_batch:
            has_context = (structure_row.instrument, structure_row.signal_date) in context_presence
            if not has_context:
                missing_context_count += 1
            filter_row = _build_filter_snapshot_row(
                run_id=run_id,
                structure_row=structure_row,
                has_context=has_context,
                filter_contract_version=filter_contract_version,
            )
            materialization_action = _upsert_filter_snapshot(connection, filter_row=filter_row)
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {FILTER_RUN_SNAPSHOT_TABLE} (
                    run_id,
                    filter_snapshot_nk,
                    materialization_action,
                    trigger_admissible,
                    primary_blocking_condition
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    filter_row.filter_snapshot_nk,
                    materialization_action,
                    filter_row.trigger_admissible,
                    filter_row.primary_blocking_condition,
                ],
            )
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

            if filter_row.trigger_admissible:
                admissible_count += 1
            else:
                blocked_count += 1

    materialized_snapshot_count = inserted_count + reused_count + rematerialized_count
    return FilterSnapshotBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        filter_contract_version=filter_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in structure_rows}),
        candidate_structure_count=len(structure_rows),
        materialized_snapshot_count=materialized_snapshot_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        missing_context_count=missing_context_count,
        admissible_count=admissible_count,
        blocked_count=blocked_count,
        filter_ledger_path=str(filter_path),
        structure_ledger_path=str(structure_path),
        malf_ledger_path=str(malf_path),
        source_structure_table=source_structure_table,
        source_context_table=source_context_table,
        source_timeframe=source_timeframe,
    )


def _bounded_by_instrument_batches(
    structure_rows: list[_StructureSnapshotInputRow],
    *,
    batch_size: int,
) -> list[list[_StructureSnapshotInputRow]]:
    if not structure_rows:
        return []
    normalized_batch_size = max(int(batch_size), 1)
    batches: list[list[_StructureSnapshotInputRow]] = []
    current_batch: list[_StructureSnapshotInputRow] = []
    current_instruments: set[str] = set()
    for row in structure_rows:
        if current_batch and row.instrument not in current_instruments and len(current_instruments) >= normalized_batch_size:
            batches.append(current_batch)
            current_batch = []
            current_instruments = set()
        current_batch.append(row)
        current_instruments.add(row.instrument)
    if current_batch:
        batches.append(current_batch)
    return batches


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    source_structure_table: str,
    source_context_table: str,
    filter_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {FILTER_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_structure_table,
            source_context_table,
            filter_contract_version
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
            source_structure_table,
            source_context_table,
            filter_contract_version,
        ],
    )


def _build_filter_snapshot_row(
    *,
    run_id: str,
    structure_row: _StructureSnapshotInputRow,
    has_context: bool,
    filter_contract_version: str,
) -> _FilterSnapshotRow:
    blocking_conditions: list[str] = []
    if structure_row.structure_progress_state == "failed":
        blocking_conditions.append("structure_progress_failed")
    elif structure_row.reversal_stage in {"trigger", "hold"} and structure_row.trend_direction == "down":
        blocking_conditions.append("reversal_stage_pending")

    admission_notes: list[str] = []
    if structure_row.structure_progress_state in {"stalled", "unknown"} and not blocking_conditions:
        admission_notes.append(f"???????????:{structure_row.structure_progress_state}")
    admission_notes.append(
        f"canonical_context={structure_row.major_state}/{structure_row.trend_direction}/{structure_row.reversal_stage}"
    )
    if structure_row.weekly_major_state or structure_row.monthly_major_state:
        admission_notes.append(
            "read_only_context="
            f"W:{structure_row.weekly_major_state}/{structure_row.weekly_reversal_stage};"
            f"M:{structure_row.monthly_major_state}/{structure_row.monthly_reversal_stage}"
        )
    if not has_context:
        admission_notes.append("?? execution_context????????????")
    if structure_row.break_confirmation_status == "confirmed":
        admission_notes.append("break_confirmation=confirmed 仅 sidecar 提示")
    if structure_row.exhaustion_risk_bucket in {"elevated", "high"}:
        admission_notes.append(f"exhaustion_risk={structure_row.exhaustion_risk_bucket}")

    primary_blocking_condition = blocking_conditions[0] if blocking_conditions else None
    blocking_conditions_json = json.dumps(blocking_conditions, ensure_ascii=False, sort_keys=True)
    filter_snapshot_nk = _build_filter_snapshot_nk(
        structure_snapshot_nk=structure_row.structure_snapshot_nk,
        source_context_nk=structure_row.source_context_nk,
        filter_contract_version=filter_contract_version,
    )
    return _FilterSnapshotRow(
        filter_snapshot_nk=filter_snapshot_nk,
        structure_snapshot_nk=structure_row.structure_snapshot_nk,
        instrument=structure_row.instrument,
        signal_date=structure_row.signal_date,
        asof_date=structure_row.asof_date,
        major_state=structure_row.major_state,
        trend_direction=structure_row.trend_direction,
        reversal_stage=structure_row.reversal_stage,
        wave_id=structure_row.wave_id,
        current_hh_count=structure_row.current_hh_count,
        current_ll_count=structure_row.current_ll_count,
        daily_major_state=structure_row.daily_major_state,
        daily_trend_direction=structure_row.daily_trend_direction,
        daily_reversal_stage=structure_row.daily_reversal_stage,
        daily_wave_id=structure_row.daily_wave_id,
        daily_current_hh_count=structure_row.daily_current_hh_count,
        daily_current_ll_count=structure_row.daily_current_ll_count,
        daily_source_context_nk=structure_row.daily_source_context_nk,
        weekly_major_state=structure_row.weekly_major_state,
        weekly_trend_direction=structure_row.weekly_trend_direction,
        weekly_reversal_stage=structure_row.weekly_reversal_stage,
        weekly_wave_id=structure_row.weekly_wave_id,
        weekly_current_hh_count=structure_row.weekly_current_hh_count,
        weekly_current_ll_count=structure_row.weekly_current_ll_count,
        weekly_source_context_nk=structure_row.weekly_source_context_nk,
        monthly_major_state=structure_row.monthly_major_state,
        monthly_trend_direction=structure_row.monthly_trend_direction,
        monthly_reversal_stage=structure_row.monthly_reversal_stage,
        monthly_wave_id=structure_row.monthly_wave_id,
        monthly_current_hh_count=structure_row.monthly_current_hh_count,
        monthly_current_ll_count=structure_row.monthly_current_ll_count,
        monthly_source_context_nk=structure_row.monthly_source_context_nk,
        trigger_admissible=not blocking_conditions,
        primary_blocking_condition=primary_blocking_condition,
        blocking_conditions_json=blocking_conditions_json,
        admission_notes="; ".join(admission_notes) if admission_notes else None,
        break_confirmation_status=structure_row.break_confirmation_status,
        break_confirmation_ref=structure_row.break_confirmation_ref,
        stats_snapshot_nk=structure_row.stats_snapshot_nk,
        exhaustion_risk_bucket=structure_row.exhaustion_risk_bucket,
        reversal_probability_bucket=structure_row.reversal_probability_bucket,
        source_context_nk=structure_row.source_context_nk,
        filter_contract_version=filter_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _build_filter_snapshot_nk(
    *,
    structure_snapshot_nk: str,
    source_context_nk: str,
    filter_contract_version: str,
) -> str:
    return "|".join(
        [
            structure_snapshot_nk,
            source_context_nk,
            filter_contract_version,
        ]
    )


def _upsert_filter_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    filter_row: _FilterSnapshotRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            major_state, trend_direction, reversal_stage, wave_id, current_hh_count, current_ll_count,
            daily_major_state, daily_trend_direction, daily_reversal_stage, daily_wave_id, daily_current_hh_count, daily_current_ll_count, daily_source_context_nk,
            weekly_major_state, weekly_trend_direction, weekly_reversal_stage, weekly_wave_id, weekly_current_hh_count, weekly_current_ll_count, weekly_source_context_nk,
            monthly_major_state, monthly_trend_direction, monthly_reversal_stage, monthly_wave_id, monthly_current_hh_count, monthly_current_ll_count, monthly_source_context_nk,
            trigger_admissible, primary_blocking_condition, blocking_conditions_json, admission_notes,
            break_confirmation_status, break_confirmation_ref, stats_snapshot_nk, exhaustion_risk_bucket, reversal_probability_bucket,
            first_seen_run_id
        FROM {FILTER_SNAPSHOT_TABLE}
        WHERE filter_snapshot_nk = ?
        """,
        [filter_row.filter_snapshot_nk],
    ).fetchone()
    new_fingerprint = (
        filter_row.major_state, filter_row.trend_direction, filter_row.reversal_stage, filter_row.wave_id, filter_row.current_hh_count, filter_row.current_ll_count,
        filter_row.daily_major_state, filter_row.daily_trend_direction, filter_row.daily_reversal_stage, filter_row.daily_wave_id, filter_row.daily_current_hh_count, filter_row.daily_current_ll_count, filter_row.daily_source_context_nk,
        filter_row.weekly_major_state, filter_row.weekly_trend_direction, filter_row.weekly_reversal_stage, filter_row.weekly_wave_id, filter_row.weekly_current_hh_count, filter_row.weekly_current_ll_count, filter_row.weekly_source_context_nk,
        filter_row.monthly_major_state, filter_row.monthly_trend_direction, filter_row.monthly_reversal_stage, filter_row.monthly_wave_id, filter_row.monthly_current_hh_count, filter_row.monthly_current_ll_count, filter_row.monthly_source_context_nk,
        filter_row.trigger_admissible, filter_row.primary_blocking_condition, filter_row.blocking_conditions_json, filter_row.admission_notes,
        filter_row.break_confirmation_status, filter_row.break_confirmation_ref, filter_row.stats_snapshot_nk, filter_row.exhaustion_risk_bucket, filter_row.reversal_probability_bucket,
    )
    insert_values = [
        filter_row.filter_snapshot_nk, filter_row.structure_snapshot_nk, filter_row.instrument, filter_row.signal_date, filter_row.asof_date,
        filter_row.major_state, filter_row.trend_direction, filter_row.reversal_stage, filter_row.wave_id, filter_row.current_hh_count, filter_row.current_ll_count,
        filter_row.daily_major_state, filter_row.daily_trend_direction, filter_row.daily_reversal_stage, filter_row.daily_wave_id, filter_row.daily_current_hh_count, filter_row.daily_current_ll_count, filter_row.daily_source_context_nk,
        filter_row.weekly_major_state, filter_row.weekly_trend_direction, filter_row.weekly_reversal_stage, filter_row.weekly_wave_id, filter_row.weekly_current_hh_count, filter_row.weekly_current_ll_count, filter_row.weekly_source_context_nk,
        filter_row.monthly_major_state, filter_row.monthly_trend_direction, filter_row.monthly_reversal_stage, filter_row.monthly_wave_id, filter_row.monthly_current_hh_count, filter_row.monthly_current_ll_count, filter_row.monthly_source_context_nk,
        filter_row.trigger_admissible, filter_row.primary_blocking_condition, filter_row.blocking_conditions_json, filter_row.admission_notes,
        filter_row.break_confirmation_status, filter_row.break_confirmation_ref, filter_row.stats_snapshot_nk, filter_row.exhaustion_risk_bucket, filter_row.reversal_probability_bucket,
        filter_row.source_context_nk, filter_row.filter_contract_version, filter_row.first_seen_run_id, filter_row.last_materialized_run_id,
    ]
    if existing_row is None:
        placeholders = ", ".join("?" for _ in insert_values)
        connection.execute(
            f"""
            INSERT INTO {FILTER_SNAPSHOT_TABLE} (
                filter_snapshot_nk, structure_snapshot_nk, instrument, signal_date, asof_date,
                major_state, trend_direction, reversal_stage, wave_id, current_hh_count, current_ll_count,
                daily_major_state, daily_trend_direction, daily_reversal_stage, daily_wave_id, daily_current_hh_count, daily_current_ll_count, daily_source_context_nk,
                weekly_major_state, weekly_trend_direction, weekly_reversal_stage, weekly_wave_id, weekly_current_hh_count, weekly_current_ll_count, weekly_source_context_nk,
                monthly_major_state, monthly_trend_direction, monthly_reversal_stage, monthly_wave_id, monthly_current_hh_count, monthly_current_ll_count, monthly_source_context_nk,
                trigger_admissible, primary_blocking_condition, blocking_conditions_json, admission_notes,
                break_confirmation_status, break_confirmation_ref, stats_snapshot_nk, exhaustion_risk_bucket, reversal_probability_bucket,
                source_context_nk, filter_contract_version, first_seen_run_id, last_materialized_run_id
            )
            VALUES ({placeholders})
            """,
            insert_values,
        )
        return "inserted"
    first_seen_run_id = str(existing_row[-1]) if existing_row[-1] is not None else filter_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {FILTER_SNAPSHOT_TABLE}
        SET
            major_state = ?, trend_direction = ?, reversal_stage = ?, wave_id = ?, current_hh_count = ?, current_ll_count = ?,
            daily_major_state = ?, daily_trend_direction = ?, daily_reversal_stage = ?, daily_wave_id = ?, daily_current_hh_count = ?, daily_current_ll_count = ?, daily_source_context_nk = ?,
            weekly_major_state = ?, weekly_trend_direction = ?, weekly_reversal_stage = ?, weekly_wave_id = ?, weekly_current_hh_count = ?, weekly_current_ll_count = ?, weekly_source_context_nk = ?,
            monthly_major_state = ?, monthly_trend_direction = ?, monthly_reversal_stage = ?, monthly_wave_id = ?, monthly_current_hh_count = ?, monthly_current_ll_count = ?, monthly_source_context_nk = ?,
            trigger_admissible = ?, primary_blocking_condition = ?, blocking_conditions_json = ?, admission_notes = ?,
            break_confirmation_status = ?, break_confirmation_ref = ?, stats_snapshot_nk = ?, exhaustion_risk_bucket = ?, reversal_probability_bucket = ?,
            first_seen_run_id = ?, last_materialized_run_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE filter_snapshot_nk = ?
        """,
        [*new_fingerprint, first_seen_run_id, filter_row.last_materialized_run_id, filter_row.filter_snapshot_nk],
    )
    return "reused" if tuple(existing_row[:-1]) == new_fingerprint else "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: FilterSnapshotBuildSummary,
) -> None:
    _update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        summary_payload=summary.as_dict(),
    )


def _update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {FILTER_RUN_TABLE}
        SET
            run_status = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True),
            run_id,
        ],
    )


def _load_table_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchall()
    if not rows:
        raise ValueError(f"Missing table: {table_name}")
    return {str(row[0]) for row in rows}


def _resolve_existing_column(
    available_columns: set[str],
    candidates: tuple[str, ...],
    *,
    field_name: str,
    table_name: str,
) -> str:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    raise ValueError(f"Missing required column `{field_name}` in table `{table_name}`.")


def _resolve_optional_column(available_columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    return None


def _normalize_date_value(value: object, *, field_name: str) -> date:
    if value is None:
        raise ValueError(f"Missing required date field: {field_name}")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_progress_state(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"advancing", "stalled", "failed", "unknown"}:
        return normalized
    return "unknown"


def _normalize_optional_str(value: object, *, default: str = "") -> str:
    if value is None:
        return default
    candidate = str(value).strip()
    return candidate or default


def _normalize_optional_nullable_str(value: object) -> str | None:
    if value is None:
        return None
    candidate = str(value).strip()
    return candidate or None


def _normalize_optional_nullable_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _normalize_optional_int(value: object) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def _write_summary(summary: FilterSnapshotBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
