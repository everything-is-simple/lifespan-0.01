"""执行 `structure snapshot` 官方 producer 的最小 bounded 运行时。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.malf.bootstrap import (
    MALF_STATE_SNAPSHOT_TABLE,
    PIVOT_CONFIRMED_BREAK_LEDGER_TABLE,
    SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE,
)
from mlq.structure.bootstrap import (
    STRUCTURE_RUN_SNAPSHOT_TABLE,
    STRUCTURE_RUN_TABLE,
    STRUCTURE_SNAPSHOT_TABLE,
    bootstrap_structure_snapshot_ledger,
    structure_ledger_path,
)


DEFAULT_STRUCTURE_CONTEXT_TABLE: Final[str] = MALF_STATE_SNAPSHOT_TABLE
DEFAULT_STRUCTURE_INPUT_TABLE: Final[str] = MALF_STATE_SNAPSHOT_TABLE
DEFAULT_STRUCTURE_BREAK_CONFIRMATION_TABLE: Final[str | None] = None
DEFAULT_STRUCTURE_STATS_TABLE: Final[str | None] = None
DEFAULT_STRUCTURE_SOURCE_TIMEFRAME: Final[str] = "D"
DEFAULT_STRUCTURE_CONTRACT_VERSION: Final[str] = "structure-snapshot-v1"
LEGACY_STRUCTURE_CONTEXT_TABLE: Final[str] = "pas_context_snapshot"
LEGACY_STRUCTURE_INPUT_TABLE: Final[str] = "structure_candidate_snapshot"


@dataclass(frozen=True)
class StructureSnapshotBuildSummary:
    """总结一次 `structure snapshot` producer 的 bounded 运行结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    structure_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    candidate_input_count: int
    materialized_snapshot_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    missing_context_count: int
    advancing_count: int
    stalled_count: int
    failed_count: int
    unknown_count: int
    structure_ledger_path: str
    malf_ledger_path: str
    source_context_table: str
    source_structure_input_table: str
    source_break_confirmation_table: str | None
    source_stats_table: str | None
    source_timeframe: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _StructureInputRow:
    instrument: str
    signal_date: date
    asof_date: date
    new_high_count: int
    new_low_count: int
    refresh_density: float
    advancement_density: float
    is_failed_extreme: bool
    failure_type: str | None


@dataclass(frozen=True)
class _StructureContextRow:
    instrument: str
    signal_date: date
    asof_date: date
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int
    source_context_nk: str


@dataclass(frozen=True)
class _BreakConfirmationRow:
    instrument: str
    trigger_bar_dt: date
    confirmation_status: str
    break_event_nk: str


@dataclass(frozen=True)
class _StatsSnapshotRow:
    instrument: str
    signal_date: date
    asof_bar_dt: date
    stats_snapshot_nk: str
    exhaustion_risk_bucket: str | None
    reversal_probability_bucket: str | None


@dataclass(frozen=True)
class _StructureSnapshotRow:
    structure_snapshot_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int
    new_high_count: int
    new_low_count: int
    refresh_density: float
    advancement_density: float
    is_failed_extreme: bool
    failure_type: str | None
    structure_progress_state: str
    break_confirmation_status: str | None
    break_confirmation_ref: str | None
    stats_snapshot_nk: str | None
    exhaustion_risk_bucket: str | None
    reversal_probability_bucket: str | None
    source_context_nk: str
    structure_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


def run_structure_snapshot_build(
    *,
    settings: WorkspaceRoots | None = None,
    structure_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    run_id: str | None = None,
    source_context_table: str = DEFAULT_STRUCTURE_CONTEXT_TABLE,
    source_structure_input_table: str = DEFAULT_STRUCTURE_INPUT_TABLE,
    source_break_confirmation_table: str | None = DEFAULT_STRUCTURE_BREAK_CONFIRMATION_TABLE,
    source_stats_table: str | None = DEFAULT_STRUCTURE_STATS_TABLE,
    source_timeframe: str = DEFAULT_STRUCTURE_SOURCE_TIMEFRAME,
    structure_contract_version: str = DEFAULT_STRUCTURE_CONTRACT_VERSION,
    runner_name: str = "structure_snapshot_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
) -> StructureSnapshotBuildSummary:
    """从官方 `malf` 上游物化 `structure snapshot`。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_structure_path = Path(structure_path or structure_ledger_path(workspace))
    resolved_malf_path = Path(malf_path or workspace.databases.malf)
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_timeframe = _normalize_timeframe(source_timeframe)
    materialization_run_id = run_id or _build_structure_run_id()

    _ensure_database_exists(resolved_malf_path, label="malf")
    actual_source_context_table = _resolve_structure_source_table(
        malf_path=resolved_malf_path,
        requested_table=source_context_table,
        fallback_table=LEGACY_STRUCTURE_CONTEXT_TABLE,
    )
    actual_source_input_table = _resolve_structure_source_table(
        malf_path=resolved_malf_path,
        requested_table=source_structure_input_table,
        fallback_table=LEGACY_STRUCTURE_INPUT_TABLE,
    )
    actual_break_confirmation_table = _resolve_optional_sidecar_table(
        malf_path=resolved_malf_path,
        requested_table=source_break_confirmation_table,
        fallback_table=PIVOT_CONFIRMED_BREAK_LEDGER_TABLE if actual_source_input_table == LEGACY_STRUCTURE_INPUT_TABLE else None,
    )
    actual_stats_table = _resolve_optional_sidecar_table(
        malf_path=resolved_malf_path,
        requested_table=source_stats_table,
        fallback_table=SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE if actual_source_input_table == LEGACY_STRUCTURE_INPUT_TABLE else None,
    )
    input_rows = _load_structure_input_rows(
        malf_path=resolved_malf_path,
        table_name=actual_source_input_table,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        limit=normalized_limit,
        timeframe=normalized_timeframe,
    )
    context_rows = _load_context_rows(
        malf_path=resolved_malf_path,
        table_name=actual_source_context_table,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=tuple(sorted({row.instrument for row in input_rows})),
        timeframe=normalized_timeframe,
    )
    context_map = {
        (row.instrument, row.signal_date, row.asof_date): row
        for row in context_rows
    }
    break_confirmation_map = _load_break_confirmation_rows(
        malf_path=resolved_malf_path,
        table_name=actual_break_confirmation_table,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=tuple(sorted({row.instrument for row in input_rows})),
        timeframe=normalized_timeframe,
    )
    stats_snapshot_map = _load_stats_snapshot_rows(
        malf_path=resolved_malf_path,
        table_name=actual_stats_table,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=tuple(sorted({row.instrument for row in input_rows})),
        timeframe=normalized_timeframe,
    )

    structure_connection = duckdb.connect(str(resolved_structure_path))
    try:
        bootstrap_structure_snapshot_ledger(workspace, connection=structure_connection)
        bounded_instrument_count = len({row.instrument for row in input_rows})
        _insert_run_row(
            structure_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=bounded_instrument_count,
            source_context_table=actual_source_context_table,
            source_structure_input_table=actual_source_input_table,
            structure_contract_version=structure_contract_version,
        )
        summary = _materialize_structure_rows(
            connection=structure_connection,
            run_id=materialization_run_id,
            input_rows=input_rows,
            context_map=context_map,
            break_confirmation_map=break_confirmation_map,
            stats_snapshot_map=stats_snapshot_map,
            structure_contract_version=structure_contract_version,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            structure_path=resolved_structure_path,
            malf_path=resolved_malf_path,
            source_context_table=actual_source_context_table,
            source_structure_input_table=actual_source_input_table,
            source_break_confirmation_table=actual_break_confirmation_table,
            source_stats_table=actual_stats_table,
            source_timeframe=normalized_timeframe,
            batch_size=normalized_batch_size,
        )
        _mark_run_completed(structure_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            structure_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        structure_connection.close()


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _normalize_timeframe(value: str | None) -> str:
    candidate = str(value or DEFAULT_STRUCTURE_SOURCE_TIMEFRAME).strip().upper()
    return candidate or DEFAULT_STRUCTURE_SOURCE_TIMEFRAME


def _build_structure_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"structure-snapshot-{timestamp}"


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _resolve_structure_source_table(
    *,
    malf_path: Path,
    requested_table: str,
    fallback_table: str,
) -> str:
    if _database_table_exists(malf_path, requested_table):
        return requested_table
    return fallback_table if _database_table_exists(malf_path, fallback_table) else requested_table


def _resolve_optional_sidecar_table(
    *,
    malf_path: Path,
    requested_table: str | None,
    fallback_table: str | None,
) -> str | None:
    if requested_table and _database_table_exists(malf_path, requested_table):
        return requested_table
    if fallback_table and _database_table_exists(malf_path, fallback_table):
        return fallback_table
    return None


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


def _load_structure_input_rows(
    *,
    malf_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    timeframe: str,
) -> list[_StructureInputRow]:
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
        if _is_canonical_state_table(available_columns):
            rows = _load_canonical_input_rows(
                connection,
                table_name=table_name,
                signal_start_date=signal_start_date,
                signal_end_date=signal_end_date,
                instruments=instruments,
                limit=limit,
                timeframe=timeframe,
            )
        else:
            rows = _load_bridge_input_rows(
                connection,
                table_name=table_name,
                available_columns=available_columns,
                signal_start_date=signal_start_date,
                signal_end_date=signal_end_date,
                instruments=instruments,
                limit=limit,
            )
        return rows
    finally:
        connection.close()


def _load_context_rows(
    *,
    malf_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> list[_StructureContextRow]:
    if not instruments:
        return []
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
        if _is_canonical_state_table(available_columns):
            return _load_canonical_context_rows(
                connection,
                table_name=table_name,
                signal_start_date=signal_start_date,
                signal_end_date=signal_end_date,
                instruments=instruments,
                timeframe=timeframe,
            )
        return _load_bridge_context_rows(
            connection,
            table_name=table_name,
            available_columns=available_columns,
            signal_start_date=signal_start_date,
            signal_end_date=signal_end_date,
            instruments=instruments,
        )
    finally:
        connection.close()


def _is_canonical_state_table(available_columns: set[str]) -> bool:
    return {"major_state", "trend_direction", "current_hh_count", "current_ll_count"}.issubset(available_columns)


def _load_bridge_input_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    available_columns: set[str],
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_StructureInputRow]:
    instrument_column = _resolve_existing_column(
        available_columns,
        ("instrument", "entity_code", "code"),
        field_name="instrument",
        table_name=table_name,
    )
    signal_date_column = _resolve_existing_column(
        available_columns,
        ("signal_date",),
        field_name="signal_date",
        table_name=table_name,
    )
    asof_date_column = _resolve_optional_column(available_columns, ("asof_date",)) or signal_date_column
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
            {instrument_column} AS instrument,
            {signal_date_column} AS signal_date,
            {asof_date_column} AS asof_date,
            {_resolve_existing_column(available_columns, ("new_high_count",), field_name="new_high_count", table_name=table_name)} AS new_high_count,
            {_resolve_existing_column(available_columns, ("new_low_count",), field_name="new_low_count", table_name=table_name)} AS new_low_count,
            {_resolve_existing_column(available_columns, ("refresh_density",), field_name="refresh_density", table_name=table_name)} AS refresh_density,
            {_resolve_existing_column(available_columns, ("advancement_density",), field_name="advancement_density", table_name=table_name)} AS advancement_density,
            {_resolve_existing_column(available_columns, ("is_failed_extreme",), field_name="is_failed_extreme", table_name=table_name)} AS is_failed_extreme,
            {_resolve_optional_column(available_columns, ("failure_type",)) or "NULL"} AS failure_type
        FROM {table_name}
        {where_sql}
        ORDER BY signal_date, instrument, asof_date
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        _StructureInputRow(
            instrument=str(row[0]),
            signal_date=_normalize_date_value(row[1], field_name="signal_date"),
            asof_date=_normalize_date_value(row[2], field_name="asof_date"),
            new_high_count=_normalize_optional_int(row[3]),
            new_low_count=_normalize_optional_int(row[4]),
            refresh_density=_normalize_optional_float(row[5]),
            advancement_density=_normalize_optional_float(row[6]),
            is_failed_extreme=bool(row[7]),
            failure_type=_normalize_optional_nullable_str(row[8]),
        )
        for row in rows
    ]


def _load_canonical_input_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    timeframe: str,
) -> list[_StructureInputRow]:
    available_columns = _load_table_columns(connection, table_name)
    code_column = _resolve_existing_column(
        available_columns,
        ("code", "instrument", "entity_code"),
        field_name="code",
        table_name=table_name,
    )
    asof_date_column = _resolve_existing_column(
        available_columns,
        ("asof_bar_dt", "asof_date", "signal_date"),
        field_name="asof_bar_dt",
        table_name=table_name,
    )
    timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
    asset_type_column = _resolve_optional_column(available_columns, ("asset_type",))
    parameters: list[object] = []
    where_clauses: list[str] = []
    if signal_start_date is not None:
        where_clauses.append(f"{asof_date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{asof_date_column} <= ?")
        parameters.append(signal_end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{code_column} IN ({placeholders})")
        parameters.extend(instruments)
    if timeframe_column is not None:
        where_clauses.append(f"{timeframe_column} = ?")
        parameters.append(timeframe)
    if asset_type_column is not None:
        where_clauses.append(f"{asset_type_column} = ?")
        parameters.append("stock")
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = connection.execute(
        f"""
        SELECT
            {code_column} AS instrument,
            {asof_date_column} AS signal_date,
            {asof_date_column} AS asof_date,
            COALESCE(current_hh_count, 0) AS current_hh_count,
            COALESCE(current_ll_count, 0) AS current_ll_count,
            major_state,
            trend_direction
        FROM {table_name}
        {where_sql}
        ORDER BY {asof_date_column}, {code_column}
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    input_rows: list[_StructureInputRow] = []
    for row in rows:
        major_state = _normalize_optional_str(row[5], default="牛逆")
        trend_direction = _normalize_optional_str(row[6], default="down").lower()
        bullish_context = _map_major_state_to_context_code(major_state).startswith("BULL_")
        new_high_count = _normalize_optional_int(row[3]) if bullish_context else 0
        new_low_count = _normalize_optional_int(row[4]) if not bullish_context else 0
        refresh_density = min(float(new_high_count) / 4.0, 1.0) if new_high_count > 0 else 0.0
        advancement_density = 1.0 if trend_direction == "up" else 0.0
        failure_type = _derive_failure_type_from_major_state(major_state)
        input_rows.append(
            _StructureInputRow(
                instrument=str(row[0]),
                signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                new_high_count=new_high_count,
                new_low_count=new_low_count,
                refresh_density=refresh_density,
                advancement_density=advancement_density,
                is_failed_extreme=failure_type is not None,
                failure_type=failure_type,
            )
        )
    return input_rows


def _load_bridge_context_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    available_columns: set[str],
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> list[_StructureContextRow]:
    instrument_column = _resolve_existing_column(
        available_columns,
        ("instrument", "entity_code", "code"),
        field_name="instrument",
        table_name=table_name,
    )
    signal_date_column = _resolve_existing_column(
        available_columns,
        ("signal_date",),
        field_name="signal_date",
        table_name=table_name,
    )
    asof_date_column = _resolve_optional_column(available_columns, ("asof_date", "calc_date")) or signal_date_column
    source_context_column = _resolve_optional_column(available_columns, ("source_context_nk",))
    order_columns = [
        column_name
        for column_name in (
            _resolve_optional_column(available_columns, ("asof_date",)),
            _resolve_optional_column(available_columns, ("calc_date",)),
            _resolve_optional_column(available_columns, ("created_at",)),
        )
        if column_name is not None
    ]
    order_sql = ", ".join(f"{column_name} DESC" for column_name in order_columns) or "1"
    placeholders = ", ".join("?" for _ in instruments)
    parameters: list[object] = [*instruments]
    where_clauses = [f"{instrument_column} IN ({placeholders})"]
    if signal_start_date is not None:
        where_clauses.append(f"{signal_date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{signal_date_column} <= ?")
        parameters.append(signal_end_date)
    where_sql = f"WHERE {' AND '.join(where_clauses)}"
    rows = connection.execute(
        f"""
        WITH ranked_context AS (
            SELECT
                {instrument_column} AS instrument,
                {signal_date_column} AS signal_date,
                {asof_date_column} AS asof_date,
                COALESCE({_resolve_optional_column(available_columns, ("malf_context_4",)) or "'UNKNOWN'"}, 'UNKNOWN') AS malf_context_4,
                COALESCE({_resolve_optional_column(available_columns, ("lifecycle_rank_high",)) or "0"}, 0) AS lifecycle_rank_high,
                COALESCE({_resolve_optional_column(available_columns, ("lifecycle_rank_total",)) or "0"}, 0) AS lifecycle_rank_total,
                {source_context_column if source_context_column is not None else "NULL"} AS source_context_nk,
                ROW_NUMBER() OVER (
                    PARTITION BY {instrument_column}, {signal_date_column}, {asof_date_column}
                    ORDER BY {order_sql}
                ) AS row_rank
            FROM {table_name}
            {where_sql}
        )
        SELECT
            instrument,
            signal_date,
            asof_date,
            malf_context_4,
            lifecycle_rank_high,
            lifecycle_rank_total,
            source_context_nk
        FROM ranked_context
        WHERE row_rank = 1
        """,
        parameters,
    ).fetchall()
    context_rows: list[_StructureContextRow] = []
    for row in rows:
        signal_date_value = _normalize_date_value(row[1], field_name="signal_date")
        asof_date_value = _normalize_date_value(row[2], field_name="asof_date")
        instrument_value = str(row[0])
        malf_context_4 = _normalize_optional_str(row[3], default="UNKNOWN")
        source_context_nk = _normalize_optional_str(
            row[6],
            default=_build_source_context_nk(
                instrument=instrument_value,
                signal_date=signal_date_value,
                asof_date=asof_date_value,
                malf_context_4=malf_context_4,
            ),
        )
        context_rows.append(
            _StructureContextRow(
                instrument=instrument_value,
                signal_date=signal_date_value,
                asof_date=asof_date_value,
                malf_context_4=malf_context_4,
                lifecycle_rank_high=_normalize_optional_int(row[4]),
                lifecycle_rank_total=_normalize_optional_int(row[5]),
                source_context_nk=source_context_nk,
            )
        )
    return context_rows


def _load_canonical_context_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> list[_StructureContextRow]:
    available_columns = _load_table_columns(connection, table_name)
    code_column = _resolve_existing_column(
        available_columns,
        ("code", "instrument", "entity_code"),
        field_name="code",
        table_name=table_name,
    )
    asof_date_column = _resolve_existing_column(
        available_columns,
        ("asof_bar_dt", "asof_date", "signal_date"),
        field_name="asof_bar_dt",
        table_name=table_name,
    )
    snapshot_nk_column = _resolve_optional_column(available_columns, ("snapshot_nk",))
    timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
    asset_type_column = _resolve_optional_column(available_columns, ("asset_type",))
    parameters: list[object] = []
    where_clauses: list[str] = []
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{code_column} IN ({placeholders})")
        parameters.extend(instruments)
    if signal_start_date is not None:
        where_clauses.append(f"{asof_date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{asof_date_column} <= ?")
        parameters.append(signal_end_date)
    if timeframe_column is not None:
        where_clauses.append(f"{timeframe_column} = ?")
        parameters.append(timeframe)
    if asset_type_column is not None:
        where_clauses.append(f"{asset_type_column} = ?")
        parameters.append("stock")
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = connection.execute(
        f"""
        SELECT
            {code_column} AS instrument,
            {asof_date_column} AS signal_date,
            {asof_date_column} AS asof_date,
            major_state,
            COALESCE(current_hh_count, 0) AS current_hh_count,
            COALESCE(current_ll_count, 0) AS current_ll_count,
            {snapshot_nk_column if snapshot_nk_column is not None else "NULL"} AS snapshot_nk
        FROM {table_name}
        {where_sql}
        ORDER BY {asof_date_column}, {code_column}
        """,
        parameters,
    ).fetchall()
    context_rows: list[_StructureContextRow] = []
    for row in rows:
        signal_date_value = _normalize_date_value(row[1], field_name="signal_date")
        asof_date_value = _normalize_date_value(row[2], field_name="asof_date")
        major_state = _normalize_optional_str(row[3], default="牛逆")
        malf_context_4 = _map_major_state_to_context_code(major_state)
        lifecycle_rank_high = _derive_lifecycle_rank_high(
            malf_context_4=malf_context_4,
            current_hh_count=_normalize_optional_int(row[4]),
            current_ll_count=_normalize_optional_int(row[5]),
        )
        source_context_nk = _normalize_optional_str(
            row[6],
            default=_build_source_context_nk(
                instrument=str(row[0]),
                signal_date=signal_date_value,
                asof_date=asof_date_value,
                malf_context_4=malf_context_4,
            ),
        )
        context_rows.append(
            _StructureContextRow(
                instrument=str(row[0]),
                signal_date=signal_date_value,
                asof_date=asof_date_value,
                malf_context_4=malf_context_4,
                lifecycle_rank_high=lifecycle_rank_high,
                lifecycle_rank_total=4,
                source_context_nk=source_context_nk,
            )
        )
    return context_rows


def _load_break_confirmation_rows(
    *,
    malf_path: Path,
    table_name: str | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> dict[tuple[str, date], _BreakConfirmationRow]:
    if not malf_path.exists() or not instruments or not table_name:
        return {}
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
    except ValueError:
        return {}
    try:
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument", "entity_code", "code"),
            field_name="instrument",
            table_name=table_name,
        )
        trigger_date_column = _resolve_existing_column(
            available_columns,
            ("trigger_bar_dt", "signal_date"),
            field_name="trigger_bar_dt",
            table_name=table_name,
        )
        timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"{instrument_column} IN ({placeholders})"]
        if signal_start_date is not None:
            where_clauses.append(f"{trigger_date_column} >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append(f"{trigger_date_column} <= ?")
            parameters.append(signal_end_date)
        if timeframe_column is not None:
            where_clauses.append(f"{timeframe_column} = ?")
            parameters.append(timeframe)
        rows = connection.execute(
            f"""
            SELECT
                {instrument_column} AS instrument,
                {trigger_date_column} AS trigger_bar_dt,
                COALESCE({_resolve_optional_column(available_columns, ("confirmation_status",)) or "'pending'"}, 'pending') AS confirmation_status,
                COALESCE({_resolve_optional_column(available_columns, ("break_event_nk",)) or "NULL"}, '') AS break_event_nk
            FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY {trigger_date_column}, {instrument_column}
            """,
            parameters,
        ).fetchall()
        return {
            (str(row[0]), _normalize_date_value(row[1], field_name="trigger_bar_dt")): _BreakConfirmationRow(
                instrument=str(row[0]),
                trigger_bar_dt=_normalize_date_value(row[1], field_name="trigger_bar_dt"),
                confirmation_status=_normalize_optional_str(row[2], default="pending"),
                break_event_nk=_normalize_optional_str(row[3]),
            )
            for row in rows
        }
    finally:
        connection.close()


def _load_stats_snapshot_rows(
    *,
    malf_path: Path,
    table_name: str | None,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    timeframe: str,
) -> dict[tuple[str, date, date], _StatsSnapshotRow]:
    if not malf_path.exists() or not instruments or not table_name:
        return {}
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
    except ValueError:
        return {}
    try:
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument", "entity_code", "code"),
            field_name="instrument",
            table_name=table_name,
        )
        signal_date_column = _resolve_existing_column(
            available_columns,
            ("signal_date",),
            field_name="signal_date",
            table_name=table_name,
        )
        asof_date_column = _resolve_optional_column(available_columns, ("asof_bar_dt", "asof_date")) or signal_date_column
        timeframe_column = _resolve_optional_column(available_columns, ("timeframe",))
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
        rows = connection.execute(
            f"""
            SELECT
                {instrument_column} AS instrument,
                {signal_date_column} AS signal_date,
                {asof_date_column} AS asof_bar_dt,
                COALESCE({_resolve_optional_column(available_columns, ("stats_snapshot_nk",)) or "NULL"}, '') AS stats_snapshot_nk,
                {_resolve_optional_column(available_columns, ("exhaustion_risk_bucket",)) or "NULL"} AS exhaustion_risk_bucket,
                {_resolve_optional_column(available_columns, ("reversal_probability_bucket",)) or "NULL"} AS reversal_probability_bucket
            FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY {signal_date_column}, {instrument_column}, {asof_date_column}
            """,
            parameters,
        ).fetchall()
        return {
            (
                str(row[0]),
                _normalize_date_value(row[1], field_name="signal_date"),
                _normalize_date_value(row[2], field_name="asof_bar_dt"),
            ): _StatsSnapshotRow(
                instrument=str(row[0]),
                signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                asof_bar_dt=_normalize_date_value(row[2], field_name="asof_bar_dt"),
                stats_snapshot_nk=_normalize_optional_str(row[3]),
                exhaustion_risk_bucket=_normalize_optional_nullable_str(row[4]),
                reversal_probability_bucket=_normalize_optional_nullable_str(row[5]),
            )
            for row in rows
        }
    finally:
        connection.close()


def _materialize_structure_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    input_rows: list[_StructureInputRow],
    context_map: dict[tuple[str, date, date], _StructureContextRow],
    break_confirmation_map: dict[tuple[str, date], _BreakConfirmationRow],
    stats_snapshot_map: dict[tuple[str, date, date], _StatsSnapshotRow],
    structure_contract_version: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    structure_path: Path,
    malf_path: Path,
    source_context_table: str,
    source_structure_input_table: str,
    source_break_confirmation_table: str | None,
    source_stats_table: str | None,
    source_timeframe: str,
    batch_size: int,
) -> StructureSnapshotBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    missing_context_count = 0
    advancing_count = 0
    stalled_count = 0
    failed_count = 0
    unknown_count = 0

    for input_batch in _bounded_by_instrument_batches(input_rows, batch_size=batch_size):
        for input_row in input_batch:
            context_row = context_map.get((input_row.instrument, input_row.signal_date, input_row.asof_date))
            if context_row is None:
                # `structure` 必须挂在正式上下文上，缺上下文时宁可跳过，也不伪造自然键。
                missing_context_count += 1
                continue
            snapshot_row = _build_structure_snapshot_row(
                run_id=run_id,
                input_row=input_row,
                context_row=context_row,
                break_confirmation_row=break_confirmation_map.get((input_row.instrument, input_row.signal_date)),
                stats_snapshot_row=stats_snapshot_map.get((input_row.instrument, input_row.signal_date, input_row.asof_date)),
                structure_contract_version=structure_contract_version,
            )
            materialization_action = _upsert_structure_snapshot(connection, snapshot_row=snapshot_row)
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {STRUCTURE_RUN_SNAPSHOT_TABLE} (
                    run_id,
                    structure_snapshot_nk,
                    materialization_action,
                    structure_progress_state
                )
                VALUES (?, ?, ?, ?)
                """,
                [
                    run_id,
                    snapshot_row.structure_snapshot_nk,
                    materialization_action,
                    snapshot_row.structure_progress_state,
                ],
            )
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

            if snapshot_row.structure_progress_state == "advancing":
                advancing_count += 1
            elif snapshot_row.structure_progress_state == "stalled":
                stalled_count += 1
            elif snapshot_row.structure_progress_state == "failed":
                failed_count += 1
            else:
                unknown_count += 1

    materialized_snapshot_count = inserted_count + reused_count + rematerialized_count
    return StructureSnapshotBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        structure_contract_version=structure_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in input_rows}),
        candidate_input_count=len(input_rows),
        materialized_snapshot_count=materialized_snapshot_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        missing_context_count=missing_context_count,
        advancing_count=advancing_count,
        stalled_count=stalled_count,
        failed_count=failed_count,
        unknown_count=unknown_count,
        structure_ledger_path=str(structure_path),
        malf_ledger_path=str(malf_path),
        source_context_table=source_context_table,
        source_structure_input_table=source_structure_input_table,
        source_break_confirmation_table=source_break_confirmation_table,
        source_stats_table=source_stats_table,
        source_timeframe=source_timeframe,
    )


def _bounded_by_instrument_batches(
    input_rows: list[_StructureInputRow],
    *,
    batch_size: int,
) -> list[list[_StructureInputRow]]:
    if not input_rows:
        return []
    normalized_batch_size = max(int(batch_size), 1)
    batches: list[list[_StructureInputRow]] = []
    current_batch: list[_StructureInputRow] = []
    current_instruments: set[str] = set()
    for row in input_rows:
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
    source_context_table: str,
    source_structure_input_table: str,
    structure_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {STRUCTURE_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_context_table,
            source_structure_input_table,
            structure_contract_version
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
            source_context_table,
            source_structure_input_table,
            structure_contract_version,
        ],
    )


def _build_structure_snapshot_row(
    *,
    run_id: str,
    input_row: _StructureInputRow,
    context_row: _StructureContextRow,
    break_confirmation_row: _BreakConfirmationRow | None,
    stats_snapshot_row: _StatsSnapshotRow | None,
    structure_contract_version: str,
) -> _StructureSnapshotRow:
    structure_progress_state = _derive_structure_progress_state(input_row)
    structure_snapshot_nk = _build_structure_snapshot_nk(
        instrument=input_row.instrument,
        signal_date=input_row.signal_date,
        asof_date=input_row.asof_date,
        source_context_nk=context_row.source_context_nk,
        structure_contract_version=structure_contract_version,
    )
    return _StructureSnapshotRow(
        structure_snapshot_nk=structure_snapshot_nk,
        instrument=input_row.instrument,
        signal_date=input_row.signal_date,
        asof_date=input_row.asof_date,
        malf_context_4=context_row.malf_context_4,
        lifecycle_rank_high=context_row.lifecycle_rank_high,
        lifecycle_rank_total=context_row.lifecycle_rank_total,
        new_high_count=input_row.new_high_count,
        new_low_count=input_row.new_low_count,
        refresh_density=input_row.refresh_density,
        advancement_density=input_row.advancement_density,
        is_failed_extreme=input_row.is_failed_extreme,
        failure_type=input_row.failure_type,
        structure_progress_state=structure_progress_state,
        break_confirmation_status=None if break_confirmation_row is None else break_confirmation_row.confirmation_status,
        break_confirmation_ref=None if break_confirmation_row is None else break_confirmation_row.break_event_nk,
        stats_snapshot_nk=None if stats_snapshot_row is None else stats_snapshot_row.stats_snapshot_nk,
        exhaustion_risk_bucket=None if stats_snapshot_row is None else stats_snapshot_row.exhaustion_risk_bucket,
        reversal_probability_bucket=None if stats_snapshot_row is None else stats_snapshot_row.reversal_probability_bucket,
        source_context_nk=context_row.source_context_nk,
        structure_contract_version=structure_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _derive_structure_progress_state(input_row: _StructureInputRow) -> str:
    if input_row.is_failed_extreme or input_row.failure_type is not None:
        return "failed"
    if input_row.new_high_count > 0 or input_row.refresh_density > 0 or input_row.advancement_density > 0:
        return "advancing"
    if input_row.new_low_count > 0:
        return "stalled"
    return "unknown"


def _map_major_state_to_context_code(major_state: str) -> str:
    mapping = {
        "牛顺": "BULL_MAINSTREAM",
        "熊逆": "BULL_COUNTERTREND",
        "牛逆": "BEAR_COUNTERTREND",
        "熊顺": "BEAR_MAINSTREAM",
    }
    return mapping.get(major_state, "UNKNOWN")


def _derive_lifecycle_rank_high(
    *,
    malf_context_4: str,
    current_hh_count: int,
    current_ll_count: int,
) -> int:
    raw_rank = current_hh_count if malf_context_4.startswith("BULL_") else current_ll_count
    return max(0, min(raw_rank, 4))


def _derive_failure_type_from_major_state(major_state: str) -> str | None:
    if major_state == "熊顺":
        return "failed_breakdown"
    if major_state == "牛逆":
        return "failed_extreme"
    return None


def _build_source_context_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    malf_context_4: str,
) -> str:
    return "|".join(
        [
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            malf_context_4,
        ]
    )


def _build_structure_snapshot_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    source_context_nk: str,
    structure_contract_version: str,
) -> str:
    return "|".join(
        [
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            source_context_nk,
            structure_contract_version,
        ]
    )


def _upsert_structure_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    snapshot_row: _StructureSnapshotRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            malf_context_4,
            lifecycle_rank_high,
            lifecycle_rank_total,
            new_high_count,
            new_low_count,
            refresh_density,
            advancement_density,
            is_failed_extreme,
            failure_type,
            structure_progress_state,
            break_confirmation_status,
            break_confirmation_ref,
            stats_snapshot_nk,
            exhaustion_risk_bucket,
            reversal_probability_bucket,
            first_seen_run_id
        FROM {STRUCTURE_SNAPSHOT_TABLE}
        WHERE structure_snapshot_nk = ?
        """,
        [snapshot_row.structure_snapshot_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {STRUCTURE_SNAPSHOT_TABLE} (
                structure_snapshot_nk,
                instrument,
                signal_date,
                asof_date,
                malf_context_4,
                lifecycle_rank_high,
                lifecycle_rank_total,
                new_high_count,
                new_low_count,
                refresh_density,
                advancement_density,
                is_failed_extreme,
                failure_type,
                structure_progress_state,
                break_confirmation_status,
                break_confirmation_ref,
                stats_snapshot_nk,
                exhaustion_risk_bucket,
                reversal_probability_bucket,
                source_context_nk,
                structure_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                snapshot_row.structure_snapshot_nk,
                snapshot_row.instrument,
                snapshot_row.signal_date,
                snapshot_row.asof_date,
                snapshot_row.malf_context_4,
                snapshot_row.lifecycle_rank_high,
                snapshot_row.lifecycle_rank_total,
                snapshot_row.new_high_count,
                snapshot_row.new_low_count,
                snapshot_row.refresh_density,
                snapshot_row.advancement_density,
                snapshot_row.is_failed_extreme,
                snapshot_row.failure_type,
                snapshot_row.structure_progress_state,
                snapshot_row.break_confirmation_status,
                snapshot_row.break_confirmation_ref,
                snapshot_row.stats_snapshot_nk,
                snapshot_row.exhaustion_risk_bucket,
                snapshot_row.reversal_probability_bucket,
                snapshot_row.source_context_nk,
                snapshot_row.structure_contract_version,
                snapshot_row.first_seen_run_id,
                snapshot_row.last_materialized_run_id,
            ],
        )
        return "inserted"

    existing_fingerprint = (
        _normalize_optional_str(existing_row[0], default="UNKNOWN"),
        _normalize_optional_int(existing_row[1]),
        _normalize_optional_int(existing_row[2]),
        _normalize_optional_int(existing_row[3]),
        _normalize_optional_int(existing_row[4]),
        _normalize_optional_float(existing_row[5]),
        _normalize_optional_float(existing_row[6]),
        bool(existing_row[7]),
        _normalize_optional_nullable_str(existing_row[8]),
        _normalize_optional_str(existing_row[9], default="unknown"),
        _normalize_optional_nullable_str(existing_row[10]),
        _normalize_optional_nullable_str(existing_row[11]),
        _normalize_optional_nullable_str(existing_row[12]),
        _normalize_optional_nullable_str(existing_row[13]),
        _normalize_optional_nullable_str(existing_row[14]),
    )
    new_fingerprint = (
        snapshot_row.malf_context_4,
        snapshot_row.lifecycle_rank_high,
        snapshot_row.lifecycle_rank_total,
        snapshot_row.new_high_count,
        snapshot_row.new_low_count,
        snapshot_row.refresh_density,
        snapshot_row.advancement_density,
        snapshot_row.is_failed_extreme,
        snapshot_row.failure_type,
        snapshot_row.structure_progress_state,
        snapshot_row.break_confirmation_status,
        snapshot_row.break_confirmation_ref,
        snapshot_row.stats_snapshot_nk,
        snapshot_row.exhaustion_risk_bucket,
        snapshot_row.reversal_probability_bucket,
    )
    first_seen_run_id = str(existing_row[15]) if existing_row[15] is not None else snapshot_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {STRUCTURE_SNAPSHOT_TABLE}
        SET
            malf_context_4 = ?,
            lifecycle_rank_high = ?,
            lifecycle_rank_total = ?,
            new_high_count = ?,
            new_low_count = ?,
            refresh_density = ?,
            advancement_density = ?,
            is_failed_extreme = ?,
            failure_type = ?,
            structure_progress_state = ?,
            break_confirmation_status = ?,
            break_confirmation_ref = ?,
            stats_snapshot_nk = ?,
            exhaustion_risk_bucket = ?,
            reversal_probability_bucket = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE structure_snapshot_nk = ?
        """,
        [
            snapshot_row.malf_context_4,
            snapshot_row.lifecycle_rank_high,
            snapshot_row.lifecycle_rank_total,
            snapshot_row.new_high_count,
            snapshot_row.new_low_count,
            snapshot_row.refresh_density,
            snapshot_row.advancement_density,
            snapshot_row.is_failed_extreme,
            snapshot_row.failure_type,
            snapshot_row.structure_progress_state,
            snapshot_row.break_confirmation_status,
            snapshot_row.break_confirmation_ref,
            snapshot_row.stats_snapshot_nk,
            snapshot_row.exhaustion_risk_bucket,
            snapshot_row.reversal_probability_bucket,
            first_seen_run_id,
            snapshot_row.last_materialized_run_id,
            snapshot_row.structure_snapshot_nk,
        ],
    )
    if existing_fingerprint == new_fingerprint:
        return "reused"
    return "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: StructureSnapshotBuildSummary,
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
        UPDATE {STRUCTURE_RUN_TABLE}
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


def _normalize_optional_int(value: object) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def _normalize_optional_float(value: object) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def _write_summary(summary: StructureSnapshotBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
