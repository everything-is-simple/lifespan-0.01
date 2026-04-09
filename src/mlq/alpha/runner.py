"""执行 `alpha formal signal` 官方 producer 的最小 bounded 运行时。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.alpha.bootstrap import (
    ALPHA_FORMAL_SIGNAL_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE,
    ALPHA_FORMAL_SIGNAL_RUN_TABLE,
    alpha_ledger_path,
    bootstrap_alpha_formal_signal_ledger,
)
from mlq.core.paths import WorkspaceRoots, default_settings


DEFAULT_ALPHA_FORMAL_SIGNAL_TRIGGER_TABLE: Final[str] = "alpha_trigger_event"
DEFAULT_ALPHA_FORMAL_SIGNAL_FILTER_TABLE: Final[str] = "filter_snapshot"
DEFAULT_ALPHA_FORMAL_SIGNAL_STRUCTURE_TABLE: Final[str] = "structure_snapshot"
DEFAULT_ALPHA_FORMAL_SIGNAL_FALLBACK_CONTEXT_TABLE: Final[str] = "pas_context_snapshot"
DEFAULT_ALPHA_FORMAL_SIGNAL_CONTRACT_VERSION: Final[str] = "alpha-formal-signal-v1"


@dataclass(frozen=True)
class AlphaFormalSignalBuildSummary:
    """总结一次 `alpha formal signal` producer 的 bounded 运行结果。"""

    run_id: str
    producer_name: str
    producer_version: str
    signal_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    candidate_trigger_count: int
    materialized_signal_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    missing_context_count: int
    admitted_count: int
    blocked_count: int
    deferred_count: int
    alpha_ledger_path: str
    filter_ledger_path: str
    structure_ledger_path: str
    legacy_context_path: str | None
    source_trigger_table: str
    source_filter_table: str
    source_structure_table: str
    fallback_context_table: str | None

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _TriggerRow:
    source_trigger_event_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str


@dataclass(frozen=True)
class _ContextRow:
    instrument: str
    signal_date: date
    asof_date: date
    formal_signal_status: str
    trigger_admissible: bool
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int


@dataclass(frozen=True)
class _FormalSignalEventRow:
    signal_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str
    formal_signal_status: str
    trigger_admissible: bool
    malf_context_4: str
    lifecycle_rank_high: int
    lifecycle_rank_total: int
    source_trigger_event_nk: str
    signal_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


def run_alpha_formal_signal_build(
    *,
    settings: WorkspaceRoots | None = None,
    alpha_path: Path | None = None,
    filter_path: Path | None = None,
    structure_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    run_id: str | None = None,
    source_trigger_table: str = DEFAULT_ALPHA_FORMAL_SIGNAL_TRIGGER_TABLE,
    source_filter_table: str = DEFAULT_ALPHA_FORMAL_SIGNAL_FILTER_TABLE,
    source_structure_table: str = DEFAULT_ALPHA_FORMAL_SIGNAL_STRUCTURE_TABLE,
    source_context_table: str | None = DEFAULT_ALPHA_FORMAL_SIGNAL_FALLBACK_CONTEXT_TABLE,
    signal_contract_version: str = DEFAULT_ALPHA_FORMAL_SIGNAL_CONTRACT_VERSION,
    producer_name: str = "alpha_formal_signal_producer",
    producer_version: str = "v1",
    summary_path: Path | None = None,
) -> AlphaFormalSignalBuildSummary:
    """从官方触发事实和 `filter/structure snapshot` 物化 `alpha formal signal`。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    resolved_filter_path = Path(filter_path or workspace.databases.filter)
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    resolved_malf_path = Path(malf_path or workspace.databases.malf)
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    materialization_run_id = run_id or _build_alpha_formal_signal_run_id()

    _ensure_database_exists(resolved_filter_path, label="filter")
    _ensure_database_exists(resolved_structure_path, label="structure")
    alpha_connection = duckdb.connect(str(resolved_alpha_path))
    try:
        bootstrap_alpha_formal_signal_ledger(workspace, connection=alpha_connection)
        trigger_rows = _load_trigger_rows(
            connection=alpha_connection,
            table_name=source_trigger_table,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=normalized_instruments,
            limit=normalized_limit,
        )
        context_rows = _load_official_context_rows(
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            filter_table_name=source_filter_table,
            structure_table_name=source_structure_table,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=tuple(sorted({row.instrument for row in trigger_rows})),
            legacy_context_path=resolved_malf_path,
            legacy_context_table=source_context_table,
        )
        context_map = {
            (row.instrument, row.signal_date, row.asof_date): row
            for row in context_rows
        }

        bounded_instrument_count = len({row.instrument for row in trigger_rows})
        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            producer_name=producer_name,
            producer_version=producer_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=bounded_instrument_count,
            source_trigger_table=source_trigger_table,
            source_context_table=source_filter_table,
            signal_contract_version=signal_contract_version,
        )

        summary = _materialize_formal_signal_rows(
            connection=alpha_connection,
            run_id=materialization_run_id,
            trigger_rows=trigger_rows,
            context_map=context_map,
            signal_contract_version=signal_contract_version,
            producer_name=producer_name,
            producer_version=producer_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            alpha_path=resolved_alpha_path,
            filter_path=resolved_filter_path,
            structure_path=resolved_structure_path,
            legacy_context_path=resolved_malf_path if source_context_table else None,
            source_trigger_table=source_trigger_table,
            source_filter_table=source_filter_table,
            source_structure_table=source_structure_table,
            fallback_context_table=source_context_table,
            batch_size=normalized_batch_size,
        )
        _mark_run_completed(
            alpha_connection,
            run_id=materialization_run_id,
            summary=summary,
        )
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


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_alpha_formal_signal_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"alpha-formal-signal-{timestamp}"


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _load_trigger_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_TriggerRow]:
    available_columns = _load_table_columns(connection, table_name)
    signal_date_column = _resolve_existing_column(
        available_columns,
        ("signal_date",),
        field_name="signal_date",
        table_name=table_name,
    )
    instrument_column = _resolve_existing_column(
        available_columns,
        ("instrument", "code"),
        field_name="instrument",
        table_name=table_name,
    )
    select_sql = _build_trigger_select_sql(table_name=table_name, available_columns=available_columns)
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
        {select_sql}
        {where_sql}
        ORDER BY signal_date, instrument, source_trigger_event_nk
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        _TriggerRow(
            source_trigger_event_nk=str(row[0]),
            instrument=str(row[1]),
            signal_date=_normalize_date_value(row[2], field_name="signal_date"),
            asof_date=_normalize_date_value(row[3], field_name="asof_date"),
            trigger_family=str(row[4]),
            trigger_type=str(row[5]),
            pattern_code=str(row[6]),
        )
        for row in rows
    ]


def _build_trigger_select_sql(*, table_name: str, available_columns: set[str]) -> str:
    source_trigger_column = _resolve_existing_column(
        available_columns,
        ("source_trigger_event_nk", "signal_id", "trigger_event_nk"),
        field_name="source_trigger_event_nk",
        table_name=table_name,
    )
    instrument_column = _resolve_existing_column(
        available_columns,
        ("instrument", "code"),
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
    trigger_family_column = _resolve_optional_column(available_columns, ("trigger_family",))
    trigger_type_column = _resolve_existing_column(
        available_columns,
        ("trigger_type",),
        field_name="trigger_type",
        table_name=table_name,
    )
    pattern_code_column = _resolve_existing_column(
        available_columns,
        ("pattern_code", "pattern", "trigger_type"),
        field_name="pattern_code",
        table_name=table_name,
    )
    return f"""
        SELECT
            {source_trigger_column} AS source_trigger_event_nk,
            {instrument_column} AS instrument,
            {signal_date_column} AS signal_date,
            {asof_date_column} AS asof_date,
            {("'PAS'" if trigger_family_column is None else trigger_family_column)} AS trigger_family,
            {trigger_type_column} AS trigger_type,
            {pattern_code_column} AS pattern_code
        FROM {table_name}
    """


def _load_official_context_rows(
    *,
    filter_path: Path,
    structure_path: Path,
    filter_table_name: str,
    structure_table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    legacy_context_path: Path | None,
    legacy_context_table: str | None,
) -> list[_ContextRow]:
    if not instruments:
        return []
    connection = duckdb.connect(str(filter_path), read_only=False)
    try:
        structure_path_sql = str(structure_path).replace("\\", "/").replace("'", "''")
        connection.execute(f"ATTACH '{structure_path_sql}' AS structure_db")
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"instrument IN ({placeholders})"]
        if signal_start_date is not None:
            where_clauses.append("signal_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("signal_date <= ?")
            parameters.append(signal_end_date)
        rows = connection.execute(
            f"""
            WITH ranked_filter AS (
                SELECT
                    instrument,
                    signal_date,
                    asof_date,
                    structure_snapshot_nk,
                    trigger_admissible,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument, signal_date, asof_date
                        ORDER BY last_materialized_run_id DESC
                    ) AS row_rank
                FROM {filter_table_name}
                WHERE {' AND '.join(where_clauses)}
            )
            SELECT
                rf.instrument,
                rf.signal_date,
                rf.asof_date,
                CASE WHEN rf.trigger_admissible THEN 'admitted' ELSE 'blocked' END AS formal_signal_status,
                rf.trigger_admissible,
                s.malf_context_4,
                s.lifecycle_rank_high,
                s.lifecycle_rank_total
            FROM ranked_filter AS rf
            INNER JOIN structure_db.main.{structure_table_name} AS s
                ON s.structure_snapshot_nk = rf.structure_snapshot_nk
            WHERE rf.row_rank = 1
            """,
            parameters,
        ).fetchall()
        if rows:
            return [
                _ContextRow(
                    instrument=str(row[0]),
                    signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                    asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                    formal_signal_status=_normalize_formal_signal_status(row[3]),
                    trigger_admissible=bool(row[4]),
                    malf_context_4=_normalize_optional_str(row[5], default="UNKNOWN"),
                    lifecycle_rank_high=_normalize_optional_int(row[6]),
                    lifecycle_rank_total=_normalize_optional_int(row[7]),
                )
                for row in rows
            ]
    except duckdb.Error:
        if legacy_context_table is None or legacy_context_path is None or not legacy_context_path.exists():
            raise
    finally:
        connection.close()
    return _load_context_rows(
        malf_path=legacy_context_path,
        table_name=legacy_context_table,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        instruments=instruments,
    )


def _load_context_rows(
    *,
    malf_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> list[_ContextRow]:
    if not instruments:
        return []
    connection = duckdb.connect(str(malf_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, table_name)
        signal_date_column = _resolve_existing_column(
            available_columns,
            ("signal_date",),
            field_name="signal_date",
            table_name=table_name,
        )
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument", "entity_code", "code"),
            field_name="instrument",
            table_name=table_name,
        )
        parameters: list[object] = []
        where_clauses: list[str] = []
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{instrument_column} IN ({placeholders})")
        parameters.extend(instruments)
        if signal_start_date is not None:
            where_clauses.append(f"{signal_date_column} >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append(f"{signal_date_column} <= ?")
            parameters.append(signal_end_date)
        where_sql = f"WHERE {' AND '.join(where_clauses)}"
        select_sql = _build_context_select_sql(
            table_name=table_name,
            available_columns=available_columns,
            instrument_column=instrument_column,
            signal_date_column=signal_date_column,
            filter_sql=where_sql,
        )
        rows = connection.execute(select_sql, parameters).fetchall()
        return [
            _ContextRow(
                instrument=str(row[0]),
                signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                formal_signal_status=_normalize_formal_signal_status(row[3]),
                trigger_admissible=bool(row[4]),
                malf_context_4=_normalize_optional_str(row[5], default="UNKNOWN"),
                lifecycle_rank_high=_normalize_optional_int(row[6]),
                lifecycle_rank_total=_normalize_optional_int(row[7]),
            )
            for row in rows
        ]
    finally:
        connection.close()


def _build_context_select_sql(
    *,
    table_name: str,
    available_columns: set[str],
    instrument_column: str,
    signal_date_column: str,
    filter_sql: str,
) -> str:
    formal_signal_status_column = _resolve_optional_column(
        available_columns,
        ("formal_signal_status", "admission_status"),
    )
    trigger_admissible_column = _resolve_optional_column(
        available_columns,
        ("trigger_admissible", "filter_trigger_admissible"),
    )
    malf_context_4_column = _resolve_optional_column(available_columns, ("malf_context_4",))
    lifecycle_rank_high_column = _resolve_optional_column(available_columns, ("lifecycle_rank_high",))
    lifecycle_rank_total_column = _resolve_optional_column(available_columns, ("lifecycle_rank_total",))
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
    asof_date_column = _resolve_optional_column(available_columns, ("asof_date", "calc_date")) or signal_date_column
    status_sql = (
        f"{formal_signal_status_column} AS formal_signal_status"
        if formal_signal_status_column is not None
        else (
            f"CASE WHEN {trigger_admissible_column} THEN 'admitted' ELSE 'blocked' END AS formal_signal_status"
            if trigger_admissible_column is not None
            else "'blocked' AS formal_signal_status"
        )
    )
    admissible_sql = (
        f"COALESCE({trigger_admissible_column}, FALSE) AS trigger_admissible"
        if trigger_admissible_column is not None
        else "FALSE AS trigger_admissible"
    )
    malf_context_sql = (
        f"COALESCE({malf_context_4_column}, 'UNKNOWN') AS malf_context_4"
        if malf_context_4_column is not None
        else "'UNKNOWN' AS malf_context_4"
    )
    lifecycle_high_sql = (
        f"COALESCE({lifecycle_rank_high_column}, 0) AS lifecycle_rank_high"
        if lifecycle_rank_high_column is not None
        else "0 AS lifecycle_rank_high"
    )
    lifecycle_total_sql = (
        f"COALESCE({lifecycle_rank_total_column}, 0) AS lifecycle_rank_total"
        if lifecycle_rank_total_column is not None
        else "0 AS lifecycle_rank_total"
    )
    return f"""
        WITH ranked_context AS (
            SELECT
                {instrument_column} AS instrument,
                {signal_date_column} AS signal_date,
                {asof_date_column} AS asof_date,
                {status_sql},
                {admissible_sql},
                {malf_context_sql},
                {lifecycle_high_sql},
                {lifecycle_total_sql},
                ROW_NUMBER() OVER (
                    PARTITION BY {instrument_column}, {signal_date_column}, {asof_date_column}
                    ORDER BY {order_sql}
                ) AS row_rank
            FROM {table_name}
            {filter_sql}
        )
        SELECT
            instrument,
            signal_date,
            asof_date,
            formal_signal_status,
            trigger_admissible,
            malf_context_4,
            lifecycle_rank_high,
            lifecycle_rank_total
        FROM ranked_context
        WHERE row_rank = 1
    """


def _materialize_formal_signal_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    trigger_rows: list[_TriggerRow],
    context_map: dict[tuple[str, date, date], _ContextRow],
    signal_contract_version: str,
    producer_name: str,
    producer_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    alpha_path: Path,
    filter_path: Path,
    structure_path: Path,
    legacy_context_path: Path | None,
    source_trigger_table: str,
    source_filter_table: str,
    source_structure_table: str,
    fallback_context_table: str | None,
    batch_size: int,
) -> AlphaFormalSignalBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    missing_context_count = 0
    admitted_count = 0
    blocked_count = 0
    deferred_count = 0

    for trigger_batch in _bounded_by_instrument_batches(trigger_rows, batch_size=batch_size):
        for trigger_row in trigger_batch:
            context_row = context_map.get((trigger_row.instrument, trigger_row.signal_date, trigger_row.asof_date))
            if context_row is None:
                missing_context_count += 1
                continue
            event_row = _build_formal_signal_event_row(
                run_id=run_id,
                trigger_row=trigger_row,
                context_row=context_row,
                signal_contract_version=signal_contract_version,
            )
            materialization_action = _upsert_formal_signal_event(
                connection,
                event_row=event_row,
            )
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {ALPHA_FORMAL_SIGNAL_RUN_EVENT_TABLE} (
                    run_id,
                    signal_nk,
                    materialization_action,
                    formal_signal_status,
                    source_trigger_event_nk
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    event_row.signal_nk,
                    materialization_action,
                    event_row.formal_signal_status,
                    event_row.source_trigger_event_nk,
                ],
            )
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

            if event_row.formal_signal_status == "admitted":
                admitted_count += 1
            elif event_row.formal_signal_status == "blocked":
                blocked_count += 1
            else:
                deferred_count += 1

    materialized_signal_count = inserted_count + reused_count + rematerialized_count
    return AlphaFormalSignalBuildSummary(
        run_id=run_id,
        producer_name=producer_name,
        producer_version=producer_version,
        signal_contract_version=signal_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in trigger_rows}),
        candidate_trigger_count=len(trigger_rows),
        materialized_signal_count=materialized_signal_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        missing_context_count=missing_context_count,
        admitted_count=admitted_count,
        blocked_count=blocked_count,
        deferred_count=deferred_count,
        alpha_ledger_path=str(alpha_path),
        filter_ledger_path=str(filter_path),
        structure_ledger_path=str(structure_path),
        legacy_context_path=None if legacy_context_path is None else str(legacy_context_path),
        source_trigger_table=source_trigger_table,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
        fallback_context_table=fallback_context_table,
    )


def _bounded_by_instrument_batches(
    trigger_rows: list[_TriggerRow],
    *,
    batch_size: int,
) -> list[list[_TriggerRow]]:
    if not trigger_rows:
        return []
    normalized_batch_size = max(int(batch_size), 1)
    batches: list[list[_TriggerRow]] = []
    current_batch: list[_TriggerRow] = []
    current_instruments: set[str] = set()
    for row in trigger_rows:
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
    producer_name: str,
    producer_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    source_trigger_table: str,
    source_context_table: str,
    signal_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {ALPHA_FORMAL_SIGNAL_RUN_TABLE} (
            run_id,
            producer_name,
            producer_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_trigger_table,
            source_context_table,
            signal_contract_version,
            notes
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            producer_name,
            producer_version,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_trigger_table,
            source_context_table,
            signal_contract_version,
            "bounded alpha formal signal producer",
        ],
    )


def _build_formal_signal_event_row(
    *,
    run_id: str,
    trigger_row: _TriggerRow,
    context_row: _ContextRow,
    signal_contract_version: str,
) -> _FormalSignalEventRow:
    signal_nk = _build_signal_nk(
        instrument=trigger_row.instrument,
        signal_date=trigger_row.signal_date,
        asof_date=trigger_row.asof_date,
        trigger_family=trigger_row.trigger_family,
        trigger_type=trigger_row.trigger_type,
        pattern_code=trigger_row.pattern_code,
        source_trigger_event_nk=trigger_row.source_trigger_event_nk,
        signal_contract_version=signal_contract_version,
    )
    return _FormalSignalEventRow(
        signal_nk=signal_nk,
        instrument=trigger_row.instrument,
        signal_date=trigger_row.signal_date,
        asof_date=trigger_row.asof_date,
        trigger_family=trigger_row.trigger_family,
        trigger_type=trigger_row.trigger_type,
        pattern_code=trigger_row.pattern_code,
        formal_signal_status=context_row.formal_signal_status,
        trigger_admissible=context_row.trigger_admissible,
        malf_context_4=context_row.malf_context_4,
        lifecycle_rank_high=context_row.lifecycle_rank_high,
        lifecycle_rank_total=context_row.lifecycle_rank_total,
        source_trigger_event_nk=trigger_row.source_trigger_event_nk,
        signal_contract_version=signal_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _build_signal_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    trigger_family: str,
    trigger_type: str,
    pattern_code: str,
    source_trigger_event_nk: str,
    signal_contract_version: str,
) -> str:
    return "|".join(
        [
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            trigger_family,
            trigger_type,
            pattern_code,
            source_trigger_event_nk,
            signal_contract_version,
        ]
    )


def _upsert_formal_signal_event(
    connection: duckdb.DuckDBPyConnection,
    *,
    event_row: _FormalSignalEventRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            formal_signal_status,
            trigger_admissible,
            malf_context_4,
            lifecycle_rank_high,
            lifecycle_rank_total,
            first_seen_run_id
        FROM {ALPHA_FORMAL_SIGNAL_EVENT_TABLE}
        WHERE signal_nk = ?
        """,
        [event_row.signal_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {ALPHA_FORMAL_SIGNAL_EVENT_TABLE} (
                signal_nk,
                instrument,
                signal_date,
                asof_date,
                trigger_family,
                trigger_type,
                pattern_code,
                formal_signal_status,
                trigger_admissible,
                malf_context_4,
                lifecycle_rank_high,
                lifecycle_rank_total,
                source_trigger_event_nk,
                signal_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event_row.signal_nk,
                event_row.instrument,
                event_row.signal_date,
                event_row.asof_date,
                event_row.trigger_family,
                event_row.trigger_type,
                event_row.pattern_code,
                event_row.formal_signal_status,
                event_row.trigger_admissible,
                event_row.malf_context_4,
                event_row.lifecycle_rank_high,
                event_row.lifecycle_rank_total,
                event_row.source_trigger_event_nk,
                event_row.signal_contract_version,
                event_row.first_seen_run_id,
                event_row.last_materialized_run_id,
            ],
        )
        return "inserted"

    existing_fingerprint = (
        _normalize_formal_signal_status(existing_row[0]),
        bool(existing_row[1]),
        _normalize_optional_str(existing_row[2], default="UNKNOWN"),
        _normalize_optional_int(existing_row[3]),
        _normalize_optional_int(existing_row[4]),
    )
    new_fingerprint = (
        event_row.formal_signal_status,
        event_row.trigger_admissible,
        event_row.malf_context_4,
        event_row.lifecycle_rank_high,
        event_row.lifecycle_rank_total,
    )
    first_seen_run_id = str(existing_row[5]) if existing_row[5] is not None else event_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {ALPHA_FORMAL_SIGNAL_EVENT_TABLE}
        SET
            formal_signal_status = ?,
            trigger_admissible = ?,
            malf_context_4 = ?,
            lifecycle_rank_high = ?,
            lifecycle_rank_total = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE signal_nk = ?
        """,
        [
            event_row.formal_signal_status,
            event_row.trigger_admissible,
            event_row.malf_context_4,
            event_row.lifecycle_rank_high,
            event_row.lifecycle_rank_total,
            first_seen_run_id,
            event_row.last_materialized_run_id,
            event_row.signal_nk,
        ],
    )
    if existing_fingerprint == new_fingerprint:
        return "reused"
    return "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: AlphaFormalSignalBuildSummary,
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
        UPDATE {ALPHA_FORMAL_SIGNAL_RUN_TABLE}
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


def _normalize_formal_signal_status(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"admitted", "blocked", "deferred"}:
        return normalized
    if normalized in {"admit", "accepted"}:
        return "admitted"
    if normalized in {"reject", "rejected"}:
        return "blocked"
    return "blocked"


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


def _normalize_optional_int(value: object) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def _write_summary(summary: AlphaFormalSignalBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
