"""执行 `alpha trigger ledger` 官方 bounded materialization。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.alpha.bootstrap import (
    ALPHA_TRIGGER_EVENT_TABLE,
    ALPHA_TRIGGER_RUN_EVENT_TABLE,
    ALPHA_TRIGGER_RUN_TABLE,
    alpha_ledger_path,
    bootstrap_alpha_trigger_ledger,
)
from mlq.core.paths import WorkspaceRoots, default_settings


DEFAULT_ALPHA_TRIGGER_INPUT_TABLE: Final[str] = "alpha_trigger_candidate"
DEFAULT_ALPHA_TRIGGER_FILTER_TABLE: Final[str] = "filter_snapshot"
DEFAULT_ALPHA_TRIGGER_STRUCTURE_TABLE: Final[str] = "structure_snapshot"
DEFAULT_ALPHA_TRIGGER_CONTRACT_VERSION: Final[str] = "alpha-trigger-v1"


@dataclass(frozen=True)
class AlphaTriggerBuildSummary:
    """总结一次 `alpha trigger ledger` runner 的 bounded 运行结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    trigger_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    candidate_trigger_count: int
    materialized_trigger_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    missing_context_count: int
    alpha_ledger_path: str
    filter_ledger_path: str
    structure_ledger_path: str
    source_trigger_input_table: str
    source_filter_table: str
    source_structure_table: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _TriggerInputRow:
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str


@dataclass(frozen=True)
class _OfficialContextRow:
    instrument: str
    signal_date: date
    asof_date: date
    filter_snapshot_nk: str
    structure_snapshot_nk: str
    upstream_context_fingerprint: str


@dataclass(frozen=True)
class _TriggerEventRow:
    trigger_event_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str
    source_filter_snapshot_nk: str
    source_structure_snapshot_nk: str
    upstream_context_fingerprint: str
    trigger_contract_version: str
    first_seen_run_id: str
    last_materialized_run_id: str


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
) -> AlphaTriggerBuildSummary:
    """从 bounded detector 输入与官方 `filter/structure snapshot` 物化 trigger ledger。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    resolved_filter_path = Path(filter_path or workspace.databases.filter)
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
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
            instruments=tuple(sorted({row.instrument for row in input_rows})),
        )
        context_map = {
            (row.instrument, row.signal_date, row.asof_date): row
            for row in context_rows
        }

        bounded_instrument_count = len({row.instrument for row in input_rows})
        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=bounded_instrument_count,
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
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
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


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_alpha_trigger_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"alpha-trigger-{timestamp}"


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _load_trigger_input_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_TriggerInputRow]:
    available_columns = _load_table_columns(connection, table_name)
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
    trigger_type_column = _resolve_existing_column(
        available_columns,
        ("trigger_type",),
        field_name="trigger_type",
        table_name=table_name,
    )
    trigger_family_column = _resolve_optional_column(available_columns, ("trigger_family",))
    pattern_code_column = _resolve_existing_column(
        available_columns,
        ("pattern_code", "pattern", "trigger_type"),
        field_name="pattern_code",
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
            {instrument_column} AS instrument,
            {signal_date_column} AS signal_date,
            {asof_date_column} AS asof_date,
            {("'PAS'" if trigger_family_column is None else trigger_family_column)} AS trigger_family,
            {trigger_type_column} AS trigger_type,
            {pattern_code_column} AS pattern_code
        FROM {table_name}
        {where_sql}
        ORDER BY signal_date, instrument, trigger_type, pattern_code
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        _TriggerInputRow(
            instrument=str(row[0]),
            signal_date=_normalize_date_value(row[1], field_name="signal_date"),
            asof_date=_normalize_date_value(row[2], field_name="asof_date"),
            trigger_family=_normalize_optional_str(row[3], default="PAS"),
            trigger_type=_normalize_optional_str(row[4]),
            pattern_code=_normalize_optional_str(row[5]),
        )
        for row in rows
    ]


def _load_official_context_rows(
    *,
    filter_path: Path,
    structure_path: Path,
    filter_table_name: str,
    structure_table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
) -> list[_OfficialContextRow]:
    if not instruments:
        return []
    connection = duckdb.connect(str(filter_path), read_only=True)
    try:
        structure_path_sql = str(structure_path).replace("\\", "/").replace("'", "''")
        connection.execute(f"ATTACH '{structure_path_sql}' AS structure_db")
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"f.instrument IN ({placeholders})"]
        if signal_start_date is not None:
            where_clauses.append("f.signal_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("f.signal_date <= ?")
            parameters.append(signal_end_date)
        rows = connection.execute(
            f"""
            WITH ranked_filter AS (
                SELECT
                    f.filter_snapshot_nk,
                    f.structure_snapshot_nk,
                    f.instrument,
                    f.signal_date,
                    f.asof_date,
                    f.trigger_admissible,
                    f.primary_blocking_condition,
                    f.blocking_conditions_json,
                    f.admission_notes,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.instrument, f.signal_date, f.asof_date
                        ORDER BY f.updated_at DESC, f.last_materialized_run_id DESC
                    ) AS row_rank
                FROM {filter_table_name} AS f
                WHERE {' AND '.join(where_clauses)}
            )
            SELECT
                rf.instrument,
                rf.signal_date,
                rf.asof_date,
                rf.filter_snapshot_nk,
                rf.structure_snapshot_nk,
                rf.trigger_admissible,
                rf.primary_blocking_condition,
                rf.blocking_conditions_json,
                rf.admission_notes,
                s.structure_progress_state,
                s.is_failed_extreme,
                s.failure_type
            FROM ranked_filter AS rf
            INNER JOIN structure_db.main.{structure_table_name} AS s
                ON s.structure_snapshot_nk = rf.structure_snapshot_nk
            WHERE rf.row_rank = 1
            """,
            parameters,
        ).fetchall()
        context_rows: list[_OfficialContextRow] = []
        for row in rows:
            fingerprint = json.dumps(
                {
                    "filter_snapshot_nk": str(row[3]),
                    "structure_snapshot_nk": str(row[4]),
                    "trigger_admissible": bool(row[5]),
                    "primary_blocking_condition": _normalize_optional_nullable_str(row[6]),
                    "blocking_conditions_json": _normalize_optional_str(row[7], default="[]"),
                    "admission_notes": _normalize_optional_nullable_str(row[8]),
                    "structure_progress_state": _normalize_optional_str(row[9], default="unknown"),
                    "is_failed_extreme": bool(row[10]),
                    "failure_type": _normalize_optional_nullable_str(row[11]),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            context_rows.append(
                _OfficialContextRow(
                    instrument=str(row[0]),
                    signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                    asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                    filter_snapshot_nk=str(row[3]),
                    structure_snapshot_nk=str(row[4]),
                    upstream_context_fingerprint=fingerprint,
                )
            )
        return context_rows
    finally:
        connection.close()


def _materialize_trigger_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    input_rows: list[_TriggerInputRow],
    context_map: dict[tuple[str, date, date], _OfficialContextRow],
    trigger_contract_version: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    alpha_path: Path,
    filter_path: Path,
    structure_path: Path,
    source_trigger_input_table: str,
    source_filter_table: str,
    source_structure_table: str,
    batch_size: int,
) -> AlphaTriggerBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    missing_context_count = 0

    for input_batch in _bounded_by_instrument_batches(input_rows, batch_size=batch_size):
        for input_row in input_batch:
            context_row = context_map.get((input_row.instrument, input_row.signal_date, input_row.asof_date))
            if context_row is None:
                missing_context_count += 1
                continue
            event_row = _build_trigger_event_row(
                run_id=run_id,
                input_row=input_row,
                context_row=context_row,
                trigger_contract_version=trigger_contract_version,
            )
            materialization_action = _upsert_trigger_event(connection, event_row=event_row)
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {ALPHA_TRIGGER_RUN_EVENT_TABLE} (
                    run_id,
                    trigger_event_nk,
                    materialization_action,
                    trigger_type
                )
                VALUES (?, ?, ?, ?)
                """,
                [
                    run_id,
                    event_row.trigger_event_nk,
                    materialization_action,
                    event_row.trigger_type,
                ],
            )
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1

    materialized_trigger_count = inserted_count + reused_count + rematerialized_count
    return AlphaTriggerBuildSummary(
        run_id=run_id,
        runner_name=runner_name,
        runner_version=runner_version,
        trigger_contract_version=trigger_contract_version,
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in input_rows}),
        candidate_trigger_count=len(input_rows),
        materialized_trigger_count=materialized_trigger_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        missing_context_count=missing_context_count,
        alpha_ledger_path=str(alpha_path),
        filter_ledger_path=str(filter_path),
        structure_ledger_path=str(structure_path),
        source_trigger_input_table=source_trigger_input_table,
        source_filter_table=source_filter_table,
        source_structure_table=source_structure_table,
    )


def _bounded_by_instrument_batches(
    input_rows: list[_TriggerInputRow],
    *,
    batch_size: int,
) -> list[list[_TriggerInputRow]]:
    if not input_rows:
        return []
    normalized_batch_size = max(int(batch_size), 1)
    batches: list[list[_TriggerInputRow]] = []
    current_batch: list[_TriggerInputRow] = []
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
    candidate_trigger_count: int,
    source_trigger_input_table: str,
    source_filter_table: str,
    source_structure_table: str,
    trigger_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {ALPHA_TRIGGER_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            candidate_trigger_count,
            source_trigger_input_table,
            source_filter_table,
            source_structure_table,
            trigger_contract_version,
            notes
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            candidate_trigger_count,
            source_trigger_input_table,
            source_filter_table,
            source_structure_table,
            trigger_contract_version,
            "bounded alpha trigger ledger materialization",
        ],
    )


def _build_trigger_event_row(
    *,
    run_id: str,
    input_row: _TriggerInputRow,
    context_row: _OfficialContextRow,
    trigger_contract_version: str,
) -> _TriggerEventRow:
    trigger_event_nk = _build_trigger_event_nk(
        instrument=input_row.instrument,
        signal_date=input_row.signal_date,
        asof_date=input_row.asof_date,
        trigger_family=input_row.trigger_family,
        trigger_type=input_row.trigger_type,
        pattern_code=input_row.pattern_code,
        trigger_contract_version=trigger_contract_version,
    )
    return _TriggerEventRow(
        trigger_event_nk=trigger_event_nk,
        instrument=input_row.instrument,
        signal_date=input_row.signal_date,
        asof_date=input_row.asof_date,
        trigger_family=input_row.trigger_family,
        trigger_type=input_row.trigger_type,
        pattern_code=input_row.pattern_code,
        source_filter_snapshot_nk=context_row.filter_snapshot_nk,
        source_structure_snapshot_nk=context_row.structure_snapshot_nk,
        upstream_context_fingerprint=context_row.upstream_context_fingerprint,
        trigger_contract_version=trigger_contract_version,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _build_trigger_event_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    trigger_family: str,
    trigger_type: str,
    pattern_code: str,
    trigger_contract_version: str,
) -> str:
    return "|".join(
        [
            instrument,
            signal_date.isoformat(),
            asof_date.isoformat(),
            trigger_family,
            trigger_type,
            pattern_code,
            trigger_contract_version,
        ]
    )


def _upsert_trigger_event(
    connection: duckdb.DuckDBPyConnection,
    *,
    event_row: _TriggerEventRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            source_filter_snapshot_nk,
            source_structure_snapshot_nk,
            upstream_context_fingerprint,
            first_seen_run_id
        FROM {ALPHA_TRIGGER_EVENT_TABLE}
        WHERE trigger_event_nk = ?
        """,
        [event_row.trigger_event_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {ALPHA_TRIGGER_EVENT_TABLE} (
                trigger_event_nk,
                instrument,
                signal_date,
                asof_date,
                trigger_family,
                trigger_type,
                pattern_code,
                source_filter_snapshot_nk,
                source_structure_snapshot_nk,
                upstream_context_fingerprint,
                trigger_contract_version,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event_row.trigger_event_nk,
                event_row.instrument,
                event_row.signal_date,
                event_row.asof_date,
                event_row.trigger_family,
                event_row.trigger_type,
                event_row.pattern_code,
                event_row.source_filter_snapshot_nk,
                event_row.source_structure_snapshot_nk,
                event_row.upstream_context_fingerprint,
                event_row.trigger_contract_version,
                event_row.first_seen_run_id,
                event_row.last_materialized_run_id,
            ],
        )
        return "inserted"

    existing_fingerprint = (
        _normalize_optional_str(existing_row[0]),
        _normalize_optional_str(existing_row[1]),
        _normalize_optional_str(existing_row[2]),
    )
    new_fingerprint = (
        event_row.source_filter_snapshot_nk,
        event_row.source_structure_snapshot_nk,
        event_row.upstream_context_fingerprint,
    )
    first_seen_run_id = str(existing_row[3]) if existing_row[3] is not None else event_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {ALPHA_TRIGGER_EVENT_TABLE}
        SET
            source_filter_snapshot_nk = ?,
            source_structure_snapshot_nk = ?,
            upstream_context_fingerprint = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE trigger_event_nk = ?
        """,
        [
            event_row.source_filter_snapshot_nk,
            event_row.source_structure_snapshot_nk,
            event_row.upstream_context_fingerprint,
            first_seen_run_id,
            event_row.last_materialized_run_id,
            event_row.trigger_event_nk,
        ],
    )
    if existing_fingerprint == new_fingerprint:
        return "reused"
    return "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: AlphaTriggerBuildSummary,
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
        UPDATE {ALPHA_TRIGGER_RUN_TABLE}
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


def _write_summary(summary: AlphaTriggerBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
