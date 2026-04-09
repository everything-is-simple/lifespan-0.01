"""执行 `alpha family ledger` 官方 bounded materialization。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.alpha.bootstrap import (
    ALPHA_FAMILY_EVENT_TABLE,
    ALPHA_FAMILY_RUN_EVENT_TABLE,
    ALPHA_FAMILY_RUN_TABLE,
    alpha_ledger_path,
    bootstrap_alpha_family_ledger,
)
from mlq.core.paths import WorkspaceRoots, default_settings


ALPHA_FAMILY_SCOPE_ALL: Final[tuple[str, ...]] = ("bof", "tst", "pb", "cpb", "bpb")
DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE: Final[str] = "alpha_trigger_event"
DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE: Final[str] = "alpha_trigger_candidate"
DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION: Final[str] = "alpha-family-v1"

_DEFAULT_FAMILY_CODE_BY_TYPE: Final[dict[str, str]] = {
    "bof": "bof_core",
    "tst": "tst_core",
    "pb": "pb_core",
    "cpb": "cpb_core",
    "bpb": "bpb_core",
}


@dataclass(frozen=True)
class AlphaFamilyBuildSummary:
    """总结一次 `alpha family ledger` runner 的 bounded 运行结果。"""

    run_id: str
    producer_name: str
    producer_version: str
    family_contract_version: str
    family_scope: list[str]
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    candidate_trigger_count: int
    materialized_family_event_count: int
    inserted_count: int
    reused_count: int
    rematerialized_count: int
    family_counts: dict[str, int]
    alpha_ledger_path: str
    source_trigger_table: str
    source_candidate_table: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


@dataclass(frozen=True)
class _TriggerRow:
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


@dataclass(frozen=True)
class _FamilyEventRow:
    family_event_nk: str
    trigger_event_nk: str
    instrument: str
    signal_date: date
    asof_date: date
    trigger_family: str
    trigger_type: str
    pattern_code: str
    family_code: str
    family_contract_version: str
    payload_json: str
    first_seen_run_id: str
    last_materialized_run_id: str


def run_alpha_family_build(
    *,
    settings: WorkspaceRoots | None = None,
    alpha_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    family_scope: list[str] | tuple[str, ...] | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    run_id: str | None = None,
    source_trigger_table: str = DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE,
    source_candidate_table: str = DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE,
    family_contract_version: str = DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION,
    producer_name: str = "alpha_family_builder",
    producer_version: str = "v1",
    summary_path: Path | None = None,
) -> AlphaFamilyBuildSummary:
    """从官方 `alpha_trigger_event` 与 bounded family candidate 输入物化 family ledger。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_family_scope = _normalize_family_scope(family_scope)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    materialization_run_id = run_id or _build_alpha_family_run_id()

    alpha_connection = duckdb.connect(str(resolved_alpha_path))
    try:
        bootstrap_alpha_family_ledger(workspace, connection=alpha_connection)
        trigger_rows = _load_trigger_rows(
            connection=alpha_connection,
            table_name=source_trigger_table,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            family_scope=normalized_family_scope,
            instruments=normalized_instruments,
            limit=normalized_limit,
        )
        candidate_map = _load_candidate_rows(
            connection=alpha_connection,
            table_name=source_candidate_table,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            family_scope=normalized_family_scope,
            instruments=tuple(sorted({row.instrument for row in trigger_rows})),
        )

        bounded_instrument_count = len({row.instrument for row in trigger_rows})
        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            producer_name=producer_name,
            producer_version=producer_version,
            family_scope=normalized_family_scope,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=bounded_instrument_count,
            source_trigger_table=source_trigger_table,
            source_candidate_table=source_candidate_table,
            family_contract_version=family_contract_version,
        )
        summary = _materialize_family_rows(
            connection=alpha_connection,
            run_id=materialization_run_id,
            trigger_rows=trigger_rows,
            candidate_map=candidate_map,
            family_contract_version=family_contract_version,
            family_scope=normalized_family_scope,
            producer_name=producer_name,
            producer_version=producer_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            alpha_path=resolved_alpha_path,
            source_trigger_table=source_trigger_table,
            source_candidate_table=source_candidate_table,
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


def _normalize_family_scope(family_scope: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    normalized = tuple(
        dict.fromkeys(
            item.strip().lower()
            for item in (family_scope or ALPHA_FAMILY_SCOPE_ALL)
            if str(item).strip()
        )
    )
    if not normalized:
        raise ValueError("Family scope cannot be empty.")
    invalid_scope = tuple(item for item in normalized if item not in ALPHA_FAMILY_SCOPE_ALL)
    if invalid_scope:
        raise ValueError(
            "Unsupported family scope: "
            + ", ".join(invalid_scope)
            + f". Supported families: {', '.join(ALPHA_FAMILY_SCOPE_ALL)}."
        )
    return normalized


def _build_alpha_family_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"alpha-family-{timestamp}"


def _load_trigger_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    family_scope: tuple[str, ...],
    instruments: tuple[str, ...],
    limit: int,
) -> list[_TriggerRow]:
    parameters: list[object] = []
    where_clauses: list[str] = []
    if signal_start_date is not None:
        where_clauses.append("signal_date >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append("signal_date <= ?")
        parameters.append(signal_end_date)
    if family_scope:
        placeholders = ", ".join("?" for _ in family_scope)
        where_clauses.append(f"LOWER(trigger_type) IN ({placeholders})")
        parameters.extend(family_scope)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"instrument IN ({placeholders})")
        parameters.extend(instruments)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = connection.execute(
        f"""
        SELECT
            trigger_event_nk,
            instrument,
            signal_date,
            asof_date,
            trigger_family,
            trigger_type,
            pattern_code,
            source_filter_snapshot_nk,
            source_structure_snapshot_nk,
            upstream_context_fingerprint
        FROM {table_name}
        {where_sql}
        ORDER BY signal_date, instrument, trigger_type, pattern_code
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        _TriggerRow(
            trigger_event_nk=str(row[0]),
            instrument=str(row[1]),
            signal_date=_normalize_date_value(row[2], field_name="signal_date"),
            asof_date=_normalize_date_value(row[3], field_name="asof_date"),
            trigger_family=_normalize_optional_str(row[4], default="PAS"),
            trigger_type=_normalize_optional_str(row[5]).lower(),
            pattern_code=_normalize_optional_str(row[6]),
            source_filter_snapshot_nk=_normalize_optional_str(row[7]),
            source_structure_snapshot_nk=_normalize_optional_str(row[8]),
            upstream_context_fingerprint=_normalize_optional_str(row[9], default="{}"),
        )
        for row in rows
    ]


def _load_candidate_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    family_scope: tuple[str, ...],
    instruments: tuple[str, ...],
) -> dict[tuple[str, date, date, str, str], dict[str, object]]:
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
    optional_payload_columns = sorted(
        column_name
        for column_name in available_columns
        if column_name
        not in {
            instrument_column,
            signal_date_column,
            asof_date_column,
            trigger_family_column,
            trigger_type_column,
            pattern_code_column,
        }
    )
    parameters: list[object] = []
    where_clauses: list[str] = []
    if signal_start_date is not None:
        where_clauses.append(f"{signal_date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{signal_date_column} <= ?")
        parameters.append(signal_end_date)
    if family_scope:
        placeholders = ", ".join("?" for _ in family_scope)
        where_clauses.append(f"LOWER({trigger_type_column}) IN ({placeholders})")
        parameters.extend(family_scope)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{instrument_column} IN ({placeholders})")
        parameters.extend(instruments)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    select_columns = [
        f"{instrument_column} AS instrument",
        f"{signal_date_column} AS signal_date",
        f"{asof_date_column} AS asof_date",
        (
            f"{trigger_family_column} AS trigger_family"
            if trigger_family_column is not None
            else "'PAS' AS trigger_family"
        ),
        f"{trigger_type_column} AS trigger_type",
        f"{pattern_code_column} AS pattern_code",
        *(f"{column_name} AS {column_name}" for column_name in optional_payload_columns),
    ]
    rows = connection.execute(
        f"""
        SELECT
            {", ".join(select_columns)}
        FROM {table_name}
        {where_sql}
        """,
        parameters,
    ).fetchall()
    column_names = [str(item[0]) for item in connection.description]
    candidate_map: dict[tuple[str, date, date, str, str], dict[str, object]] = {}
    for row in rows:
        row_dict = {
            column_name: row[index]
            for index, column_name in enumerate(column_names)
        }
        candidate_map[
            (
                _normalize_optional_str(row_dict["instrument"]),
                _normalize_date_value(row_dict["signal_date"], field_name="signal_date"),
                _normalize_date_value(row_dict["asof_date"], field_name="asof_date"),
                _normalize_optional_str(row_dict["trigger_type"]).lower(),
                _normalize_optional_str(row_dict["pattern_code"]),
            )
        ] = row_dict
    return candidate_map


def _materialize_family_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    trigger_rows: list[_TriggerRow],
    candidate_map: dict[tuple[str, date, date, str, str], dict[str, object]],
    family_contract_version: str,
    family_scope: tuple[str, ...],
    producer_name: str,
    producer_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    alpha_path: Path,
    source_trigger_table: str,
    source_candidate_table: str,
    batch_size: int,
) -> AlphaFamilyBuildSummary:
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    family_counts = {family_name: 0 for family_name in family_scope}
    for trigger_batch in _bounded_by_instrument_batches(trigger_rows, batch_size=batch_size):
        for trigger_row in trigger_batch:
            candidate_payload = candidate_map.get(
                (
                    trigger_row.instrument,
                    trigger_row.signal_date,
                    trigger_row.asof_date,
                    trigger_row.trigger_type,
                    trigger_row.pattern_code,
                )
            )
            event_row = _build_family_event_row(
                run_id=run_id,
                trigger_row=trigger_row,
                candidate_payload=candidate_payload,
                family_contract_version=family_contract_version,
            )
            materialization_action = _upsert_family_event(connection, event_row=event_row)
            connection.execute(
                f"""
                INSERT OR REPLACE INTO {ALPHA_FAMILY_RUN_EVENT_TABLE} (
                    run_id,
                    family_event_nk,
                    trigger_event_nk,
                    materialization_action,
                    family_code
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    event_row.family_event_nk,
                    event_row.trigger_event_nk,
                    materialization_action,
                    event_row.family_code,
                ],
            )
            family_counts[trigger_row.trigger_type] = family_counts.get(trigger_row.trigger_type, 0) + 1
            if materialization_action == "inserted":
                inserted_count += 1
            elif materialization_action == "reused":
                reused_count += 1
            else:
                rematerialized_count += 1
    materialized_family_event_count = inserted_count + reused_count + rematerialized_count
    return AlphaFamilyBuildSummary(
        run_id=run_id,
        producer_name=producer_name,
        producer_version=producer_version,
        family_contract_version=family_contract_version,
        family_scope=list(family_scope),
        signal_start_date=None if signal_start_date is None else signal_start_date.isoformat(),
        signal_end_date=None if signal_end_date is None else signal_end_date.isoformat(),
        bounded_instrument_count=len({row.instrument for row in trigger_rows}),
        candidate_trigger_count=len(trigger_rows),
        materialized_family_event_count=materialized_family_event_count,
        inserted_count=inserted_count,
        reused_count=reused_count,
        rematerialized_count=rematerialized_count,
        family_counts=family_counts,
        alpha_ledger_path=str(alpha_path),
        source_trigger_table=source_trigger_table,
        source_candidate_table=source_candidate_table,
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
    family_scope: tuple[str, ...],
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    source_trigger_table: str,
    source_candidate_table: str,
    family_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {ALPHA_FAMILY_RUN_TABLE} (
            run_id,
            producer_name,
            producer_version,
            run_status,
            family_scope,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_trigger_table,
            source_candidate_table,
            family_contract_version,
            notes
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            producer_name,
            producer_version,
            json.dumps(list(family_scope), ensure_ascii=False),
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_trigger_table,
            source_candidate_table,
            family_contract_version,
            "bounded alpha family ledger materialization",
        ],
    )


def _build_family_event_row(
    *,
    run_id: str,
    trigger_row: _TriggerRow,
    candidate_payload: dict[str, object] | None,
    family_contract_version: str,
) -> _FamilyEventRow:
    family_code = _resolve_family_code(trigger_row.trigger_type, candidate_payload)
    payload_json = json.dumps(
        _build_payload(
            trigger_row=trigger_row,
            family_code=family_code,
            candidate_payload=candidate_payload,
        ),
        ensure_ascii=False,
        sort_keys=True,
    )
    return _FamilyEventRow(
        family_event_nk=_build_family_event_nk(
            trigger_event_nk=trigger_row.trigger_event_nk,
            trigger_family=trigger_row.trigger_family,
            trigger_type=trigger_row.trigger_type,
            pattern_code=trigger_row.pattern_code,
            family_code=family_code,
            family_contract_version=family_contract_version,
        ),
        trigger_event_nk=trigger_row.trigger_event_nk,
        instrument=trigger_row.instrument,
        signal_date=trigger_row.signal_date,
        asof_date=trigger_row.asof_date,
        trigger_family=trigger_row.trigger_family,
        trigger_type=trigger_row.trigger_type,
        pattern_code=trigger_row.pattern_code,
        family_code=family_code,
        family_contract_version=family_contract_version,
        payload_json=payload_json,
        first_seen_run_id=run_id,
        last_materialized_run_id=run_id,
    )


def _resolve_family_code(trigger_type: str, candidate_payload: dict[str, object] | None) -> str:
    if candidate_payload is not None:
        candidate_family_code = _normalize_optional_str(candidate_payload.get("family_code"))
        if candidate_family_code:
            return candidate_family_code
    return _DEFAULT_FAMILY_CODE_BY_TYPE.get(trigger_type, f"{trigger_type}_core")


def _build_family_event_nk(
    *,
    trigger_event_nk: str,
    trigger_family: str,
    trigger_type: str,
    pattern_code: str,
    family_code: str,
    family_contract_version: str,
) -> str:
    return "|".join(
        [
            trigger_event_nk,
            trigger_family,
            trigger_type,
            pattern_code,
            family_code,
            family_contract_version,
        ]
    )


def _build_payload(
    *,
    trigger_row: _TriggerRow,
    family_code: str,
    candidate_payload: dict[str, object] | None,
) -> dict[str, object]:
    # 共享 `payload_json` 同时承载 family 最小解释层与 trigger 上游指纹，
    # 这样官方 trigger 语义变化时，family ledger 才能稳定记账 rematerialized。
    return {
        "family_code": family_code,
        "pattern_code": trigger_row.pattern_code,
        "trigger_type": trigger_row.trigger_type,
        "source_trigger": {
            "trigger_event_nk": trigger_row.trigger_event_nk,
            "source_filter_snapshot_nk": trigger_row.source_filter_snapshot_nk,
            "source_structure_snapshot_nk": trigger_row.source_structure_snapshot_nk,
            "upstream_context_fingerprint": trigger_row.upstream_context_fingerprint,
        },
        "candidate_payload": _normalize_candidate_payload(candidate_payload),
    }


def _normalize_candidate_payload(candidate_payload: dict[str, object] | None) -> dict[str, object]:
    if candidate_payload is None:
        return {}
    normalized_payload: dict[str, object] = {}
    for key, value in candidate_payload.items():
        if key in {"instrument", "signal_date", "asof_date", "trigger_family", "trigger_type", "pattern_code"}:
            continue
        normalized_payload[key] = _normalize_json_value(value)
    return normalized_payload


def _normalize_json_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _upsert_family_event(
    connection: duckdb.DuckDBPyConnection,
    *,
    event_row: _FamilyEventRow,
) -> str:
    existing_row = connection.execute(
        f"""
        SELECT
            payload_json,
            first_seen_run_id
        FROM {ALPHA_FAMILY_EVENT_TABLE}
        WHERE family_event_nk = ?
        """,
        [event_row.family_event_nk],
    ).fetchone()
    if existing_row is None:
        connection.execute(
            f"""
            INSERT INTO {ALPHA_FAMILY_EVENT_TABLE} (
                family_event_nk,
                trigger_event_nk,
                instrument,
                signal_date,
                asof_date,
                trigger_family,
                trigger_type,
                pattern_code,
                family_code,
                family_contract_version,
                payload_json,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event_row.family_event_nk,
                event_row.trigger_event_nk,
                event_row.instrument,
                event_row.signal_date,
                event_row.asof_date,
                event_row.trigger_family,
                event_row.trigger_type,
                event_row.pattern_code,
                event_row.family_code,
                event_row.family_contract_version,
                event_row.payload_json,
                event_row.first_seen_run_id,
                event_row.last_materialized_run_id,
            ],
        )
        return "inserted"
    existing_payload = _normalize_optional_str(existing_row[0], default="{}")
    first_seen_run_id = str(existing_row[1]) if existing_row[1] is not None else event_row.first_seen_run_id
    connection.execute(
        f"""
        UPDATE {ALPHA_FAMILY_EVENT_TABLE}
        SET
            payload_json = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE family_event_nk = ?
        """,
        [
            event_row.payload_json,
            first_seen_run_id,
            event_row.last_materialized_run_id,
            event_row.family_event_nk,
        ],
    )
    if existing_payload == event_row.payload_json:
        return "reused"
    return "rematerialized"


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: AlphaFamilyBuildSummary,
) -> None:
    _update_run_summary(
        connection,
        run_id=run_id,
        run_status="completed",
        materialized_family_event_count=summary.materialized_family_event_count,
        summary_payload=summary.as_dict(),
    )


def _update_run_summary(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    run_status: str,
    materialized_family_event_count: int = 0,
    summary_payload: dict[str, object],
) -> None:
    connection.execute(
        f"""
        UPDATE {ALPHA_FAMILY_RUN_TABLE}
        SET
            run_status = ?,
            materialized_family_event_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [
            run_status,
            materialized_family_event_count,
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


def _write_summary(summary: AlphaFamilyBuildSummary, summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
