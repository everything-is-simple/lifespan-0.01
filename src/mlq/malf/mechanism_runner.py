"""执行 `malf` 机制层 sidecar 账本的最小 bounded runner。"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.malf.bootstrap import (
    MALF_MECHANISM_CHECKPOINT_TABLE,
    MALF_MECHANISM_RUN_TABLE,
    PAS_CONTEXT_SNAPSHOT_TABLE,
    PIVOT_CONFIRMED_BREAK_LEDGER_TABLE,
    SAME_TIMEFRAME_STATS_PROFILE_TABLE,
    SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE,
    STRUCTURE_CANDIDATE_SNAPSHOT_TABLE,
    bootstrap_malf_ledger,
    malf_ledger_path,
)


DEFAULT_MECHANISM_TIMEFRAME: Final[str] = "D"
DEFAULT_MECHANISM_SAMPLE_VERSION: Final[str] = "bridge-v1"
DEFAULT_MECHANISM_CONTRACT_VERSION: Final[str] = "malf-mechanism-v1"

_METRIC_FIELDS: Final[tuple[str, ...]] = (
    "new_high_count",
    "new_low_count",
    "refresh_density",
    "advancement_density",
)


@dataclass(frozen=True)
class MalfMechanismBuildSummary:
    """总结一次机制层 sidecar bounded runner 的执行结果。"""

    run_id: str
    runner_name: str
    runner_version: str
    timeframe: str
    stats_sample_version: str
    mechanism_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    source_candidate_count: int
    break_ledger_count: int
    stats_profile_count: int
    stats_snapshot_count: int
    break_inserted_count: int
    break_reused_count: int
    break_rematerialized_count: int
    profile_inserted_count: int
    profile_reused_count: int
    profile_rematerialized_count: int
    snapshot_inserted_count: int
    snapshot_reused_count: int
    snapshot_rematerialized_count: int
    checkpoint_upserted_count: int
    confirmed_break_count: int
    pending_break_count: int
    malf_ledger_path: str
    source_context_table: str
    source_structure_input_table: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _MechanismInputRow:
    instrument: str
    signal_date: date
    asof_date: date
    source_context_nk: str
    source_candidate_nk: str
    malf_context_4: str
    new_high_count: int
    new_low_count: int
    refresh_density: float
    advancement_density: float
    is_failed_extreme: bool
    failure_type: str | None


def run_malf_mechanism_build(
    *,
    settings: WorkspaceRoots | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 500,
    batch_size: int = 100,
    timeframe: str = DEFAULT_MECHANISM_TIMEFRAME,
    stats_sample_version: str = DEFAULT_MECHANISM_SAMPLE_VERSION,
    mechanism_contract_version: str = DEFAULT_MECHANISM_CONTRACT_VERSION,
    run_id: str | None = None,
    source_context_table: str = PAS_CONTEXT_SNAPSHOT_TABLE,
    source_structure_input_table: str = STRUCTURE_CANDIDATE_SNAPSHOT_TABLE,
    runner_name: str = "malf_mechanism_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
) -> MalfMechanismBuildSummary:
    """从 bridge v1 `malf` 输入物化 break/stats sidecar 历史账本。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    resolved_malf_path = Path(malf_path or malf_ledger_path(workspace))
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    materialization_run_id = run_id or _build_run_id(prefix="malf-mechanism")

    if not resolved_malf_path.exists():
        raise FileNotFoundError(f"Missing malf database: {resolved_malf_path}")

    connection = duckdb.connect(str(resolved_malf_path))
    try:
        bootstrap_malf_ledger(workspace, connection=connection)
        _insert_run_row(
            connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=0,
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
            timeframe=timeframe,
            stats_sample_version=stats_sample_version,
            mechanism_contract_version=mechanism_contract_version,
        )
        checkpoint_map = _load_checkpoint_map(connection, timeframe=timeframe)
        input_rows = _load_mechanism_input_rows(
            connection=connection,
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=normalized_instruments,
            limit=normalized_limit,
            batch_size=normalized_batch_size,
            checkpoint_map=checkpoint_map,
            timeframe=timeframe,
        )
        break_rows = _derive_break_rows(
            input_rows=input_rows,
            timeframe=timeframe,
            run_id=materialization_run_id,
        )
        profile_rows, metric_samples = _derive_profile_rows(
            input_rows=input_rows,
            timeframe=timeframe,
            stats_sample_version=stats_sample_version,
            run_id=materialization_run_id,
        )
        snapshot_rows = _derive_snapshot_rows(
            input_rows=input_rows,
            timeframe=timeframe,
            stats_sample_version=stats_sample_version,
            mechanism_contract_version=mechanism_contract_version,
            run_id=materialization_run_id,
            profile_rows=profile_rows,
            metric_samples=metric_samples,
        )
        counts = _materialize_rows(
            connection=connection,
            break_rows=break_rows,
            profile_rows=profile_rows,
            snapshot_rows=snapshot_rows,
        )
        checkpoint_upserted_count = _upsert_checkpoints(
            connection,
            input_rows=input_rows,
            timeframe=timeframe,
            run_id=materialization_run_id,
        )
        bounded_instrument_count = len({row.instrument for row in input_rows})
        summary = MalfMechanismBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            timeframe=timeframe,
            stats_sample_version=stats_sample_version,
            mechanism_contract_version=mechanism_contract_version,
            signal_start_date=None if normalized_start_date is None else normalized_start_date.isoformat(),
            signal_end_date=None if normalized_end_date is None else normalized_end_date.isoformat(),
            bounded_instrument_count=bounded_instrument_count,
            source_candidate_count=len(input_rows),
            break_ledger_count=len(break_rows),
            stats_profile_count=len(profile_rows),
            stats_snapshot_count=len(snapshot_rows),
            break_inserted_count=counts["break_inserted_count"],
            break_reused_count=counts["break_reused_count"],
            break_rematerialized_count=counts["break_rematerialized_count"],
            profile_inserted_count=counts["profile_inserted_count"],
            profile_reused_count=counts["profile_reused_count"],
            profile_rematerialized_count=counts["profile_rematerialized_count"],
            snapshot_inserted_count=counts["snapshot_inserted_count"],
            snapshot_reused_count=counts["snapshot_reused_count"],
            snapshot_rematerialized_count=counts["snapshot_rematerialized_count"],
            checkpoint_upserted_count=checkpoint_upserted_count,
            confirmed_break_count=sum(1 for row in break_rows if row["confirmation_status"] == "confirmed"),
            pending_break_count=sum(1 for row in break_rows if row["confirmation_status"] != "confirmed"),
            malf_ledger_path=str(resolved_malf_path),
            source_context_table=source_context_table,
            source_structure_input_table=source_structure_input_table,
        )
        _mark_run_completed(connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception:
        _mark_run_failed(connection, run_id=materialization_run_id)
        raise
    finally:
        connection.close()


def _normalize_instruments(instruments: list[str] | tuple[str, ...] | None) -> set[str]:
    normalized: set[str] = set()
    for instrument in instruments or ():
        candidate = str(instrument).strip().upper()
        if not candidate:
            continue
        normalized.add(candidate)
        if "." in candidate:
            normalized.add(candidate.split(".", 1)[0])
    return normalized


def _load_mechanism_input_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_context_table: str,
    source_structure_input_table: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
    batch_size: int,
    checkpoint_map: dict[tuple[str, str], tuple[date | None, date | None]],
    timeframe: str,
) -> list[_MechanismInputRow]:
    available_context_columns = _load_table_columns(connection, source_context_table)
    available_structure_columns = _load_table_columns(connection, source_structure_input_table)
    instrument_column = _resolve_existing_column(
        available_structure_columns,
        ("instrument", "entity_code", "code"),
        field_name="instrument",
        table_name=source_structure_input_table,
    )
    signal_date_column = _resolve_existing_column(
        available_structure_columns,
        ("signal_date",),
        field_name="signal_date",
        table_name=source_structure_input_table,
    )
    asof_date_column = _resolve_optional_column(available_structure_columns, ("asof_date",)) or signal_date_column
    structure_parameters: list[object] = []
    structure_where: list[str] = []
    if signal_start_date is not None:
        structure_where.append(f"s.{signal_date_column} >= ?")
        structure_parameters.append(signal_start_date)
    if signal_end_date is not None:
        structure_where.append(f"s.{signal_date_column} <= ?")
        structure_parameters.append(signal_end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        structure_where.append(f"s.{instrument_column} IN ({placeholders})")
        structure_parameters.extend(instruments)
    where_sql = f"WHERE {' AND '.join(structure_where)}" if structure_where else ""
    limit_sql = max(int(limit), 1) * max(int(batch_size), 1)
    rows = connection.execute(
        f"""
        WITH ranked_context AS (
            SELECT
                {_resolve_existing_column(available_context_columns, ("entity_code", "instrument", "code"), field_name="instrument", table_name=source_context_table)} AS instrument,
                {_resolve_existing_column(available_context_columns, ("signal_date",), field_name="signal_date", table_name=source_context_table)} AS signal_date,
                {(_resolve_optional_column(available_context_columns, ("asof_date", "calc_date")) or _resolve_existing_column(available_context_columns, ("signal_date",), field_name="signal_date", table_name=source_context_table))} AS asof_date,
                COALESCE({_resolve_optional_column(available_context_columns, ("source_context_nk",)) or "NULL"}, '') AS source_context_nk,
                COALESCE({_resolve_optional_column(available_context_columns, ("malf_context_4",)) or "'UNKNOWN'"}, 'UNKNOWN') AS malf_context_4,
                ROW_NUMBER() OVER (
                    PARTITION BY {_resolve_existing_column(available_context_columns, ("entity_code", "instrument", "code"), field_name="instrument", table_name=source_context_table)},
                                 {_resolve_existing_column(available_context_columns, ("signal_date",), field_name="signal_date", table_name=source_context_table)},
                                 {(_resolve_optional_column(available_context_columns, ("asof_date", "calc_date")) or _resolve_existing_column(available_context_columns, ("signal_date",), field_name="signal_date", table_name=source_context_table))}
                    ORDER BY COALESCE({_resolve_optional_column(available_context_columns, ("updated_at", "created_at")) or "CURRENT_TIMESTAMP"}, CURRENT_TIMESTAMP) DESC
                ) AS row_rank
            FROM {source_context_table}
        )
        SELECT
            s.{instrument_column} AS instrument,
            s.{signal_date_column} AS signal_date,
            s.{asof_date_column} AS asof_date,
            COALESCE(c.source_context_nk, '') AS source_context_nk,
            COALESCE({_resolve_optional_column(available_structure_columns, ("candidate_nk",)) or "NULL"}, '') AS source_candidate_nk,
            COALESCE(c.malf_context_4, 'UNKNOWN') AS malf_context_4,
            COALESCE({_resolve_optional_column(available_structure_columns, ("new_high_count",)) or "0"}, 0) AS new_high_count,
            COALESCE({_resolve_optional_column(available_structure_columns, ("new_low_count",)) or "0"}, 0) AS new_low_count,
            COALESCE({_resolve_optional_column(available_structure_columns, ("refresh_density",)) or "0.0"}, 0.0) AS refresh_density,
            COALESCE({_resolve_optional_column(available_structure_columns, ("advancement_density",)) or "0.0"}, 0.0) AS advancement_density,
            COALESCE({_resolve_optional_column(available_structure_columns, ("is_failed_extreme",)) or "FALSE"}, FALSE) AS is_failed_extreme,
            {_resolve_optional_column(available_structure_columns, ("failure_type",)) or "NULL"} AS failure_type
        FROM {source_structure_input_table} AS s
        LEFT JOIN ranked_context AS c
          ON c.instrument = s.{instrument_column}
         AND c.signal_date = s.{signal_date_column}
         AND c.asof_date = s.{asof_date_column}
         AND c.row_rank = 1
        {where_sql}
        ORDER BY s.{signal_date_column}, s.{instrument_column}, s.{asof_date_column}
        LIMIT ?
        """,
        [*structure_parameters, limit_sql],
    ).fetchall()
    input_rows = [
        _MechanismInputRow(
            instrument=str(row[0]),
            signal_date=_normalize_date_value(row[1], field_name="signal_date"),
            asof_date=_normalize_date_value(row[2], field_name="asof_date"),
            source_context_nk=_normalize_optional_str(
                row[3],
                default=_build_source_context_nk(
                    instrument=str(row[0]),
                    signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                    asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                    malf_context_4=_normalize_optional_str(row[5], default="UNKNOWN"),
                ),
            ),
            source_candidate_nk=_normalize_optional_str(
                row[4],
                default=_build_source_candidate_nk(
                    instrument=str(row[0]),
                    signal_date=_normalize_date_value(row[1], field_name="signal_date"),
                    asof_date=_normalize_date_value(row[2], field_name="asof_date"),
                ),
            ),
            malf_context_4=_normalize_optional_str(row[5], default="UNKNOWN"),
            new_high_count=_normalize_optional_int(row[6]),
            new_low_count=_normalize_optional_int(row[7]),
            refresh_density=_normalize_optional_float(row[8]),
            advancement_density=_normalize_optional_float(row[9]),
            is_failed_extreme=bool(row[10]),
            failure_type=_normalize_optional_nullable_str(row[11]),
        )
        for row in rows
    ]
    if signal_start_date is not None or signal_end_date is not None:
        return input_rows
    return [
        row
        for row in input_rows
        if _should_process_after_checkpoint(
            row,
            timeframe=timeframe,
            checkpoint_map=checkpoint_map,
        )
    ]


def _should_process_after_checkpoint(
    row: _MechanismInputRow,
    *,
    timeframe: str,
    checkpoint_map: dict[tuple[str, str], tuple[date | None, date | None]],
) -> bool:
    last_signal_date, last_asof_date = checkpoint_map.get((row.instrument, timeframe), (None, None))
    if last_signal_date is None and last_asof_date is None:
        return True
    if last_signal_date is not None and row.signal_date > last_signal_date:
        return True
    if last_signal_date is not None and row.signal_date < last_signal_date:
        return False
    if last_asof_date is not None and row.asof_date <= last_asof_date:
        return False
    return True


def _load_checkpoint_map(
    connection: duckdb.DuckDBPyConnection,
    *,
    timeframe: str,
) -> dict[tuple[str, str], tuple[date | None, date | None]]:
    rows = connection.execute(
        f"""
        SELECT instrument, timeframe, last_signal_date, last_asof_date
        FROM {MALF_MECHANISM_CHECKPOINT_TABLE}
        WHERE timeframe = ?
        """,
        [timeframe],
    ).fetchall()
    return {
        (str(row[0]), str(row[1])): (
            _coerce_date(row[2]),
            _coerce_date(row[3]),
        )
        for row in rows
    }


def _derive_break_rows(
    *,
    input_rows: list[_MechanismInputRow],
    timeframe: str,
    run_id: str,
) -> list[dict[str, object]]:
    break_rows: list[dict[str, object]] = []
    grouped_rows = _group_rows_by_instrument(input_rows)
    for instrument_rows in grouped_rows.values():
        for index, row in enumerate(instrument_rows):
            if not row.is_failed_extreme and row.failure_type is None:
                continue
            direction = _derive_break_direction(row)
            confirmation_status, confirmation_row = _derive_confirmation(
                trigger_row=row,
                direction=direction,
                future_rows=instrument_rows[index + 1 : index + 4],
            )
            guard_pivot_id = _build_guard_pivot_id(
                instrument=row.instrument,
                timeframe=timeframe,
                signal_date=row.signal_date,
                direction=direction,
            )
            break_rows.append(
                {
                    "break_event_nk": _build_break_event_nk(
                        instrument=row.instrument,
                        timeframe=timeframe,
                        guard_pivot_id=guard_pivot_id,
                        trigger_bar_dt=row.signal_date,
                    ),
                    "instrument": row.instrument,
                    "timeframe": timeframe,
                    "guard_pivot_id": guard_pivot_id,
                    "guard_pivot_role": "LOW_GUARD" if direction == "DOWN" else "HIGH_GUARD",
                    "origin_context": row.malf_context_4,
                    "trigger_bar_dt": row.signal_date,
                    "trigger_price_proxy": float(row.new_high_count - row.new_low_count),
                    "break_direction": direction,
                    "confirmation_status": confirmation_status,
                    "confirmation_bar_dt": None if confirmation_row is None else confirmation_row.signal_date,
                    "confirmation_pivot_id": None
                    if confirmation_row is None
                    else _build_confirmation_pivot_id(
                        instrument=confirmation_row.instrument,
                        timeframe=timeframe,
                        signal_date=confirmation_row.signal_date,
                        direction=direction,
                    ),
                    "confirmation_pivot_role": None if confirmation_row is None else "CONFIRMED_PROGRESS",
                    "source_context_nk": row.source_context_nk,
                    "source_candidate_nk": row.source_candidate_nk,
                    "first_seen_run_id": run_id,
                    "last_materialized_run_id": run_id,
                }
            )
    return break_rows


def _derive_profile_rows(
    *,
    input_rows: list[_MechanismInputRow],
    timeframe: str,
    stats_sample_version: str,
    run_id: str,
) -> tuple[list[dict[str, object]], dict[tuple[str, str], list[float]]]:
    grouped_samples: dict[tuple[str, str], list[float]] = {}
    for row in input_rows:
        for metric_name in _METRIC_FIELDS:
            grouped_samples.setdefault((row.malf_context_4, metric_name), []).append(float(getattr(row, metric_name)))

    profile_rows: list[dict[str, object]] = []
    for (regime_family, metric_name), samples in sorted(grouped_samples.items()):
        ordered_samples = sorted(samples)
        stats_profile_nk = _build_stats_profile_nk(
            universe="ALL",
            timeframe=timeframe,
            regime_family=regime_family,
            metric_name=metric_name,
            sample_version=stats_sample_version,
        )
        profile_rows.append(
            {
                "stats_profile_nk": stats_profile_nk,
                "universe": "ALL",
                "timeframe": timeframe,
                "regime_family": regime_family,
                "metric_name": metric_name,
                "sample_version": stats_sample_version,
                "sample_size": len(ordered_samples),
                "p10": _percentile(ordered_samples, 0.10),
                "p25": _percentile(ordered_samples, 0.25),
                "p50": _percentile(ordered_samples, 0.50),
                "p75": _percentile(ordered_samples, 0.75),
                "p90": _percentile(ordered_samples, 0.90),
                "mean": mean(ordered_samples) if ordered_samples else 0.0,
                "std": pstdev(ordered_samples) if len(ordered_samples) > 1 else 0.0,
                "bucket_definition_json": json.dumps(
                    {
                        "elevated_at_or_above": "p75",
                        "high_at_or_above": "p90",
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "first_seen_run_id": run_id,
                "last_materialized_run_id": run_id,
            }
        )
    return profile_rows, grouped_samples


def _derive_snapshot_rows(
    *,
    input_rows: list[_MechanismInputRow],
    timeframe: str,
    stats_sample_version: str,
    mechanism_contract_version: str,
    run_id: str,
    profile_rows: list[dict[str, object]],
    metric_samples: dict[tuple[str, str], list[float]],
) -> list[dict[str, object]]:
    profile_refs = {
        (str(row["regime_family"]), str(row["metric_name"])): str(row["stats_profile_nk"])
        for row in profile_rows
    }
    snapshot_rows: list[dict[str, object]] = []
    for row in input_rows:
        metric_percentiles = {
            metric_name: _empirical_percentile(
                metric_samples.get((row.malf_context_4, metric_name), []),
                float(getattr(row, metric_name)),
            )
            for metric_name in _METRIC_FIELDS
        }
        exhaustion_signal = max(
            metric_percentiles["new_high_count"],
            metric_percentiles["new_low_count"],
            metric_percentiles["advancement_density"],
        )
        reversal_signal = max(
            metric_percentiles["refresh_density"],
            1.0 if row.is_failed_extreme else 0.0,
        )
        snapshot_rows.append(
            {
                "stats_snapshot_nk": _build_stats_snapshot_nk(
                    instrument=row.instrument,
                    timeframe=timeframe,
                    asof_bar_dt=row.asof_date,
                    sample_version=stats_sample_version,
                    mechanism_contract_version=mechanism_contract_version,
                ),
                "instrument": row.instrument,
                "timeframe": timeframe,
                "signal_date": row.signal_date,
                "asof_bar_dt": row.asof_date,
                "regime_family": row.malf_context_4,
                "sample_version": stats_sample_version,
                "stats_contract_version": mechanism_contract_version,
                "source_context_nk": row.source_context_nk,
                "source_candidate_nk": row.source_candidate_nk,
                "new_high_count_percentile": metric_percentiles["new_high_count"],
                "new_low_count_percentile": metric_percentiles["new_low_count"],
                "refresh_density_percentile": metric_percentiles["refresh_density"],
                "advancement_density_percentile": metric_percentiles["advancement_density"],
                "exhaustion_risk_bucket": _percentile_bucket(exhaustion_signal),
                "reversal_probability_bucket": _percentile_bucket(reversal_signal),
                "source_profile_refs_json": json.dumps(
                    {
                        metric_name: profile_refs.get((row.malf_context_4, metric_name))
                        for metric_name in _METRIC_FIELDS
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "first_seen_run_id": run_id,
                "last_materialized_run_id": run_id,
            }
        )
    return snapshot_rows


def _materialize_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    break_rows: list[dict[str, object]],
    profile_rows: list[dict[str, object]],
    snapshot_rows: list[dict[str, object]],
) -> dict[str, int]:
    counts = {
        "break_inserted_count": 0,
        "break_reused_count": 0,
        "break_rematerialized_count": 0,
        "profile_inserted_count": 0,
        "profile_reused_count": 0,
        "profile_rematerialized_count": 0,
        "snapshot_inserted_count": 0,
        "snapshot_reused_count": 0,
        "snapshot_rematerialized_count": 0,
    }
    for row in break_rows:
        counts[f"break_{_upsert_break_row(connection, row=row)}_count"] += 1
    for row in profile_rows:
        counts[f"profile_{_upsert_profile_row(connection, row=row)}_count"] += 1
    for row in snapshot_rows:
        counts[f"snapshot_{_upsert_snapshot_row(connection, row=row)}_count"] += 1
    return counts


def _upsert_break_row(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT
            guard_pivot_role,
            origin_context,
            break_direction,
            confirmation_status,
            confirmation_bar_dt,
            confirmation_pivot_id,
            confirmation_pivot_role,
            source_context_nk,
            source_candidate_nk,
            first_seen_run_id
        FROM {PIVOT_CONFIRMED_BREAK_LEDGER_TABLE}
        WHERE break_event_nk = ?
        """,
        [row["break_event_nk"]],
    ).fetchone()
    fingerprint = (
        str(row["guard_pivot_role"]),
        str(row["origin_context"]),
        str(row["break_direction"]),
        str(row["confirmation_status"]),
        _coerce_date(row["confirmation_bar_dt"]),
        _normalize_optional_nullable_str(row["confirmation_pivot_id"]),
        _normalize_optional_nullable_str(row["confirmation_pivot_role"]),
        str(row["source_context_nk"]),
        str(row["source_candidate_nk"]),
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {PIVOT_CONFIRMED_BREAK_LEDGER_TABLE} (
                break_event_nk,
                instrument,
                timeframe,
                guard_pivot_id,
                guard_pivot_role,
                origin_context,
                trigger_bar_dt,
                trigger_price_proxy,
                break_direction,
                confirmation_status,
                confirmation_bar_dt,
                confirmation_pivot_id,
                confirmation_pivot_role,
                source_context_nk,
                source_candidate_nk,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [row[column] for column in (
                "break_event_nk",
                "instrument",
                "timeframe",
                "guard_pivot_id",
                "guard_pivot_role",
                "origin_context",
                "trigger_bar_dt",
                "trigger_price_proxy",
                "break_direction",
                "confirmation_status",
                "confirmation_bar_dt",
                "confirmation_pivot_id",
                "confirmation_pivot_role",
                "source_context_nk",
                "source_candidate_nk",
                "first_seen_run_id",
                "last_materialized_run_id",
            )],
        )
        return "inserted"
    existing_fingerprint = (
        _normalize_optional_str(existing[0]),
        _normalize_optional_str(existing[1]),
        _normalize_optional_str(existing[2]),
        _normalize_optional_str(existing[3]),
        _coerce_date(existing[4]),
        _normalize_optional_nullable_str(existing[5]),
        _normalize_optional_nullable_str(existing[6]),
        _normalize_optional_str(existing[7]),
        _normalize_optional_str(existing[8]),
    )
    first_seen_run_id = _normalize_optional_str(existing[9], default=str(row["first_seen_run_id"]))
    connection.execute(
        f"""
        UPDATE {PIVOT_CONFIRMED_BREAK_LEDGER_TABLE}
        SET
            guard_pivot_role = ?,
            origin_context = ?,
            break_direction = ?,
            confirmation_status = ?,
            confirmation_bar_dt = ?,
            confirmation_pivot_id = ?,
            confirmation_pivot_role = ?,
            source_context_nk = ?,
            source_candidate_nk = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE break_event_nk = ?
        """,
        [
            row["guard_pivot_role"],
            row["origin_context"],
            row["break_direction"],
            row["confirmation_status"],
            row["confirmation_bar_dt"],
            row["confirmation_pivot_id"],
            row["confirmation_pivot_role"],
            row["source_context_nk"],
            row["source_candidate_nk"],
            first_seen_run_id,
            row["last_materialized_run_id"],
            row["break_event_nk"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _upsert_profile_row(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT
            sample_size, p10, p25, p50, p75, p90, mean, std, bucket_definition_json, first_seen_run_id
        FROM {SAME_TIMEFRAME_STATS_PROFILE_TABLE}
        WHERE stats_profile_nk = ?
        """,
        [row["stats_profile_nk"]],
    ).fetchone()
    fingerprint = tuple(row[key] for key in ("sample_size", "p10", "p25", "p50", "p75", "p90", "mean", "std", "bucket_definition_json"))
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {SAME_TIMEFRAME_STATS_PROFILE_TABLE} (
                stats_profile_nk, universe, timeframe, regime_family, metric_name, sample_version,
                sample_size, p10, p25, p50, p75, p90, mean, std, bucket_definition_json,
                first_seen_run_id, last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [row[column] for column in (
                "stats_profile_nk", "universe", "timeframe", "regime_family", "metric_name", "sample_version",
                "sample_size", "p10", "p25", "p50", "p75", "p90", "mean", "std", "bucket_definition_json",
                "first_seen_run_id", "last_materialized_run_id",
            )],
        )
        return "inserted"
    existing_fingerprint = tuple(existing[index] for index in range(9))
    first_seen_run_id = _normalize_optional_str(existing[9], default=str(row["first_seen_run_id"]))
    connection.execute(
        f"""
        UPDATE {SAME_TIMEFRAME_STATS_PROFILE_TABLE}
        SET
            sample_size = ?, p10 = ?, p25 = ?, p50 = ?, p75 = ?, p90 = ?, mean = ?, std = ?,
            bucket_definition_json = ?, first_seen_run_id = ?, last_materialized_run_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE stats_profile_nk = ?
        """,
        [
            row["sample_size"], row["p10"], row["p25"], row["p50"], row["p75"], row["p90"], row["mean"], row["std"],
            row["bucket_definition_json"], first_seen_run_id, row["last_materialized_run_id"], row["stats_profile_nk"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _upsert_snapshot_row(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT
            regime_family, new_high_count_percentile, new_low_count_percentile, refresh_density_percentile,
            advancement_density_percentile, exhaustion_risk_bucket, reversal_probability_bucket,
            source_profile_refs_json, first_seen_run_id
        FROM {SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE}
        WHERE stats_snapshot_nk = ?
        """,
        [row["stats_snapshot_nk"]],
    ).fetchone()
    fingerprint = tuple(
        row[key]
        for key in (
            "regime_family",
            "new_high_count_percentile",
            "new_low_count_percentile",
            "refresh_density_percentile",
            "advancement_density_percentile",
            "exhaustion_risk_bucket",
            "reversal_probability_bucket",
            "source_profile_refs_json",
        )
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE} (
                stats_snapshot_nk, instrument, timeframe, signal_date, asof_bar_dt, regime_family,
                sample_version, stats_contract_version, source_context_nk, source_candidate_nk,
                new_high_count_percentile, new_low_count_percentile, refresh_density_percentile,
                advancement_density_percentile, exhaustion_risk_bucket, reversal_probability_bucket,
                source_profile_refs_json, first_seen_run_id, last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [row[column] for column in (
                "stats_snapshot_nk", "instrument", "timeframe", "signal_date", "asof_bar_dt", "regime_family",
                "sample_version", "stats_contract_version", "source_context_nk", "source_candidate_nk",
                "new_high_count_percentile", "new_low_count_percentile", "refresh_density_percentile",
                "advancement_density_percentile", "exhaustion_risk_bucket", "reversal_probability_bucket",
                "source_profile_refs_json", "first_seen_run_id", "last_materialized_run_id",
            )],
        )
        return "inserted"
    existing_fingerprint = tuple(existing[index] for index in range(8))
    first_seen_run_id = _normalize_optional_str(existing[8], default=str(row["first_seen_run_id"]))
    connection.execute(
        f"""
        UPDATE {SAME_TIMEFRAME_STATS_SNAPSHOT_TABLE}
        SET
            regime_family = ?, new_high_count_percentile = ?, new_low_count_percentile = ?,
            refresh_density_percentile = ?, advancement_density_percentile = ?, exhaustion_risk_bucket = ?,
            reversal_probability_bucket = ?, source_profile_refs_json = ?, first_seen_run_id = ?,
            last_materialized_run_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE stats_snapshot_nk = ?
        """,
        [
            row["regime_family"], row["new_high_count_percentile"], row["new_low_count_percentile"],
            row["refresh_density_percentile"], row["advancement_density_percentile"], row["exhaustion_risk_bucket"],
            row["reversal_probability_bucket"], row["source_profile_refs_json"], first_seen_run_id,
            row["last_materialized_run_id"], row["stats_snapshot_nk"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _upsert_checkpoints(
    connection: duckdb.DuckDBPyConnection,
    *,
    input_rows: list[_MechanismInputRow],
    timeframe: str,
    run_id: str,
) -> int:
    latest_rows: dict[str, _MechanismInputRow] = {}
    for row in input_rows:
        current = latest_rows.get(row.instrument)
        if current is None or (row.signal_date, row.asof_date) >= (current.signal_date, current.asof_date):
            latest_rows[row.instrument] = row
    for instrument, row in latest_rows.items():
        existing = connection.execute(
            f"""
            SELECT instrument
            FROM {MALF_MECHANISM_CHECKPOINT_TABLE}
            WHERE instrument = ? AND timeframe = ?
            """,
            [instrument, timeframe],
        ).fetchone()
        if existing is None:
            connection.execute(
                f"""
                INSERT INTO {MALF_MECHANISM_CHECKPOINT_TABLE} (
                    instrument, timeframe, last_signal_date, last_asof_date, last_run_id, source_context_nk
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [instrument, timeframe, row.signal_date, row.asof_date, run_id, row.source_context_nk],
            )
            continue
        connection.execute(
            f"""
            UPDATE {MALF_MECHANISM_CHECKPOINT_TABLE}
            SET
                last_signal_date = ?,
                last_asof_date = ?,
                last_run_id = ?,
                source_context_nk = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE instrument = ? AND timeframe = ?
            """,
            [row.signal_date, row.asof_date, run_id, row.source_context_nk, instrument, timeframe],
        )
    return len(latest_rows)


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
    timeframe: str,
    stats_sample_version: str,
    mechanism_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {MALF_MECHANISM_RUN_TABLE} (
            run_id, runner_name, runner_version, run_status, signal_start_date, signal_end_date,
            bounded_instrument_count, source_context_table, source_structure_input_table, timeframe,
            stats_sample_version, mechanism_contract_version
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?)
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
            timeframe,
            stats_sample_version,
            mechanism_contract_version,
        ],
    )


def _mark_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: MalfMechanismBuildSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_MECHANISM_RUN_TABLE}
        SET
            run_status = 'completed',
            bounded_instrument_count = ?,
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [summary.bounded_instrument_count, json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True), run_id],
    )


def _mark_run_failed(connection: duckdb.DuckDBPyConnection, *, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_MECHANISM_RUN_TABLE}
        SET run_status = 'failed', completed_at = CURRENT_TIMESTAMP
        WHERE run_id = ?
        """,
        [run_id],
    )


def _group_rows_by_instrument(input_rows: list[_MechanismInputRow]) -> dict[str, list[_MechanismInputRow]]:
    grouped: dict[str, list[_MechanismInputRow]] = {}
    for row in input_rows:
        grouped.setdefault(row.instrument, []).append(row)
    for rows in grouped.values():
        rows.sort(key=lambda item: (item.signal_date, item.asof_date))
    return grouped


def _derive_break_direction(row: _MechanismInputRow) -> str:
    if row.new_low_count > row.new_high_count:
        return "DOWN"
    if row.new_high_count > row.new_low_count:
        return "UP"
    if "BULL" in row.malf_context_4:
        return "DOWN"
    if "BEAR" in row.malf_context_4:
        return "UP"
    return "DOWN" if row.failure_type and "down" in row.failure_type.lower() else "UP"


def _derive_confirmation(
    *,
    trigger_row: _MechanismInputRow,
    direction: str,
    future_rows: list[_MechanismInputRow],
) -> tuple[str, _MechanismInputRow | None]:
    for row in future_rows:
        if direction == "DOWN" and (row.new_low_count > 0 or row.is_failed_extreme):
            return "confirmed", row
        if direction == "UP" and (row.new_high_count > 0 or (row.is_failed_extreme and row.new_high_count >= row.new_low_count)):
            return "confirmed", row
    return "pending", None


def _percentile(values: list[float], level: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    index = (len(values) - 1) * level
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return float(values[lower])
    lower_value = float(values[lower])
    upper_value = float(values[upper])
    return lower_value + (upper_value - lower_value) * (index - lower)


def _empirical_percentile(values: list[float], value: float) -> float:
    if not values:
        return 0.0
    ordered_values = sorted(float(item) for item in values)
    less_or_equal = sum(1 for item in ordered_values if item <= float(value))
    return float(less_or_equal) / float(len(ordered_values))


def _percentile_bucket(value: float) -> str:
    if value >= 0.90:
        return "high"
    if value >= 0.75:
        return "elevated"
    return "normal"


def _build_guard_pivot_id(*, instrument: str, timeframe: str, signal_date: date, direction: str) -> str:
    return "|".join([instrument, timeframe, signal_date.isoformat(), direction, "guard"])


def _build_confirmation_pivot_id(*, instrument: str, timeframe: str, signal_date: date, direction: str) -> str:
    return "|".join([instrument, timeframe, signal_date.isoformat(), direction, "confirmation"])


def _build_break_event_nk(*, instrument: str, timeframe: str, guard_pivot_id: str, trigger_bar_dt: date) -> str:
    return "|".join([instrument, timeframe, guard_pivot_id, trigger_bar_dt.isoformat()])


def _build_stats_profile_nk(
    *,
    universe: str,
    timeframe: str,
    regime_family: str,
    metric_name: str,
    sample_version: str,
) -> str:
    return "|".join([universe, timeframe, regime_family, metric_name, sample_version])


def _build_stats_snapshot_nk(
    *,
    instrument: str,
    timeframe: str,
    asof_bar_dt: date,
    sample_version: str,
    mechanism_contract_version: str,
) -> str:
    return "|".join([instrument, timeframe, asof_bar_dt.isoformat(), sample_version, mechanism_contract_version])


def _build_source_context_nk(
    *,
    instrument: str,
    signal_date: date,
    asof_date: date,
    malf_context_4: str,
) -> str:
    return "|".join([instrument, signal_date.isoformat(), asof_date.isoformat(), malf_context_4])


def _build_source_candidate_nk(*, instrument: str, signal_date: date, asof_date: date) -> str:
    return "|".join([instrument, signal_date.isoformat(), asof_date.isoformat(), "mechanism-source"])


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
    normalized = _coerce_date(value)
    if normalized is None:
        raise ValueError(f"Missing required date field: {field_name}")
    return normalized


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
    if value is None:
        return 0
    return int(value)


def _normalize_optional_float(value: object) -> float:
    if value is None:
        return 0.0
    return float(value)


def _coerce_date(value: object | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_run_id(*, prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"


def _write_summary(payload: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
