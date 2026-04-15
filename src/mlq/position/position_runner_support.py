"""承载 `position` data-grade runner 的 source/queue/audit helper。"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

import duckdb

from mlq.position.position_bootstrap_schema import POSITION_CHECKPOINT_TABLE
from mlq.position.position_contract_logic import build_candidate_nk
from mlq.position.position_materialization import fetch_policy_contract
from mlq.position.position_runner_shared import (
    PositionCandidateMaterializationInput,
    coerce_date,
)
from mlq.position.position_shared import PositionFormalSignalInput


def ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def load_alpha_formal_signal_rows(
    *,
    alpha_path: Path,
    alpha_formal_signal_table: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[dict[str, object]]:
    ensure_database_exists(alpha_path, label="alpha")
    connection = duckdb.connect(str(alpha_path), read_only=True)
    try:
        available_columns = load_table_columns(connection, alpha_formal_signal_table)
        signal_date_column = resolve_existing_column(
            available_columns,
            ("signal_date",),
            field_name="signal_date",
            table_name=alpha_formal_signal_table,
        )
        instrument_column = resolve_existing_column(
            available_columns,
            ("instrument", "code"),
            field_name="instrument",
            table_name=alpha_formal_signal_table,
        )
        select_sql = build_alpha_select_sql(
            table_name=alpha_formal_signal_table,
            available_columns=available_columns,
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
            {select_sql}
            {where_sql}
            ORDER BY signal_date, instrument, signal_nk
            LIMIT ?
            """,
            [*parameters, limit],
        ).fetchall()
        return [
            {
                "signal_nk": row[0],
                "instrument": row[1],
                "signal_date": row[2],
                "asof_date": row[3],
                "trigger_family": row[4],
                "trigger_type": row[5],
                "pattern_code": row[6],
                "formal_signal_status": row[7],
                "trigger_admissible": row[8],
                "malf_context_4": row[9],
                "lifecycle_rank_high": row[10],
                "lifecycle_rank_total": row[11],
                "source_trigger_event_nk": row[12],
                "signal_contract_version": row[13],
                "source_signal_run_id": row[14],
                "source_family_event_nk": row[15],
                "source_family_contract_version": row[16],
                "family_code": row[17],
                "family_role": row[18],
                "family_bias": row[19],
                "malf_alignment": row[20],
                "malf_phase_bucket": row[21],
                "family_source_context_fingerprint": row[22],
                "wave_life_percentile": row[23],
                "remaining_life_bars_p50": row[24],
                "remaining_life_bars_p75": row[25],
                "termination_risk_bucket": row[26],
                "stage_percentile_decision_code": row[27],
                "stage_percentile_action_owner": row[28],
                "stage_percentile_note": row[29],
                "stage_percentile_contract_version": row[30],
            }
            for row in rows
        ]
    finally:
        connection.close()


def load_table_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
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
        raise ValueError(f"Missing table in alpha ledger: {table_name}")
    return {str(row[0]) for row in rows}


def resolve_existing_column(
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


def build_alpha_select_sql(*, table_name: str, available_columns: set[str]) -> str:
    mappings: dict[str, tuple[str, ...]] = {
        "signal_nk": ("signal_nk", "signal_id"),
        "instrument": ("instrument", "code"),
        "signal_date": ("signal_date",),
        "asof_date": ("asof_date",),
        "trigger_type": ("trigger_type",),
        "pattern_code": ("pattern_code", "pattern"),
        "formal_signal_status": ("formal_signal_status", "admission_status"),
        "trigger_admissible": ("trigger_admissible", "filter_trigger_admissible"),
        "malf_context_4": ("malf_context_4",),
        "lifecycle_rank_high": ("lifecycle_rank_high",),
        "lifecycle_rank_total": ("lifecycle_rank_total",),
        "source_trigger_event_nk": ("source_trigger_event_nk", "source_pas_signal_id"),
        "signal_contract_version": ("signal_contract_version", "source_pas_contract_version"),
    }
    select_fields: list[str] = []
    for alias, candidates in mappings.items():
        column_name = resolve_existing_column(
            available_columns,
            candidates,
            field_name=alias,
            table_name=table_name,
        )
        select_fields.append(f"{column_name} AS {alias}")
    if "trigger_family" in available_columns:
        select_fields.insert(4, "trigger_family AS trigger_family")
    else:
        select_fields.insert(4, "'PAS' AS trigger_family")
    if "source_signal_run_id" in available_columns:
        select_fields.append("source_signal_run_id AS source_signal_run_id")
    elif "last_materialized_run_id" in available_columns:
        select_fields.append("last_materialized_run_id AS source_signal_run_id")
    else:
        select_fields.append("NULL AS source_signal_run_id")
    optional_family_columns = {
        "source_family_event_nk": ("source_family_event_nk",),
        "source_family_contract_version": ("source_family_contract_version",),
        "family_code": ("family_code",),
        "family_role": ("family_role",),
        "family_bias": ("family_bias",),
        "malf_alignment": ("malf_alignment",),
        "malf_phase_bucket": ("malf_phase_bucket",),
        "family_source_context_fingerprint": ("family_source_context_fingerprint",),
        "wave_life_percentile": ("wave_life_percentile",),
        "remaining_life_bars_p50": ("remaining_life_bars_p50",),
        "remaining_life_bars_p75": ("remaining_life_bars_p75",),
        "termination_risk_bucket": ("termination_risk_bucket",),
        "stage_percentile_decision_code": ("stage_percentile_decision_code",),
        "stage_percentile_action_owner": ("stage_percentile_action_owner",),
        "stage_percentile_note": ("stage_percentile_note",),
        "stage_percentile_contract_version": ("stage_percentile_contract_version",),
    }
    for alias, candidates in optional_family_columns.items():
        column_name = next((candidate for candidate in candidates if candidate in available_columns), None)
        if column_name is None:
            select_fields.append(f"NULL AS {alias}")
        else:
            select_fields.append(f"{column_name} AS {alias}")
    return f"SELECT {', '.join(select_fields)} FROM {table_name}"


def normalize_formal_signal_status(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"admitted", "blocked", "deferred"}:
        return normalized
    if normalized in {"admit", "accepted"}:
        return "admitted"
    if normalized in {"reject", "rejected"}:
        return "blocked"
    return "blocked"


def enrich_reference_prices(
    *,
    alpha_rows: list[dict[str, object]],
    market_base_path: Path,
    market_price_table: str,
    adjust_method: str,
    capital_base_value: float,
    allow_same_day_price_fallback: bool,
) -> tuple[list[PositionFormalSignalInput], int]:
    ensure_database_exists(market_base_path, label="market_base")
    connection = duckdb.connect(str(market_base_path), read_only=True)
    try:
        enriched_signals: list[PositionFormalSignalInput] = []
        missing_reference_price_count = 0
        for row in alpha_rows:
            signal_date = coerce_date(row["signal_date"])
            instrument = str(row["instrument"])
            reference_trade_date, reference_price = load_reference_price(
                connection,
                market_price_table=market_price_table,
                instrument=instrument,
                signal_date=signal_date,
                adjust_method=adjust_method,
                allow_same_day_price_fallback=allow_same_day_price_fallback,
            )
            if reference_trade_date is None or reference_price is None:
                missing_reference_price_count += 1
                continue
            enriched_signals.append(
                PositionFormalSignalInput(
                    signal_nk=str(row["signal_nk"]),
                    instrument=instrument,
                    signal_date=signal_date.isoformat(),
                    asof_date=coerce_date(row["asof_date"]).isoformat(),
                    trigger_family=str(row["trigger_family"]),
                    trigger_type=str(row["trigger_type"]),
                    pattern_code=str(row["pattern_code"]),
                    formal_signal_status=normalize_formal_signal_status(row["formal_signal_status"]),
                    trigger_admissible=bool(row["trigger_admissible"]),
                    malf_context_4=str(row["malf_context_4"]),
                    lifecycle_rank_high=int(row["lifecycle_rank_high"]),
                    lifecycle_rank_total=int(row["lifecycle_rank_total"]),
                    source_trigger_event_nk=str(row["source_trigger_event_nk"]),
                    signal_contract_version=str(row["signal_contract_version"]),
                    reference_trade_date=reference_trade_date.isoformat(),
                    reference_price=reference_price,
                    capital_base_value=capital_base_value,
                    source_signal_run_id=None
                    if row["source_signal_run_id"] is None
                    else str(row["source_signal_run_id"]),
                    source_family_event_nk=None
                    if row["source_family_event_nk"] is None
                    else str(row["source_family_event_nk"]),
                    source_family_contract_version=None
                    if row["source_family_contract_version"] is None
                    else str(row["source_family_contract_version"]),
                    family_code=None if row["family_code"] is None else str(row["family_code"]),
                    family_role=None if row["family_role"] is None else str(row["family_role"]),
                    family_bias=None if row["family_bias"] is None else str(row["family_bias"]),
                    malf_alignment=None if row["malf_alignment"] is None else str(row["malf_alignment"]),
                    malf_phase_bucket=None
                    if row["malf_phase_bucket"] is None
                    else str(row["malf_phase_bucket"]),
                    family_source_context_fingerprint=None
                    if row["family_source_context_fingerprint"] is None
                    else str(row["family_source_context_fingerprint"]),
                    wave_life_percentile=None
                    if row["wave_life_percentile"] is None
                    else float(row["wave_life_percentile"]),
                    remaining_life_bars_p50=None
                    if row["remaining_life_bars_p50"] is None
                    else float(row["remaining_life_bars_p50"]),
                    remaining_life_bars_p75=None
                    if row["remaining_life_bars_p75"] is None
                    else float(row["remaining_life_bars_p75"]),
                    termination_risk_bucket=None
                    if row["termination_risk_bucket"] is None
                    else str(row["termination_risk_bucket"]),
                    stage_percentile_decision_code=None
                    if row["stage_percentile_decision_code"] is None
                    else str(row["stage_percentile_decision_code"]),
                    stage_percentile_action_owner=None
                    if row["stage_percentile_action_owner"] is None
                    else str(row["stage_percentile_action_owner"]),
                    stage_percentile_note=None
                    if row["stage_percentile_note"] is None
                    else str(row["stage_percentile_note"]),
                    stage_percentile_contract_version=None
                    if row["stage_percentile_contract_version"] is None
                    else str(row["stage_percentile_contract_version"]),
                )
            )
        return enriched_signals, missing_reference_price_count
    finally:
        connection.close()


def load_reference_price(
    connection: duckdb.DuckDBPyConnection,
    *,
    market_price_table: str,
    instrument: str,
    signal_date: date,
    adjust_method: str,
    allow_same_day_price_fallback: bool,
) -> tuple[date | None, float | None]:
    row = connection.execute(
        f"""
        SELECT trade_date, close
        FROM {market_price_table}
        WHERE code = ?
          AND adjust_method = ?
          AND trade_date > ?
          AND close IS NOT NULL
        ORDER BY trade_date
        LIMIT 1
        """,
        [instrument, adjust_method, signal_date],
    ).fetchone()
    if row is None and allow_same_day_price_fallback:
        row = connection.execute(
            f"""
            SELECT trade_date, close
            FROM {market_price_table}
            WHERE code = ?
              AND adjust_method = ?
              AND trade_date >= ?
              AND close IS NOT NULL
            ORDER BY trade_date
            LIMIT 1
            """,
            [instrument, adjust_method, signal_date],
        ).fetchone()
    if row is None:
        return None, None
    trade_date_value = row[0]
    if isinstance(trade_date_value, datetime):
        normalized_trade_date = trade_date_value.date()
    else:
        normalized_trade_date = trade_date_value
    return normalized_trade_date, float(row[1])


def build_candidate_inputs(
    connection: duckdb.DuckDBPyConnection,
    enriched_signals: list[PositionFormalSignalInput],
    *,
    policy_id: str,
) -> list[PositionCandidateMaterializationInput]:
    policy_contract = fetch_policy_contract(connection, policy_id)
    return [
        PositionCandidateMaterializationInput(
            signal=signal,
            candidate_nk=build_candidate_nk(signal, policy_id),
            checkpoint_nk=build_candidate_nk(signal, policy_id),
            source_signal_fingerprint=build_source_signal_fingerprint(
                signal=signal,
                policy_id=policy_id,
                policy_contract=policy_contract,
            ),
        )
        for signal in enriched_signals
    ]


def build_source_signal_fingerprint(
    *,
    signal: PositionFormalSignalInput,
    policy_id: str,
    policy_contract,
) -> str:
    payload = asdict(signal)
    payload.update(
        {
            "policy_id": policy_id,
            "policy_family": policy_contract.policy_family,
            "policy_version": policy_contract.policy_version,
            "position_contract_version": policy_contract.position_contract_version,
        }
    )
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def load_position_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    checkpoint_nk: str,
) -> dict[str, object] | None:
    row = connection.execute(
        f"""
        SELECT
            candidate_nk,
            instrument,
            checkpoint_scope,
            last_signal_nk,
            last_reference_trade_date,
            last_source_signal_fingerprint,
            last_completed_at,
            last_run_id
        FROM {POSITION_CHECKPOINT_TABLE}
        WHERE checkpoint_nk = ?
        """,
        [checkpoint_nk],
    ).fetchone()
    if row is None:
        return None
    return {
        "candidate_nk": str(row[0]),
        "instrument": str(row[1]),
        "checkpoint_scope": str(row[2]),
        "last_signal_nk": str(row[3]),
        "last_reference_trade_date": coerce_date(row[4]),
        "last_source_signal_fingerprint": str(row[5]),
        "last_completed_at": row[6],
        "last_run_id": None if row[7] is None else str(row[7]),
    }


def load_candidate_scope_stats(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
) -> dict[str, object]:
    candidate_row = connection.execute(
        """
        SELECT candidate_status
        FROM position_candidate_audit
        WHERE candidate_nk = ?
        """,
        [candidate_nk],
    ).fetchone()
    sizing_row = connection.execute(
        """
        SELECT position_action_decision
        FROM position_sizing_snapshot
        WHERE candidate_nk = ?
        """,
        [candidate_nk],
    ).fetchone()
    risk_budget_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM position_risk_budget_snapshot
            WHERE candidate_nk = ?
            """,
            [candidate_nk],
        ).fetchone()[0]
    )
    capacity_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM position_capacity_snapshot
            WHERE candidate_nk = ?
            """,
            [candidate_nk],
        ).fetchone()[0]
    )
    sizing_count = 0 if sizing_row is None else 1
    entry_leg_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM position_entry_leg_plan
            WHERE candidate_nk = ?
            """,
            [candidate_nk],
        ).fetchone()[0]
    )
    exit_plan_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM position_exit_plan
            WHERE candidate_nk = ?
            """,
            [candidate_nk],
        ).fetchone()[0]
    )
    exit_leg_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM position_exit_leg
            WHERE exit_plan_nk IN (
                SELECT exit_plan_nk
                FROM position_exit_plan
                WHERE candidate_nk = ?
            )
            """,
            [candidate_nk],
        ).fetchone()[0]
    )
    family_snapshot_count = int(
        connection.execute(
            """
            SELECT
                (
                    SELECT COUNT(*)
                    FROM position_funding_fixed_notional_snapshot
                    WHERE candidate_nk = ?
                ) + (
                    SELECT COUNT(*)
                    FROM position_funding_single_lot_snapshot
                    WHERE candidate_nk = ?
                )
            """,
            [candidate_nk, candidate_nk],
        ).fetchone()[0]
    )
    return {
        "candidate_exists": candidate_row is not None,
        "candidate_status": "blocked" if candidate_row is None else str(candidate_row[0]),
        "position_action_decision": None if sizing_row is None else str(sizing_row[0]),
        "risk_budget_count": risk_budget_count,
        "capacity_count": capacity_count,
        "sizing_count": sizing_count,
        "family_snapshot_count": family_snapshot_count,
        "entry_leg_count": entry_leg_count,
        "exit_plan_count": exit_plan_count,
        "exit_leg_count": exit_leg_count,
        "core_complete": (
            candidate_row is not None
            and sizing_row is not None
            and risk_budget_count > 0
            and capacity_count > 0
            and entry_leg_count > 0
            and family_snapshot_count > 0
        ),
    }


def resolve_materialization_action(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    checkpoint_nk: str,
    source_signal_fingerprint: str,
) -> str:
    checkpoint_row = load_position_checkpoint(connection, checkpoint_nk=checkpoint_nk)
    stats = load_candidate_scope_stats(connection, candidate_nk=candidate_nk)
    if checkpoint_row is None:
        return "rematerialized" if stats["candidate_exists"] else "inserted"
    if (
        str(checkpoint_row["last_source_signal_fingerprint"]) == source_signal_fingerprint
        and bool(stats["core_complete"])
    ):
        return "reused"
    return "rematerialized" if stats["candidate_exists"] else "inserted"
