"""执行 `position` 对官方 `alpha formal signal` 的 bounded 正式消费。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.position.bootstrap import (
    PositionFormalSignalInput,
    PositionMaterializationSummary,
    materialize_position_from_formal_signals,
    position_ledger_path,
)


DEFAULT_ALPHA_FORMAL_SIGNAL_TABLE: Final[str] = "alpha_formal_signal_event"
DEFAULT_MARKET_BASE_PRICE_TABLE: Final[str] = "stock_daily_adjusted"
DEFAULT_MARKET_BASE_ADJUST_METHOD: Final[str] = "none"


@dataclass(frozen=True)
class PositionFormalSignalRunnerSummary:
    """总结一次 bounded runner 对 `alpha -> position` 的正式桥接结果。"""

    policy_id: str
    position_run_id: str | None
    alpha_signal_count: int
    enriched_signal_count: int
    missing_reference_price_count: int
    candidate_count: int
    admitted_count: int
    blocked_count: int
    sizing_count: int
    family_snapshot_count: int
    alpha_ledger_path: str
    market_base_path: str
    position_ledger_path: str
    alpha_formal_signal_table: str
    market_price_table: str
    adjust_method: str

    def as_dict(self) -> dict[str, object]:
        """返回适合写入 summary JSON 的稳定字典。"""

        return asdict(self)


def run_position_formal_signal_materialization(
    *,
    policy_id: str,
    capital_base_value: float,
    settings: WorkspaceRoots | None = None,
    alpha_path: Path | None = None,
    market_base_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    alpha_formal_signal_table: str = DEFAULT_ALPHA_FORMAL_SIGNAL_TABLE,
    market_price_table: str = DEFAULT_MARKET_BASE_PRICE_TABLE,
    adjust_method: str = DEFAULT_MARKET_BASE_ADJUST_METHOD,
    allow_same_day_price_fallback: bool = False,
    summary_path: Path | None = None,
) -> PositionFormalSignalRunnerSummary:
    """从官方 `alpha formal signal` 读取 bounded 样本并落入正式 `position` 账本。"""

    workspace = settings or default_settings()
    normalized_instruments = tuple(sorted({instrument for instrument in instruments or () if instrument}))
    normalized_limit = max(int(limit), 1)
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)

    resolved_alpha_path = Path(alpha_path or workspace.databases.alpha)
    resolved_market_base_path = Path(market_base_path or workspace.databases.market_base)
    resolved_position_path = position_ledger_path(workspace)

    alpha_rows = _load_alpha_formal_signal_rows(
        alpha_path=resolved_alpha_path,
        alpha_formal_signal_table=alpha_formal_signal_table,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        limit=normalized_limit,
    )
    enriched_signals, missing_reference_price_count = _enrich_reference_prices(
        alpha_rows=alpha_rows,
        market_base_path=resolved_market_base_path,
        market_price_table=market_price_table,
        adjust_method=adjust_method,
        capital_base_value=capital_base_value,
        allow_same_day_price_fallback=allow_same_day_price_fallback,
    )

    materialization_summary = _maybe_materialize(
        enriched_signals,
        policy_id=policy_id,
        settings=workspace,
        run_id=run_id,
    )
    summary = PositionFormalSignalRunnerSummary(
        policy_id=policy_id,
        position_run_id=materialization_summary.run_id if materialization_summary else None,
        alpha_signal_count=len(alpha_rows),
        enriched_signal_count=len(enriched_signals),
        missing_reference_price_count=missing_reference_price_count,
        candidate_count=materialization_summary.candidate_count if materialization_summary else 0,
        admitted_count=materialization_summary.admitted_count if materialization_summary else 0,
        blocked_count=materialization_summary.blocked_count if materialization_summary else 0,
        sizing_count=materialization_summary.sizing_count if materialization_summary else 0,
        family_snapshot_count=materialization_summary.family_snapshot_count if materialization_summary else 0,
        alpha_ledger_path=str(resolved_alpha_path),
        market_base_path=str(resolved_market_base_path),
        position_ledger_path=str(resolved_position_path),
        alpha_formal_signal_table=alpha_formal_signal_table,
        market_price_table=market_price_table,
        adjust_method=adjust_method,
    )
    _write_summary(summary, summary_path)
    return summary


def _coerce_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _load_alpha_formal_signal_rows(
    *,
    alpha_path: Path,
    alpha_formal_signal_table: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[dict[str, object]]:
    _ensure_database_exists(alpha_path, label="alpha")
    connection = duckdb.connect(str(alpha_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, alpha_formal_signal_table)
        signal_date_column = _resolve_existing_column(
            available_columns,
            ("signal_date",),
            field_name="signal_date",
            table_name=alpha_formal_signal_table,
        )
        instrument_column = _resolve_existing_column(
            available_columns,
            ("instrument", "code"),
            field_name="instrument",
            table_name=alpha_formal_signal_table,
        )
        select_sql = _build_alpha_select_sql(
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
            }
            for row in rows
        ]
    finally:
        connection.close()


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
        raise ValueError(f"Missing table in alpha ledger: {table_name}")
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


def _build_alpha_select_sql(*, table_name: str, available_columns: set[str]) -> str:
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
        column_name = _resolve_existing_column(
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
    }
    for alias, candidates in optional_family_columns.items():
        column_name = next((candidate for candidate in candidates if candidate in available_columns), None)
        if column_name is None:
            select_fields.append(f"NULL AS {alias}")
        else:
            select_fields.append(f"{column_name} AS {alias}")
    return f"SELECT {', '.join(select_fields)} FROM {table_name}"


def _normalize_formal_signal_status(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"admitted", "blocked", "deferred"}:
        return normalized
    if normalized in {"admit", "accepted"}:
        return "admitted"
    if normalized in {"reject", "rejected"}:
        return "blocked"
    return "blocked"


def _enrich_reference_prices(
    *,
    alpha_rows: list[dict[str, object]],
    market_base_path: Path,
    market_price_table: str,
    adjust_method: str,
    capital_base_value: float,
    allow_same_day_price_fallback: bool,
) -> tuple[list[PositionFormalSignalInput], int]:
    _ensure_database_exists(market_base_path, label="market_base")
    connection = duckdb.connect(str(market_base_path), read_only=True)
    try:
        enriched_signals: list[PositionFormalSignalInput] = []
        missing_reference_price_count = 0
        for row in alpha_rows:
            signal_date = _coerce_date(row["signal_date"])
            instrument = str(row["instrument"])
            reference_trade_date, reference_price = _load_reference_price(
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
                    asof_date=_coerce_date(row["asof_date"]).isoformat(),
                    trigger_family=str(row["trigger_family"]),
                    trigger_type=str(row["trigger_type"]),
                    pattern_code=str(row["pattern_code"]),
                    formal_signal_status=_normalize_formal_signal_status(row["formal_signal_status"]),
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
                )
            )
        return enriched_signals, missing_reference_price_count
    finally:
        connection.close()


def _load_reference_price(
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


def _maybe_materialize(
    formal_signals: list[PositionFormalSignalInput],
    *,
    policy_id: str,
    settings: WorkspaceRoots,
    run_id: str | None,
) -> PositionMaterializationSummary | None:
    if not formal_signals:
        return None
    return materialize_position_from_formal_signals(
        formal_signals,
        policy_id=policy_id,
        settings=settings,
        run_id=run_id,
    )


def _write_summary(
    summary: PositionFormalSignalRunnerSummary,
    summary_path: Path | None,
) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.as_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
