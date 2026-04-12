"""承接 `trade runtime` runner 的上游读取与交易日解析。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.trade.runtime_shared import (
    DEFAULT_MARKET_PRICE_ADJUST_METHOD,
    _PortfolioPlanBridgeRow,
    _normalize_date_value,
    _normalize_optional_str,
)


def _ensure_database_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} database: {path}")


def _load_portfolio_plan_rows(
    *,
    portfolio_plan_path: Path,
    source_portfolio_plan_table: str,
    portfolio_id: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[_PortfolioPlanBridgeRow]:
    connection = duckdb.connect(str(portfolio_plan_path), read_only=True)
    try:
        _ensure_required_columns(
            connection,
            source_portfolio_plan_table,
            required_columns=(
                "plan_snapshot_nk",
                "candidate_nk",
                "portfolio_id",
                "instrument",
                "reference_trade_date",
                "requested_weight",
                "admitted_weight",
                "trimmed_weight",
                "plan_status",
            ),
        )
        parameters: list[object] = [portfolio_id]
        where_clauses: list[str] = ["portfolio_id = ?"]
        if signal_start_date is not None:
            where_clauses.append("reference_trade_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("reference_trade_date <= ?")
            parameters.append(signal_end_date)
        if instruments:
            placeholders = ", ".join("?" for _ in instruments)
            where_clauses.append(f"instrument IN ({placeholders})")
            parameters.extend(instruments)
        rows = connection.execute(
            f"""
            SELECT
                plan_snapshot_nk,
                candidate_nk,
                portfolio_id,
                instrument,
                reference_trade_date,
                requested_weight,
                admitted_weight,
                trimmed_weight,
                plan_status
            FROM {source_portfolio_plan_table}
            WHERE {" AND ".join(where_clauses)}
            ORDER BY reference_trade_date, instrument, candidate_nk
            LIMIT ?
            """,
            [*parameters, limit],
        ).fetchall()
        return [
            _PortfolioPlanBridgeRow(
                plan_snapshot_nk=str(row[0]),
                candidate_nk=str(row[1]),
                portfolio_id=str(row[2]),
                instrument=str(row[3]),
                reference_trade_date=_normalize_date_value(row[4], field_name="reference_trade_date"),
                requested_weight=float(row[5] or 0.0),
                admitted_weight=float(row[6] or 0.0),
                trimmed_weight=float(row[7] or 0.0),
                plan_status=_normalize_optional_str(row[8]).lower(),
            )
            for row in rows
        ]
    finally:
        connection.close()


def _ensure_required_columns(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    *,
    required_columns: tuple[str, ...],
) -> None:
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
    available_columns = {str(row[0]) for row in rows}
    missing_columns = sorted(set(required_columns) - available_columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns in `{table_name}`: {', '.join(missing_columns)}"
        )


def _load_next_trade_date(
    *,
    market_base_path: Path,
    market_price_table: str,
    instrument: str,
    reference_trade_date: date,
) -> date:
    connection = duckdb.connect(str(market_base_path), read_only=True)
    try:
        available_columns = _load_table_columns(connection, market_price_table)
        instrument_column = _resolve_existing_column(
            available_columns,
            ("code", "instrument"),
            field_name="instrument",
            table_name=market_price_table,
        )
        parameters: list[object] = [instrument, reference_trade_date]
        adjust_filter_sql = ""
        if "adjust_method" in available_columns:
            adjust_filter_sql = "AND adjust_method = ?"
            parameters.append(DEFAULT_MARKET_PRICE_ADJUST_METHOD)
        row = connection.execute(
            f"""
            SELECT trade_date
            FROM {market_price_table}
            WHERE {instrument_column} = ?
              AND trade_date > ?
              AND open IS NOT NULL
              {adjust_filter_sql}
            ORDER BY trade_date
            LIMIT 1
            """,
            parameters,
        ).fetchone()
        if row is None:
            raise ValueError(
                f"Missing next trading day in `{market_price_table}` for {instrument} after {reference_trade_date.isoformat()}."
            )
        return _normalize_date_value(row[0], field_name="trade_date")
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
