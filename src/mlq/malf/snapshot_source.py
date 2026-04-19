"""`malf snapshot` bridge v1 runner 的 source 读取与快照派生。"""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
import pandas as pd


def _load_target_instruments(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    adjust_method: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
) -> list[str]:
    available_columns = _load_table_columns(connection, table_name)
    code_column = _resolve_existing_column(available_columns, ("code", "instrument"), field_name="code", table_name=table_name)
    date_column = _resolve_existing_column(available_columns, ("trade_date", "signal_date"), field_name="trade_date", table_name=table_name)
    parameters: list[object] = [adjust_method]
    where_clauses = ["adjust_method = ?"]
    if signal_start_date is not None:
        where_clauses.append(f"{date_column} >= ?")
        parameters.append(signal_start_date)
    if signal_end_date is not None:
        where_clauses.append(f"{date_column} <= ?")
        parameters.append(signal_end_date)
    if instruments:
        placeholders = ", ".join("?" for _ in instruments)
        where_clauses.append(f"{code_column} IN ({placeholders})")
        parameters.extend(instruments)
    rows = connection.execute(
        f"""
        SELECT DISTINCT {code_column}
        FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY {code_column}
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [str(row[0]) for row in rows]


def _load_price_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    adjust_method: str,
    instruments: list[str],
    signal_start_date: date | None,
    signal_end_date: date | None,
) -> pd.DataFrame:
    available_columns = _load_table_columns(connection, table_name)
    code_column = _resolve_existing_column(available_columns, ("code", "instrument"), field_name="code", table_name=table_name)
    name_column = "name" if "name" in available_columns else None
    date_column = _resolve_existing_column(available_columns, ("trade_date", "signal_date"), field_name="trade_date", table_name=table_name)
    select_fields = [
        f"{code_column} AS code",
        f"{name_column} AS name" if name_column is not None else f"{code_column} AS name",
        f"{date_column} AS trade_date",
        "open AS open",
        "high AS high",
        "low AS low",
        "close AS close",
    ]
    parameters: list[object] = [adjust_method, *instruments]
    where_clauses = [f"adjust_method = ?", f"{code_column} IN ({', '.join('?' for _ in instruments)})"]
    if signal_end_date is not None:
        where_clauses.append(f"{date_column} <= ?")
        parameters.append(signal_end_date)
    if signal_start_date is not None:
        # bridge v1 需要回看一段历史窗口，才能稳定派生均线和新高/新低统计。
        where_clauses.append(f"{date_column} >= ?")
        parameters.append(signal_start_date - timedelta(days=400))
    rows = connection.execute(
        f"""
        SELECT {', '.join(select_fields)}
        FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY code, trade_date
        """,
        parameters,
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["code", "name", "trade_date", "open", "high", "low", "close"])
    frame = pd.DataFrame(rows, columns=["code", "name", "trade_date", "open", "high", "low", "close"])
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _derive_malf_snapshots(
    price_frame: pd.DataFrame,
    *,
    signal_start_date: date | None,
    signal_end_date: date | None,
    adjust_method: str,
    malf_contract_version: str,
    run_id: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    context_rows: list[dict[str, object]] = []
    structure_rows: list[dict[str, object]] = []
    for code, group in price_frame.groupby("code", sort=True):
        ordered = group.sort_values("trade_date").reset_index(drop=True).copy()
        ordered["prev_close"] = ordered["close"].shift(1)
        ordered["ma20"] = ordered["close"].rolling(20, min_periods=5).mean()
        ordered["ma60"] = ordered["close"].rolling(60, min_periods=10).mean()
        ordered["ret20"] = ordered["close"] / ordered["close"].shift(20) - 1.0
        ordered["up_day"] = (ordered["close"] > ordered["prev_close"]).astype(float)
        ordered["advancement_density"] = ordered["up_day"].rolling(10, min_periods=3).mean().fillna(0.0)
        ordered["new_high_count"] = 0
        ordered["new_low_count"] = 0
        for window in (20, 60, 120):
            prior_high = ordered["high"].shift(1).rolling(window, min_periods=window).max()
            prior_low = ordered["low"].shift(1).rolling(window, min_periods=window).min()
            ordered["new_high_count"] += (ordered["close"] > prior_high).fillna(False).astype(int)
            ordered["new_low_count"] += (ordered["close"] < prior_low).fillna(False).astype(int)
        ordered["refresh_density"] = ordered["new_high_count"] / 3.0
        ordered["is_failed_extreme"] = (
            ((ordered["new_high_count"] > 0) & (ordered["close"] < ordered["open"]) & (ordered["close"] < ordered["prev_close"]))
            | (ordered["new_low_count"] > 0)
        )
        ordered["failure_type"] = None
        ordered.loc[ordered["new_low_count"] > 0, "failure_type"] = "failed_breakdown"
        ordered.loc[
            (ordered["new_high_count"] > 0)
            & (ordered["close"] < ordered["open"])
            & (ordered["close"] < ordered["prev_close"]),
            "failure_type",
        ] = "failed_extreme"
        ordered["malf_context_4"] = ordered.apply(_derive_malf_context, axis=1)
        ordered["lifecycle_rank_high"] = ordered["new_high_count"].clip(lower=0, upper=4).astype(int)
        ordered["lifecycle_rank_total"] = 4
        if signal_start_date is not None:
            ordered = ordered[ordered["trade_date"] >= pd.Timestamp(signal_start_date)]
        if signal_end_date is not None:
            ordered = ordered[ordered["trade_date"] <= pd.Timestamp(signal_end_date)]
        for row in ordered.itertuples(index=False):
            trade_date = row.trade_date.date()
            context_nk = _build_context_nk(
                code=str(code),
                signal_date=trade_date,
                asof_date=trade_date,
                malf_contract_version=malf_contract_version,
            )
            candidate_nk = _build_candidate_nk(
                code=str(code),
                signal_date=trade_date,
                asof_date=trade_date,
                malf_contract_version=malf_contract_version,
            )
            name = str(row.name)
            context_rows.append(
                {
                    "context_nk": context_nk,
                    "entity_code": str(code),
                    "entity_name": name,
                    "signal_date": trade_date,
                    "asof_date": trade_date,
                    "source_context_nk": context_nk,
                    "malf_context_4": str(row.malf_context_4),
                    "lifecycle_rank_high": int(row.lifecycle_rank_high),
                    "lifecycle_rank_total": int(row.lifecycle_rank_total),
                    "calc_date": trade_date,
                    "adjust_method": adjust_method,
                    "first_seen_run_id": run_id,
                    "last_materialized_run_id": run_id,
                }
            )
            structure_rows.append(
                {
                    "candidate_nk": candidate_nk,
                    "instrument": str(code),
                    "instrument_name": name,
                    "signal_date": trade_date,
                    "asof_date": trade_date,
                    "new_high_count": int(row.new_high_count),
                    "new_low_count": int(row.new_low_count),
                    "refresh_density": float(row.refresh_density),
                    "advancement_density": float(row.advancement_density),
                    "is_failed_extreme": bool(row.is_failed_extreme),
                    "failure_type": None if pd.isna(row.failure_type) else str(row.failure_type),
                    "adjust_method": adjust_method,
                    "first_seen_run_id": run_id,
                    "last_materialized_run_id": run_id,
                }
            )
    return context_rows, structure_rows


def _derive_malf_context(row) -> str:
    ma20 = float(row.ma20) if pd.notna(row.ma20) else float(row.close)
    ma60 = float(row.ma60) if pd.notna(row.ma60) else float(row.close)
    ret20 = float(row.ret20) if pd.notna(row.ret20) else 0.0
    if ma20 >= ma60 and ret20 >= 0.0:
        return "BULL_MAINSTREAM"
    if ma20 < ma60 and ret20 <= 0.0:
        return "BEAR_MAINSTREAM"
    if ma20 <= ma60 and ret20 > 0.0:
        return "RECOVERY_MAINSTREAM"
    return "RANGE_BALANCED"


def _build_context_nk(*, code: str, signal_date: date, asof_date: date, malf_contract_version: str) -> str:
    return "|".join([code, signal_date.isoformat(), asof_date.isoformat(), malf_contract_version])


def _build_candidate_nk(*, code: str, signal_date: date, asof_date: date, malf_contract_version: str) -> str:
    return "|".join([code, signal_date.isoformat(), asof_date.isoformat(), malf_contract_version])


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
