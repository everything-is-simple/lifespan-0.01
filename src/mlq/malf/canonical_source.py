"""承接 canonical MALF runner 的上游 scope 读取、队列管理与行情装载。"""

from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd

from mlq.malf.bootstrap import MALF_CANONICAL_CHECKPOINT_TABLE, MALF_CANONICAL_WORK_QUEUE_TABLE
from mlq.malf.canonical_shared import _build_queue_nk, _build_scope_nk


def _load_source_scope_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    adjust_method: str,
    asset_type: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int | None,
    to_python_date,
) -> list[dict[str, object]]:
    available_columns = _load_table_columns(connection, table_name)
    code_column = _resolve_existing_column(available_columns, ("code", "instrument"), "code", table_name)
    date_column = _resolve_existing_column(available_columns, ("trade_date", "signal_date"), "trade_date", table_name)
    where_clauses = ["adjust_method = ?"]
    parameters: list[object] = [adjust_method]
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
    sql = f"""
        SELECT {code_column} AS code, MAX({date_column}) AS last_trade_date
        FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
        GROUP BY {code_column}
        ORDER BY {code_column}
    """
    if limit is not None:
        sql = f"{sql}\nLIMIT ?"
        parameters.append(limit)
    rows = connection.execute(sql, parameters).fetchall()
    return [
        {
            "asset_type": asset_type,
            "code": str(code),
            "last_trade_date": to_python_date(last_trade_date),
        }
        for code, last_trade_date in rows
    ]


def _enqueue_dirty_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    scope_rows: list[dict[str, object]],
    timeframes: tuple[str, ...],
    run_id: str,
    to_python_date,
) -> dict[str, int]:
    queue_enqueued_count = 0
    for scope_row in scope_rows:
        for timeframe in timeframes:
            checkpoint_row = connection.execute(
                f"""
                SELECT last_completed_bar_dt
                FROM {MALF_CANONICAL_CHECKPOINT_TABLE}
                WHERE asset_type = ?
                  AND code = ?
                  AND timeframe = ?
                """,
                [str(scope_row["asset_type"]), str(scope_row["code"]), timeframe],
            ).fetchone()
            last_completed_bar_dt = to_python_date(checkpoint_row[0]) if checkpoint_row else None
            source_last_trade_date = to_python_date(scope_row["last_trade_date"])
            if (
                last_completed_bar_dt is not None
                and source_last_trade_date is not None
                and source_last_trade_date <= last_completed_bar_dt
            ):
                continue
            dirty_reason = "bootstrap_missing_checkpoint" if last_completed_bar_dt is None else "source_advanced"
            scope_nk = _build_scope_nk(
                asset_type=str(scope_row["asset_type"]),
                code=str(scope_row["code"]),
                timeframe=timeframe,
            )
            queue_nk = _build_queue_nk(scope_nk=scope_nk, dirty_reason=dirty_reason)
            existing = connection.execute(
                f"SELECT queue_nk FROM {MALF_CANONICAL_WORK_QUEUE_TABLE} WHERE queue_nk = ?",
                [queue_nk],
            ).fetchone()
            if existing is None:
                connection.execute(
                    f"""
                    INSERT INTO {MALF_CANONICAL_WORK_QUEUE_TABLE} (
                        queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason,
                        source_last_trade_date, queue_status, first_seen_run_id,
                        last_materialized_run_id, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, CURRENT_TIMESTAMP)
                    """,
                    [
                        queue_nk,
                        scope_nk,
                        str(scope_row["asset_type"]),
                        str(scope_row["code"]),
                        timeframe,
                        dirty_reason,
                        source_last_trade_date,
                        run_id,
                        run_id,
                    ],
                )
                queue_enqueued_count += 1
            else:
                connection.execute(
                    f"""
                    UPDATE {MALF_CANONICAL_WORK_QUEUE_TABLE}
                    SET source_last_trade_date = ?,
                        queue_status = 'pending',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE queue_nk = ?
                    """,
                    [source_last_trade_date, queue_nk],
                )
    return {"queue_enqueued_count": queue_enqueued_count}


def _claim_ready_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    run_id: str,
    to_python_date,
) -> list[dict[str, object]]:
    rows = connection.execute(
        f"""
        SELECT queue_nk, scope_nk, asset_type, code, timeframe, dirty_reason, source_last_trade_date
        FROM {MALF_CANONICAL_WORK_QUEUE_TABLE}
        WHERE asset_type = ?
          AND queue_status IN ('pending', 'claimed', 'failed')
        ORDER BY code, timeframe, enqueued_at
        """,
        [asset_type],
    ).fetchall()
    claimed_rows: list[dict[str, object]] = []
    for row in rows:
        connection.execute(
            f"""
            UPDATE {MALF_CANONICAL_WORK_QUEUE_TABLE}
            SET queue_status = 'claimed',
                claimed_at = CURRENT_TIMESTAMP,
                last_claimed_run_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE queue_nk = ?
            """,
            [run_id, str(row[0])],
        )
        claimed_rows.append(
            {
                "queue_nk": str(row[0]),
                "scope_nk": str(row[1]),
                "asset_type": str(row[2]),
                "code": str(row[3]),
                "timeframe": str(row[4]),
                "dirty_reason": str(row[5]),
                "source_last_trade_date": to_python_date(row[6]),
            }
        )
    return claimed_rows


def _mark_queue_completed(connection: duckdb.DuckDBPyConnection, *, queue_nk: str, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_CANONICAL_WORK_QUEUE_TABLE}
        SET queue_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _mark_queue_failed(connection: duckdb.DuckDBPyConnection, *, queue_nk: str, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_CANONICAL_WORK_QUEUE_TABLE}
        SET queue_status = 'failed',
            last_claimed_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE queue_nk = ?
        """,
        [run_id, queue_nk],
    )


def _load_source_bars(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    code: str,
    adjust_method: str,
    signal_end_date: date | None,
) -> pd.DataFrame:
    available_columns = _load_table_columns(connection, table_name)
    code_column = _resolve_existing_column(available_columns, ("code", "instrument"), "code", table_name)
    date_column = _resolve_existing_column(available_columns, ("trade_date", "signal_date"), "trade_date", table_name)
    name_column = "name" if "name" in available_columns else None
    volume_column = "volume" if "volume" in available_columns else None
    amount_column = "amount" if "amount" in available_columns else None
    select_fields = [
        f"{code_column} AS code",
        f"{name_column} AS name" if name_column else f"{code_column} AS name",
        f"{date_column} AS trade_date",
        "open AS open",
        "high AS high",
        "low AS low",
        "close AS close",
        f"{volume_column} AS volume" if volume_column else "NULL AS volume",
        f"{amount_column} AS amount" if amount_column else "NULL AS amount",
    ]
    parameters: list[object] = [adjust_method, code]
    where_clauses = ["adjust_method = ?", f"{code_column} = ?"]
    if signal_end_date is not None:
        where_clauses.append(f"{date_column} <= ?")
        parameters.append(signal_end_date)
    rows = connection.execute(
        f"""
        SELECT {', '.join(select_fields)}
        FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY {date_column}
        """,
        parameters,
    ).fetchall()
    if not rows:
        return pd.DataFrame(
            columns=["code", "name", "trade_date", "open", "high", "low", "close", "volume", "amount"]
        )
    frame = pd.DataFrame(
        rows,
        columns=["code", "name", "trade_date", "open", "high", "low", "close", "volume", "amount"],
    )
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    for column in ("open", "high", "low", "close", "volume", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["trade_date", "open", "high", "low", "close"]).reset_index(drop=True)


def _resample_bars_by_timeframe(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    ordered = frame.sort_values("trade_date").reset_index(drop=True).copy()
    if timeframe == "D":
        return ordered
    if ordered.empty:
        return ordered
    indexed = ordered.set_index("trade_date")
    grouped = indexed.groupby(indexed.index.to_period("W-FRI" if timeframe == "W" else "M"))
    rows: list[dict[str, object]] = []
    for _, group in grouped:
        clean_group = group.dropna(subset=["open", "high", "low", "close"])
        if clean_group.empty:
            continue
        rows.append(
            {
                "code": str(clean_group["code"].iloc[-1]),
                "name": str(clean_group["name"].iloc[-1]),
                "trade_date": clean_group.index[-1],
                "open": float(clean_group["open"].iloc[0]),
                "high": float(clean_group["high"].max()),
                "low": float(clean_group["low"].min()),
                "close": float(clean_group["close"].iloc[-1]),
                "volume": float(clean_group["volume"].fillna(0.0).sum()),
                "amount": float(clean_group["amount"].fillna(0.0).sum()),
            }
        )
    if not rows:
        return pd.DataFrame(columns=ordered.columns)
    resampled = pd.DataFrame(rows)
    resampled["trade_date"] = pd.to_datetime(resampled["trade_date"])
    return resampled.sort_values("trade_date").reset_index(drop=True)


def _load_table_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return {str(row[1]) for row in rows}


def _resolve_existing_column(
    available_columns: set[str],
    candidates: tuple[str, ...],
    field_name: str,
    table_name: str,
) -> str:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    raise ValueError(f"Missing column for {field_name} in {table_name}; expected one of {candidates}")
