"""执行 `market_base -> malf` 的最小正式语义快照桥接。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

import duckdb
import pandas as pd

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import market_base_ledger_path
from mlq.malf.bootstrap import (
    MALF_RUN_CONTEXT_SNAPSHOT_TABLE,
    MALF_RUN_STRUCTURE_SNAPSHOT_TABLE,
    MALF_RUN_TABLE,
    PAS_CONTEXT_SNAPSHOT_TABLE,
    STRUCTURE_CANDIDATE_SNAPSHOT_TABLE,
    bootstrap_malf_ledger,
    malf_ledger_path,
)


DEFAULT_MARKET_PRICE_TABLE: Final[str] = "stock_daily_adjusted"
DEFAULT_MALF_CONTRACT_VERSION: Final[str] = "malf-snapshot-v1"
DEFAULT_MALF_ADJUST_METHOD: Final[str] = "backward"


@dataclass(frozen=True)
class MalfSnapshotBuildSummary:
    run_id: str
    runner_name: str
    runner_version: str
    malf_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    bounded_instrument_count: int
    source_price_row_count: int
    context_snapshot_count: int
    structure_candidate_count: int
    context_inserted_count: int
    context_reused_count: int
    context_rematerialized_count: int
    structure_inserted_count: int
    structure_reused_count: int
    structure_rematerialized_count: int
    market_base_path: str
    malf_ledger_path: str
    source_price_table: str
    adjust_method: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def run_malf_snapshot_build(
    *,
    settings: WorkspaceRoots | None = None,
    market_base_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    adjust_method: str = DEFAULT_MALF_ADJUST_METHOD,
    source_price_table: str = DEFAULT_MARKET_PRICE_TABLE,
    malf_contract_version: str = DEFAULT_MALF_CONTRACT_VERSION,
    run_id: str | None = None,
    runner_name: str = "malf_snapshot_builder",
    runner_version: str = "v1",
    summary_path: Path | None = None,
) -> MalfSnapshotBuildSummary:
    """从官方 `market_base` 物化 `malf` 最小语义快照。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    materialization_run_id = run_id or _build_run_id(prefix="malf")
    resolved_market_base_path = Path(market_base_path or market_base_ledger_path(workspace))
    resolved_malf_path = Path(malf_path or malf_ledger_path(workspace))
    if not resolved_market_base_path.exists():
        raise FileNotFoundError(f"Missing market_base database: {resolved_market_base_path}")

    market_connection = duckdb.connect(str(resolved_market_base_path), read_only=True)
    malf_connection = duckdb.connect(str(resolved_malf_path))
    try:
        bootstrap_malf_ledger(workspace, connection=malf_connection)
        instrument_list = _load_target_instruments(
            market_connection,
            table_name=source_price_table,
            adjust_method=adjust_method,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=normalized_instruments,
            limit=normalized_limit,
        )
        _insert_run_row(
            malf_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=len(instrument_list),
            source_price_table=source_price_table,
            adjust_method=adjust_method,
            malf_contract_version=malf_contract_version,
        )

        all_context_rows: list[dict[str, object]] = []
        all_structure_rows: list[dict[str, object]] = []
        source_price_row_count = 0
        for batch in _chunked(instrument_list, size=normalized_batch_size):
            price_frame = _load_price_frame(
                market_connection,
                table_name=source_price_table,
                adjust_method=adjust_method,
                instruments=batch,
                signal_start_date=normalized_start_date,
                signal_end_date=normalized_end_date,
            )
            if price_frame.empty:
                continue
            source_price_row_count += int(len(price_frame))
            context_rows, structure_rows = _derive_malf_snapshots(
                price_frame,
                signal_start_date=normalized_start_date,
                signal_end_date=normalized_end_date,
                adjust_method=adjust_method,
                malf_contract_version=malf_contract_version,
                run_id=materialization_run_id,
            )
            all_context_rows.extend(context_rows)
            all_structure_rows.extend(structure_rows)

        counts = _materialize_snapshot_rows(
            malf_connection,
            run_id=materialization_run_id,
            context_rows=all_context_rows,
            structure_rows=all_structure_rows,
        )
        summary = MalfSnapshotBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            malf_contract_version=malf_contract_version,
            signal_start_date=None if normalized_start_date is None else normalized_start_date.isoformat(),
            signal_end_date=None if normalized_end_date is None else normalized_end_date.isoformat(),
            bounded_instrument_count=len(instrument_list),
            source_price_row_count=source_price_row_count,
            context_snapshot_count=len(all_context_rows),
            structure_candidate_count=len(all_structure_rows),
            context_inserted_count=counts["context_inserted_count"],
            context_reused_count=counts["context_reused_count"],
            context_rematerialized_count=counts["context_rematerialized_count"],
            structure_inserted_count=counts["structure_inserted_count"],
            structure_reused_count=counts["structure_reused_count"],
            structure_rematerialized_count=counts["structure_rematerialized_count"],
            market_base_path=str(resolved_market_base_path),
            malf_ledger_path=str(resolved_malf_path),
            source_price_table=source_price_table,
            adjust_method=adjust_method,
        )
        _mark_run_completed(malf_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception:
        _mark_run_failed(malf_connection, run_id=materialization_run_id)
        raise
    finally:
        market_connection.close()
        malf_connection.close()


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
            context_nk = _build_context_nk(code=str(code), signal_date=trade_date, asof_date=trade_date, malf_contract_version=malf_contract_version)
            candidate_nk = _build_candidate_nk(code=str(code), signal_date=trade_date, asof_date=trade_date, malf_contract_version=malf_contract_version)
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


def _insert_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_instrument_count: int,
    source_price_table: str,
    adjust_method: str,
    malf_contract_version: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO {MALF_RUN_TABLE} (
            run_id,
            runner_name,
            runner_version,
            run_status,
            signal_start_date,
            signal_end_date,
            bounded_instrument_count,
            source_price_table,
            adjust_method,
            malf_contract_version
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
            source_price_table,
            adjust_method,
            malf_contract_version,
        ],
    )


def _materialize_snapshot_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    context_rows: list[dict[str, object]],
    structure_rows: list[dict[str, object]],
) -> dict[str, int]:
    counts = {
        "context_inserted_count": 0,
        "context_reused_count": 0,
        "context_rematerialized_count": 0,
        "structure_inserted_count": 0,
        "structure_reused_count": 0,
        "structure_rematerialized_count": 0,
    }
    for row in context_rows:
        action = _upsert_context_snapshot(connection, row=row)
        _record_context_bridge(connection, run_id=run_id, context_nk=str(row["context_nk"]), action=action)
        counts[f"context_{action}_count"] += 1
    for row in structure_rows:
        action = _upsert_structure_candidate_snapshot(connection, row=row)
        _record_structure_bridge(connection, run_id=run_id, candidate_nk=str(row["candidate_nk"]), action=action)
        counts[f"structure_{action}_count"] += 1
    return counts


def _upsert_context_snapshot(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT
            context_nk,
            entity_name,
            source_context_nk,
            malf_context_4,
            lifecycle_rank_high,
            lifecycle_rank_total,
            calc_date,
            adjust_method,
            first_seen_run_id
        FROM {PAS_CONTEXT_SNAPSHOT_TABLE}
        WHERE entity_code = ?
          AND signal_date = ?
          AND asof_date = ?
        """,
        [row["entity_code"], row["signal_date"], row["asof_date"]],
    ).fetchone()
    fingerprint = (
        str(row["context_nk"]),
        str(row["entity_name"]),
        str(row["source_context_nk"]),
        str(row["malf_context_4"]),
        int(row["lifecycle_rank_high"]),
        int(row["lifecycle_rank_total"]),
        row["calc_date"],
        str(row["adjust_method"]),
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {PAS_CONTEXT_SNAPSHOT_TABLE} (
                context_nk,
                entity_code,
                entity_name,
                signal_date,
                asof_date,
                source_context_nk,
                malf_context_4,
                lifecycle_rank_high,
                lifecycle_rank_total,
                calc_date,
                adjust_method,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["context_nk"],
                row["entity_code"],
                row["entity_name"],
                row["signal_date"],
                row["asof_date"],
                row["source_context_nk"],
                row["malf_context_4"],
                row["lifecycle_rank_high"],
                row["lifecycle_rank_total"],
                row["calc_date"],
                row["adjust_method"],
                row["first_seen_run_id"],
                row["last_materialized_run_id"],
            ],
        )
        return "inserted"
    existing_fingerprint = (
        str(existing[0]) if existing[0] is not None else "",
        str(existing[1]) if existing[1] is not None else "",
        str(existing[2]) if existing[2] is not None else "",
        str(existing[3]) if existing[3] is not None else "",
        int(existing[4]) if existing[4] is not None else 0,
        int(existing[5]) if existing[5] is not None else 0,
        _coerce_date(existing[6]),
        str(existing[7]) if existing[7] is not None else "",
    )
    first_seen_run_id = str(existing[8]) if existing[8] is not None else str(row["first_seen_run_id"])
    connection.execute(
        f"""
        UPDATE {PAS_CONTEXT_SNAPSHOT_TABLE}
        SET
            context_nk = ?,
            entity_name = ?,
            source_context_nk = ?,
            malf_context_4 = ?,
            lifecycle_rank_high = ?,
            lifecycle_rank_total = ?,
            calc_date = ?,
            adjust_method = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE entity_code = ?
          AND signal_date = ?
          AND asof_date = ?
        """,
        [
            row["context_nk"],
            row["entity_name"],
            row["source_context_nk"],
            row["malf_context_4"],
            row["lifecycle_rank_high"],
            row["lifecycle_rank_total"],
            row["calc_date"],
            row["adjust_method"],
            first_seen_run_id,
            row["last_materialized_run_id"],
            row["entity_code"],
            row["signal_date"],
            row["asof_date"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _upsert_structure_candidate_snapshot(connection: duckdb.DuckDBPyConnection, *, row: dict[str, object]) -> str:
    existing = connection.execute(
        f"""
        SELECT
            candidate_nk,
            instrument_name,
            new_high_count,
            new_low_count,
            refresh_density,
            advancement_density,
            is_failed_extreme,
            failure_type,
            adjust_method,
            first_seen_run_id
        FROM {STRUCTURE_CANDIDATE_SNAPSHOT_TABLE}
        WHERE instrument = ?
          AND signal_date = ?
          AND asof_date = ?
        """,
        [row["instrument"], row["signal_date"], row["asof_date"]],
    ).fetchone()
    fingerprint = (
        str(row["candidate_nk"]),
        str(row["instrument_name"]),
        int(row["new_high_count"]),
        int(row["new_low_count"]),
        float(row["refresh_density"]),
        float(row["advancement_density"]),
        bool(row["is_failed_extreme"]),
        None if row["failure_type"] is None else str(row["failure_type"]),
        str(row["adjust_method"]),
    )
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {STRUCTURE_CANDIDATE_SNAPSHOT_TABLE} (
                candidate_nk,
                instrument,
                instrument_name,
                signal_date,
                asof_date,
                new_high_count,
                new_low_count,
                refresh_density,
                advancement_density,
                is_failed_extreme,
                failure_type,
                adjust_method,
                first_seen_run_id,
                last_materialized_run_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["candidate_nk"],
                row["instrument"],
                row["instrument_name"],
                row["signal_date"],
                row["asof_date"],
                row["new_high_count"],
                row["new_low_count"],
                row["refresh_density"],
                row["advancement_density"],
                row["is_failed_extreme"],
                row["failure_type"],
                row["adjust_method"],
                row["first_seen_run_id"],
                row["last_materialized_run_id"],
            ],
        )
        return "inserted"
    existing_fingerprint = (
        str(existing[0]) if existing[0] is not None else "",
        str(existing[1]) if existing[1] is not None else "",
        int(existing[2]) if existing[2] is not None else 0,
        int(existing[3]) if existing[3] is not None else 0,
        float(existing[4]) if existing[4] is not None else 0.0,
        float(existing[5]) if existing[5] is not None else 0.0,
        bool(existing[6]),
        None if existing[7] is None else str(existing[7]),
        str(existing[8]) if existing[8] is not None else "",
    )
    first_seen_run_id = str(existing[9]) if existing[9] is not None else str(row["first_seen_run_id"])
    connection.execute(
        f"""
        UPDATE {STRUCTURE_CANDIDATE_SNAPSHOT_TABLE}
        SET
            candidate_nk = ?,
            instrument_name = ?,
            new_high_count = ?,
            new_low_count = ?,
            refresh_density = ?,
            advancement_density = ?,
            is_failed_extreme = ?,
            failure_type = ?,
            adjust_method = ?,
            first_seen_run_id = ?,
            last_materialized_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE instrument = ?
          AND signal_date = ?
          AND asof_date = ?
        """,
        [
            row["candidate_nk"],
            row["instrument_name"],
            row["new_high_count"],
            row["new_low_count"],
            row["refresh_density"],
            row["advancement_density"],
            row["is_failed_extreme"],
            row["failure_type"],
            row["adjust_method"],
            first_seen_run_id,
            row["last_materialized_run_id"],
            row["instrument"],
            row["signal_date"],
            row["asof_date"],
        ],
    )
    return "reused" if existing_fingerprint == fingerprint else "rematerialized"


def _record_context_bridge(connection: duckdb.DuckDBPyConnection, *, run_id: str, context_nk: str, action: str) -> None:
    existing = connection.execute(
        f"SELECT run_id FROM {MALF_RUN_CONTEXT_SNAPSHOT_TABLE} WHERE run_id = ? AND context_nk = ?",
        [run_id, context_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"INSERT INTO {MALF_RUN_CONTEXT_SNAPSHOT_TABLE} (run_id, context_nk, materialization_action) VALUES (?, ?, ?)",
            [run_id, context_nk, action],
        )
        return
    connection.execute(
        f"""
        UPDATE {MALF_RUN_CONTEXT_SNAPSHOT_TABLE}
        SET materialization_action = ?, recorded_at = CURRENT_TIMESTAMP
        WHERE run_id = ? AND context_nk = ?
        """,
        [action, run_id, context_nk],
    )


def _record_structure_bridge(connection: duckdb.DuckDBPyConnection, *, run_id: str, candidate_nk: str, action: str) -> None:
    existing = connection.execute(
        f"SELECT run_id FROM {MALF_RUN_STRUCTURE_SNAPSHOT_TABLE} WHERE run_id = ? AND candidate_nk = ?",
        [run_id, candidate_nk],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"INSERT INTO {MALF_RUN_STRUCTURE_SNAPSHOT_TABLE} (run_id, candidate_nk, materialization_action) VALUES (?, ?, ?)",
            [run_id, candidate_nk, action],
        )
        return
    connection.execute(
        f"""
        UPDATE {MALF_RUN_STRUCTURE_SNAPSHOT_TABLE}
        SET materialization_action = ?, recorded_at = CURRENT_TIMESTAMP
        WHERE run_id = ? AND candidate_nk = ?
        """,
        [action, run_id, candidate_nk],
    )


def _mark_run_completed(connection: duckdb.DuckDBPyConnection, *, run_id: str, summary: MalfSnapshotBuildSummary) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_RUN_TABLE}
        SET
            run_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True), run_id],
    )


def _mark_run_failed(connection: duckdb.DuckDBPyConnection, *, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_RUN_TABLE}
        SET run_status = 'failed', completed_at = CURRENT_TIMESTAMP
        WHERE run_id = ?
        """,
        [run_id],
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


def _coerce_date(value: object | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _chunked(items: list[str], *, size: int) -> list[list[str]]:
    if not items:
        return []
    return [items[index : index + size] for index in range(0, len(items), size)]


def _build_run_id(*, prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"


def _write_summary(payload: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    output_path = Path(summary_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
