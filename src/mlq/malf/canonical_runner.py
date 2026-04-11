"""实现 canonical MALF v2 的纯语义 runner。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb
import pandas as pd

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import market_base_ledger_path
from mlq.malf.bootstrap import (
    MALF_CANONICAL_CHECKPOINT_TABLE,
    MALF_CANONICAL_RUN_TABLE,
    MALF_CANONICAL_WORK_QUEUE_TABLE,
    MALF_EXTREME_PROGRESS_LEDGER_TABLE,
    MALF_PIVOT_LEDGER_TABLE,
    MALF_SAME_LEVEL_STATS_TABLE,
    MALF_STATE_SNAPSHOT_TABLE,
    MALF_WAVE_LEDGER_TABLE,
    bootstrap_malf_ledger,
    malf_ledger_path,
)


DEFAULT_CANONICAL_MARKET_PRICE_TABLE: Final[str] = "stock_daily_adjusted"
DEFAULT_CANONICAL_ADJUST_METHOD: Final[str] = "backward"
DEFAULT_CANONICAL_CONTRACT_VERSION: Final[str] = "malf-canonical-v2"
DEFAULT_CANONICAL_RUNNER_VERSION: Final[str] = "v2"
DEFAULT_CANONICAL_SAMPLE_VERSION: Final[str] = "malf-wave-stats-v1"
DEFAULT_PIVOT_CONFIRMATION_WINDOW: Final[int] = 2
DEFAULT_TIMEFRAMES: Final[tuple[str, ...]] = ("D", "W", "M")
SUPPORTED_TIMEFRAMES: Final[tuple[str, ...]] = ("D", "W", "M")


@dataclass(frozen=True)
class MalfCanonicalBuildSummary:
    run_id: str
    runner_name: str
    runner_version: str
    canonical_contract_version: str
    signal_start_date: str | None
    signal_end_date: str | None
    timeframe_list: list[str]
    bounded_scope_count: int
    claimed_scope_count: int
    completed_scope_count: int
    failed_scope_count: int
    queue_enqueued_count: int
    queue_claimed_count: int
    pivot_row_count: int
    wave_row_count: int
    extreme_row_count: int
    state_row_count: int
    stats_row_count: int
    pivot_inserted_count: int
    pivot_reused_count: int
    pivot_rematerialized_count: int
    wave_inserted_count: int
    wave_reused_count: int
    wave_rematerialized_count: int
    extreme_inserted_count: int
    extreme_reused_count: int
    extreme_rematerialized_count: int
    state_inserted_count: int
    state_reused_count: int
    state_rematerialized_count: int
    stats_inserted_count: int
    stats_reused_count: int
    stats_rematerialized_count: int
    market_base_path: str
    malf_ledger_path: str
    source_price_table: str
    source_adjust_method: str
    pivot_confirmation_window: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _Pivot:
    pivot_nk: str
    asset_type: str
    code: str
    timeframe: str
    pivot_type: str
    pivot_bar_dt: date
    confirmed_at: date
    pivot_price: float
    prior_pivot_nk: str | None


def run_malf_canonical_build(
    *,
    settings: WorkspaceRoots | None = None,
    market_base_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    asset_type: str = "stock",
    timeframes: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    adjust_method: str = DEFAULT_CANONICAL_ADJUST_METHOD,
    source_price_table: str = DEFAULT_CANONICAL_MARKET_PRICE_TABLE,
    canonical_contract_version: str = DEFAULT_CANONICAL_CONTRACT_VERSION,
    sample_version: str = DEFAULT_CANONICAL_SAMPLE_VERSION,
    pivot_confirmation_window: int = DEFAULT_PIVOT_CONFIRMATION_WINDOW,
    run_id: str | None = None,
    runner_name: str = "malf_canonical_builder",
    runner_version: str = DEFAULT_CANONICAL_RUNNER_VERSION,
    summary_path: Path | None = None,
) -> MalfCanonicalBuildSummary:
    """从官方 market_base 构建 canonical MALF v2 历史账本。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_instruments = tuple(sorted(_normalize_instruments(instruments)))
    normalized_limit = max(int(limit), 1)
    normalized_timeframes = _normalize_timeframes(timeframes)
    normalized_window = max(int(pivot_confirmation_window), 1)
    materialization_run_id = run_id or _build_run_id(prefix="malf-canonical")
    resolved_market_base_path = Path(market_base_path or market_base_ledger_path(workspace))
    resolved_malf_path = Path(malf_path or malf_ledger_path(workspace))
    if not resolved_market_base_path.exists():
        raise FileNotFoundError(f"Missing market_base database: {resolved_market_base_path}")

    market_connection = duckdb.connect(str(resolved_market_base_path), read_only=True)
    malf_connection = duckdb.connect(str(resolved_malf_path))
    claimed_scope_rows: list[dict[str, object]] = []
    try:
        bootstrap_malf_ledger(workspace, connection=malf_connection)
        scope_rows = _load_source_scope_rows(
            market_connection,
            table_name=source_price_table,
            adjust_method=adjust_method,
            asset_type=asset_type,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            instruments=normalized_instruments,
            limit=normalized_limit,
        )
        enqueue_counts = _enqueue_dirty_scopes(
            malf_connection,
            scope_rows=scope_rows,
            timeframes=normalized_timeframes,
            run_id=materialization_run_id,
        )
        claimed_scope_rows = _claim_ready_scopes(
            malf_connection,
            asset_type=asset_type,
            run_id=materialization_run_id,
        )
        _insert_canonical_run_row(
            malf_connection,
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_scope_count=len(scope_rows) * len(normalized_timeframes),
            pivot_confirmation_window=normalized_window,
            source_price_table=source_price_table,
            source_adjust_method=adjust_method,
            canonical_contract_version=canonical_contract_version,
            timeframe_list=normalized_timeframes,
        )

        count_totals = {
            "pivot_row_count": 0,
            "wave_row_count": 0,
            "extreme_row_count": 0,
            "state_row_count": 0,
            "stats_row_count": 0,
            "pivot_inserted_count": 0,
            "pivot_reused_count": 0,
            "pivot_rematerialized_count": 0,
            "wave_inserted_count": 0,
            "wave_reused_count": 0,
            "wave_rematerialized_count": 0,
            "extreme_inserted_count": 0,
            "extreme_reused_count": 0,
            "extreme_rematerialized_count": 0,
            "state_inserted_count": 0,
            "state_reused_count": 0,
            "state_rematerialized_count": 0,
            "stats_inserted_count": 0,
            "stats_reused_count": 0,
            "stats_rematerialized_count": 0,
        }
        completed_scope_count = 0
        failed_scope_count = 0
        for scope_row in claimed_scope_rows:
            try:
                base_frame = _load_source_bars(
                    market_connection,
                    table_name=source_price_table,
                    code=str(scope_row["code"]),
                    adjust_method=adjust_method,
                    signal_end_date=normalized_end_date,
                )
                if base_frame.empty:
                    _mark_queue_completed(
                        malf_connection,
                        queue_nk=str(scope_row["queue_nk"]),
                        run_id=materialization_run_id,
                    )
                    completed_scope_count += 1
                    continue
                timeframe_frame = _resample_bars_by_timeframe(base_frame, str(scope_row["timeframe"]))
                if timeframe_frame.empty:
                    _mark_queue_completed(
                        malf_connection,
                        queue_nk=str(scope_row["queue_nk"]),
                        run_id=materialization_run_id,
                    )
                    completed_scope_count += 1
                    continue
                materialized = _build_scope_materialization(
                    timeframe_frame=timeframe_frame,
                    asset_type=asset_type,
                    code=str(scope_row["code"]),
                    timeframe=str(scope_row["timeframe"]),
                    run_id=materialization_run_id,
                    sample_version=sample_version,
                    pivot_confirmation_window=normalized_window,
                )
                counts = _materialize_scope_rows(
                    malf_connection,
                    asset_type=asset_type,
                    code=str(scope_row["code"]),
                    timeframe=str(scope_row["timeframe"]),
                    run_id=materialization_run_id,
                    materialized=materialized,
                )
                for key, value in counts.items():
                    count_totals[key] += value
                _upsert_checkpoint(
                    malf_connection,
                    asset_type=asset_type,
                    code=str(scope_row["code"]),
                    timeframe=str(scope_row["timeframe"]),
                    last_completed_bar_dt=_to_python_date(timeframe_frame["trade_date"].max()),
                    tail_start_bar_dt=_to_python_date(timeframe_frame["trade_date"].min()),
                    tail_confirm_until_dt=_to_python_date(timeframe_frame["trade_date"].max()),
                    last_wave_id=max((int(row["wave_id"]) for row in materialized["waves"]), default=0),
                    last_run_id=materialization_run_id,
                )
                _mark_queue_completed(
                    malf_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
                completed_scope_count += 1
            except Exception:
                failed_scope_count += 1
                _mark_queue_failed(
                    malf_connection,
                    queue_nk=str(scope_row["queue_nk"]),
                    run_id=materialization_run_id,
                )
                raise

        summary = MalfCanonicalBuildSummary(
            run_id=materialization_run_id,
            runner_name=runner_name,
            runner_version=runner_version,
            canonical_contract_version=canonical_contract_version,
            signal_start_date=None if normalized_start_date is None else normalized_start_date.isoformat(),
            signal_end_date=None if normalized_end_date is None else normalized_end_date.isoformat(),
            timeframe_list=list(normalized_timeframes),
            bounded_scope_count=len(scope_rows) * len(normalized_timeframes),
            claimed_scope_count=len(claimed_scope_rows),
            completed_scope_count=completed_scope_count,
            failed_scope_count=failed_scope_count,
            queue_enqueued_count=enqueue_counts["queue_enqueued_count"],
            queue_claimed_count=len(claimed_scope_rows),
            pivot_row_count=count_totals["pivot_row_count"],
            wave_row_count=count_totals["wave_row_count"],
            extreme_row_count=count_totals["extreme_row_count"],
            state_row_count=count_totals["state_row_count"],
            stats_row_count=count_totals["stats_row_count"],
            pivot_inserted_count=count_totals["pivot_inserted_count"],
            pivot_reused_count=count_totals["pivot_reused_count"],
            pivot_rematerialized_count=count_totals["pivot_rematerialized_count"],
            wave_inserted_count=count_totals["wave_inserted_count"],
            wave_reused_count=count_totals["wave_reused_count"],
            wave_rematerialized_count=count_totals["wave_rematerialized_count"],
            extreme_inserted_count=count_totals["extreme_inserted_count"],
            extreme_reused_count=count_totals["extreme_reused_count"],
            extreme_rematerialized_count=count_totals["extreme_rematerialized_count"],
            state_inserted_count=count_totals["state_inserted_count"],
            state_reused_count=count_totals["state_reused_count"],
            state_rematerialized_count=count_totals["state_rematerialized_count"],
            stats_inserted_count=count_totals["stats_inserted_count"],
            stats_reused_count=count_totals["stats_reused_count"],
            stats_rematerialized_count=count_totals["stats_rematerialized_count"],
            market_base_path=str(resolved_market_base_path),
            malf_ledger_path=str(resolved_malf_path),
            source_price_table=source_price_table,
            source_adjust_method=adjust_method,
            pivot_confirmation_window=normalized_window,
        )
        _mark_canonical_run_completed(malf_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary.as_dict(), summary_path)
        return summary
    except Exception:
        _mark_canonical_run_failed(malf_connection, run_id=materialization_run_id)
        raise
    finally:
        market_connection.close()
        malf_connection.close()


def _load_source_scope_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    adjust_method: str,
    asset_type: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    instruments: tuple[str, ...],
    limit: int,
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
    rows = connection.execute(
        f"""
        SELECT {code_column} AS code, MAX({date_column}) AS last_trade_date
        FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
        GROUP BY {code_column}
        ORDER BY {code_column}
        LIMIT ?
        """,
        [*parameters, limit],
    ).fetchall()
    return [
        {
            "asset_type": asset_type,
            "code": str(code),
            "last_trade_date": _to_python_date(last_trade_date),
        }
        for code, last_trade_date in rows
    ]


def _enqueue_dirty_scopes(
    connection: duckdb.DuckDBPyConnection,
    *,
    scope_rows: list[dict[str, object]],
    timeframes: tuple[str, ...],
    run_id: str,
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
            last_completed_bar_dt = _to_python_date(checkpoint_row[0]) if checkpoint_row else None
            source_last_trade_date = _to_python_date(scope_row["last_trade_date"])
            if last_completed_bar_dt is not None and source_last_trade_date is not None and source_last_trade_date <= last_completed_bar_dt:
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
                "source_last_trade_date": _to_python_date(row[6]),
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
        return pd.DataFrame(columns=["code", "name", "trade_date", "open", "high", "low", "close", "volume", "amount"])
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


def _build_scope_materialization(
    *,
    timeframe_frame: pd.DataFrame,
    asset_type: str,
    code: str,
    timeframe: str,
    run_id: str,
    sample_version: str,
    pivot_confirmation_window: int,
) -> dict[str, list[dict[str, object]]]:
    pivots = _detect_confirmed_pivots(
        timeframe_frame=timeframe_frame,
        asset_type=asset_type,
        code=code,
        timeframe=timeframe,
        pivot_confirmation_window=pivot_confirmation_window,
    )
    pivot_rows = [_pivot_as_row(pivot, run_id=run_id) for pivot in pivots]
    pivot_rows_by_confirmed_at: dict[date, list[_Pivot]] = {}
    for pivot in pivots:
        pivot_rows_by_confirmed_at.setdefault(pivot.confirmed_at, []).append(pivot)

    wave_rows: list[dict[str, object]] = []
    state_rows: list[dict[str, object]] = []
    extreme_rows: list[dict[str, object]] = []
    if timeframe_frame.empty:
        return {"pivots": pivot_rows, "waves": wave_rows, "extremes": extreme_rows, "states": state_rows, "stats": []}

    mode = "up"
    if len(timeframe_frame) >= 2 and float(timeframe_frame.loc[1, "close"]) < float(timeframe_frame.loc[0, "close"]):
        mode = "down"
    current_wave_id = 1
    first_row = timeframe_frame.iloc[0]
    active_wave = _new_wave_row(
        asset_type=asset_type,
        code=code,
        timeframe=timeframe,
        wave_id=current_wave_id,
        direction=mode,
        major_state="牛顺" if mode == "up" else "熊顺",
        reversal_stage="none",
        start_bar_dt=_to_python_date(first_row["trade_date"]),
        start_pivot_nk=None,
        run_id=run_id,
    )
    active_wave_high = float(first_row["high"])
    active_wave_low = float(first_row["low"])
    hh_count = 0
    ll_count = 0
    last_confirmed_h: _Pivot | None = None
    last_confirmed_l: _Pivot | None = None
    last_valid_hl: _Pivot | None = None
    last_valid_lh: _Pivot | None = None
    reversal_stage = "none"
    reversal_guard_price: float | None = None
    reversal_trigger_bar_dt: date | None = None
    reversal_guard_pivot_nk: str | None = None
    extreme_seq = 0

    def switch_mode(new_mode: str, trigger_bar_dt: date, guard_price: float | None, guard_pivot_nk: str | None) -> None:
        nonlocal mode, current_wave_id, active_wave, active_wave_high, active_wave_low
        nonlocal hh_count, ll_count, reversal_stage, reversal_guard_price, reversal_trigger_bar_dt, reversal_guard_pivot_nk
        _finalize_wave(
            active_wave,
            wave_rows=wave_rows,
            end_bar_dt=trigger_bar_dt,
            active_flag=False,
            hh_count=hh_count,
            ll_count=ll_count,
            bar_count=int(active_wave["bar_count"]),
            wave_high=active_wave_high,
            wave_low=active_wave_low,
        )
        current_wave_id += 1
        mode = new_mode
        active_wave = _new_wave_row(
            asset_type=asset_type,
            code=code,
            timeframe=timeframe,
            wave_id=current_wave_id,
            direction=new_mode,
            major_state="熊逆" if new_mode == "up" else "牛逆",
            reversal_stage="trigger",
            start_bar_dt=trigger_bar_dt,
            start_pivot_nk=guard_pivot_nk,
            run_id=run_id,
        )
        hh_count = 0
        ll_count = 0
        reversal_stage = "trigger"
        reversal_guard_price = guard_price
        reversal_trigger_bar_dt = trigger_bar_dt
        reversal_guard_pivot_nk = guard_pivot_nk

    for row in timeframe_frame.itertuples(index=False):
        trade_date = _to_python_date(row.trade_date)
        bar_high = float(row.high)
        bar_low = float(row.low)
        bar_close = float(row.close)
        active_wave["bar_count"] = int(active_wave["bar_count"]) + 1
        previous_wave_high = active_wave_high
        previous_wave_low = active_wave_low
        active_wave_high = max(active_wave_high, bar_high)
        active_wave_low = min(active_wave_low, bar_low)

        for pivot in pivot_rows_by_confirmed_at.get(trade_date, []):
            if pivot.pivot_type == "H":
                last_confirmed_h = pivot
                if mode == "down" and (last_valid_lh is None or pivot.pivot_price <= last_valid_lh.pivot_price):
                    last_valid_lh = pivot
            else:
                last_confirmed_l = pivot
                if mode == "up" and (last_valid_hl is None or pivot.pivot_price >= last_valid_hl.pivot_price):
                    last_valid_hl = pivot

        if reversal_stage == "trigger" and reversal_trigger_bar_dt is not None and trade_date > reversal_trigger_bar_dt:
            if mode == "up" and reversal_guard_price is not None:
                if bar_low >= reversal_guard_price:
                    reversal_stage = "hold"
                elif bar_low < reversal_guard_price:
                    switch_mode("down", trade_date, last_valid_hl.pivot_price if last_valid_hl else None, last_valid_hl.pivot_nk if last_valid_hl else None)
                    active_wave_high = bar_high
                    active_wave_low = bar_low
            elif mode == "down" and reversal_guard_price is not None:
                if bar_high <= reversal_guard_price:
                    reversal_stage = "hold"
                elif bar_high > reversal_guard_price:
                    switch_mode("up", trade_date, last_valid_lh.pivot_price if last_valid_lh else None, last_valid_lh.pivot_nk if last_valid_lh else None)
                    active_wave_high = bar_high
                    active_wave_low = bar_low

        if mode == "up" and last_valid_hl is not None and bar_close < last_valid_hl.pivot_price:
            switch_mode("down", trade_date, last_valid_hl.pivot_price, last_valid_hl.pivot_nk)
            active_wave_high = bar_high
            active_wave_low = bar_low
        elif mode == "down" and last_valid_lh is not None and bar_close > last_valid_lh.pivot_price:
            switch_mode("up", trade_date, last_valid_lh.pivot_price, last_valid_lh.pivot_nk)
            active_wave_high = bar_high
            active_wave_low = bar_low
        else:
            if mode == "up" and bar_high > previous_wave_high:
                hh_count += 1
                extreme_seq += 1
                extreme_rows.append(
                    {
                        "extreme_nk": _build_extreme_nk(asset_type, code, timeframe, current_wave_id, extreme_seq),
                        "asset_type": asset_type,
                        "code": code,
                        "timeframe": timeframe,
                        "wave_id": current_wave_id,
                        "extreme_seq": extreme_seq,
                        "extreme_type": "HH",
                        "break_base_extreme_nk": reversal_guard_pivot_nk,
                        "record_bar_dt": trade_date,
                        "record_price": bar_high,
                        "cumulative_count": hh_count,
                        "major_state": "牛顺",
                        "trend_direction": "up",
                        "first_seen_run_id": run_id,
                        "last_materialized_run_id": run_id,
                    }
                )
                if reversal_stage in {"trigger", "hold"}:
                    reversal_stage = "expand"
            elif mode == "down" and bar_low < previous_wave_low:
                ll_count += 1
                extreme_seq += 1
                extreme_rows.append(
                    {
                        "extreme_nk": _build_extreme_nk(asset_type, code, timeframe, current_wave_id, extreme_seq),
                        "asset_type": asset_type,
                        "code": code,
                        "timeframe": timeframe,
                        "wave_id": current_wave_id,
                        "extreme_seq": extreme_seq,
                        "extreme_type": "LL",
                        "break_base_extreme_nk": reversal_guard_pivot_nk,
                        "record_bar_dt": trade_date,
                        "record_price": bar_low,
                        "cumulative_count": ll_count,
                        "major_state": "熊顺",
                        "trend_direction": "down",
                        "first_seen_run_id": run_id,
                        "last_materialized_run_id": run_id,
                    }
                )
                if reversal_stage in {"trigger", "hold"}:
                    reversal_stage = "expand"

        major_state = _derive_major_state(mode=mode, hh_count=hh_count, ll_count=ll_count)
        active_wave["major_state"] = major_state
        active_wave["reversal_stage"] = reversal_stage
        active_wave["end_bar_dt"] = trade_date
        active_wave["hh_count"] = hh_count
        active_wave["ll_count"] = ll_count
        active_wave["wave_high"] = active_wave_high
        active_wave["wave_low"] = active_wave_low
        active_wave["range_ratio"] = _calculate_range_ratio(active_wave_high, active_wave_low, bar_close)
        state_rows.append(
            {
                "snapshot_nk": _build_snapshot_nk(asset_type, code, timeframe, trade_date),
                "asset_type": asset_type,
                "code": code,
                "timeframe": timeframe,
                "asof_bar_dt": trade_date,
                "major_state": major_state,
                "trend_direction": mode,
                "reversal_stage": reversal_stage,
                "wave_id": current_wave_id,
                "last_confirmed_h_bar_dt": None if last_confirmed_h is None else last_confirmed_h.pivot_bar_dt,
                "last_confirmed_h_price": None if last_confirmed_h is None else last_confirmed_h.pivot_price,
                "last_confirmed_l_bar_dt": None if last_confirmed_l is None else last_confirmed_l.pivot_bar_dt,
                "last_confirmed_l_price": None if last_confirmed_l is None else last_confirmed_l.pivot_price,
                "last_valid_hl_bar_dt": None if last_valid_hl is None else last_valid_hl.pivot_bar_dt,
                "last_valid_hl_price": None if last_valid_hl is None else last_valid_hl.pivot_price,
                "last_valid_lh_bar_dt": None if last_valid_lh is None else last_valid_lh.pivot_bar_dt,
                "last_valid_lh_price": None if last_valid_lh is None else last_valid_lh.pivot_price,
                "current_hh_count": hh_count,
                "current_ll_count": ll_count,
                "first_seen_run_id": run_id,
                "last_materialized_run_id": run_id,
            }
        )

    _finalize_wave(
        active_wave,
        wave_rows=wave_rows,
        end_bar_dt=_to_python_date(timeframe_frame["trade_date"].max()),
        active_flag=True,
        hh_count=hh_count,
        ll_count=ll_count,
        bar_count=int(active_wave["bar_count"]),
        wave_high=active_wave_high,
        wave_low=active_wave_low,
    )
    stats_rows = _build_stats_rows(
        asset_type=asset_type,
        code=code,
        timeframe=timeframe,
        wave_rows=wave_rows,
        sample_version=sample_version,
        run_id=run_id,
    )
    return {"pivots": pivot_rows, "waves": wave_rows, "extremes": extreme_rows, "states": state_rows, "stats": stats_rows}


def _detect_confirmed_pivots(
    *,
    timeframe_frame: pd.DataFrame,
    asset_type: str,
    code: str,
    timeframe: str,
    pivot_confirmation_window: int,
) -> list[_Pivot]:
    if len(timeframe_frame) < pivot_confirmation_window + 2:
        return []
    highs = timeframe_frame["high"].tolist()
    lows = timeframe_frame["low"].tolist()
    trade_dates = [_to_python_date(value) for value in timeframe_frame["trade_date"].tolist()]
    pivots: list[_Pivot] = []
    prior_pivot_nk: str | None = None
    for index in range(1, len(timeframe_frame) - pivot_confirmation_window):
        current_high = float(highs[index])
        current_low = float(lows[index])
        prior_high = float(highs[index - 1])
        prior_low = float(lows[index - 1])
        future_highs = [float(value) for value in highs[index + 1 : index + 1 + pivot_confirmation_window]]
        future_lows = [float(value) for value in lows[index + 1 : index + 1 + pivot_confirmation_window]]
        confirmed_at = trade_dates[index + pivot_confirmation_window]
        pivot_bar_dt = trade_dates[index]
        if current_high > prior_high and all(current_high > candidate for candidate in future_highs):
            pivot_nk = _build_pivot_nk(asset_type, code, timeframe, "H", pivot_bar_dt)
            pivots.append(
                _Pivot(
                    pivot_nk=pivot_nk,
                    asset_type=asset_type,
                    code=code,
                    timeframe=timeframe,
                    pivot_type="H",
                    pivot_bar_dt=pivot_bar_dt,
                    confirmed_at=confirmed_at,
                    pivot_price=current_high,
                    prior_pivot_nk=prior_pivot_nk,
                )
            )
            prior_pivot_nk = pivot_nk
        if current_low < prior_low and all(current_low < candidate for candidate in future_lows):
            pivot_nk = _build_pivot_nk(asset_type, code, timeframe, "L", pivot_bar_dt)
            pivots.append(
                _Pivot(
                    pivot_nk=pivot_nk,
                    asset_type=asset_type,
                    code=code,
                    timeframe=timeframe,
                    pivot_type="L",
                    pivot_bar_dt=pivot_bar_dt,
                    confirmed_at=confirmed_at,
                    pivot_price=current_low,
                    prior_pivot_nk=prior_pivot_nk,
                )
            )
            prior_pivot_nk = pivot_nk
    pivots.sort(key=lambda item: (item.confirmed_at, item.pivot_bar_dt, item.pivot_type))
    return pivots


def _pivot_as_row(pivot: _Pivot, *, run_id: str) -> dict[str, object]:
    return {
        "pivot_nk": pivot.pivot_nk,
        "asset_type": pivot.asset_type,
        "code": pivot.code,
        "timeframe": pivot.timeframe,
        "pivot_type": pivot.pivot_type,
        "pivot_bar_dt": pivot.pivot_bar_dt,
        "confirmed_at": pivot.confirmed_at,
        "pivot_price": pivot.pivot_price,
        "prior_pivot_nk": pivot.prior_pivot_nk,
        "first_seen_run_id": run_id,
        "last_materialized_run_id": run_id,
    }


def _build_stats_rows(
    *,
    asset_type: str,
    code: str,
    timeframe: str,
    wave_rows: list[dict[str, object]],
    sample_version: str,
    run_id: str,
) -> list[dict[str, object]]:
    completed_waves = [row for row in wave_rows if not bool(row["active_flag"])]
    # 初次建仓时如果一个 scope 还没有形成 completed wave，严格只吃 completed
    # 会让 same_level_stats 完全空表。这里允许在“零 completed wave”的边界下
    # 用当前 active wave 启动最小统计，以保证 card 30 的 canonical stats
    # 在批量建仓和增量续跑首轮都具备正式落表事实。
    sample_waves = completed_waves or list(wave_rows)
    if not sample_waves:
        return []
    universe = f"{asset_type}:{code}"
    rows: list[dict[str, object]] = []
    metrics = {
        "hh_count": lambda row: float(row["hh_count"]),
        "ll_count": lambda row: float(row["ll_count"]),
        "wave_duration_bars": lambda row: float(row["bar_count"]),
        "wave_range_ratio": lambda row: float(row["range_ratio"] or 0.0),
    }
    for major_state in sorted({str(row["major_state"]) for row in sample_waves}):
        state_rows = [row for row in sample_waves if str(row["major_state"]) == major_state]
        for metric_name, extractor in metrics.items():
            values = [extractor(row) for row in state_rows]
            if not values:
                continue
            series = pd.Series(values, dtype="float64")
            rows.append(
                {
                    "stats_nk": _build_stats_nk(universe, timeframe, major_state, metric_name, sample_version),
                    "universe": universe,
                    "timeframe": timeframe,
                    "major_state": major_state,
                    "metric_name": metric_name,
                    "sample_version": sample_version,
                    "sample_size": int(series.shape[0]),
                    "p10": _series_quantile(series, 0.10),
                    "p25": _series_quantile(series, 0.25),
                    "p50": _series_quantile(series, 0.50),
                    "p75": _series_quantile(series, 0.75),
                    "p90": _series_quantile(series, 0.90),
                    "mean": float(series.mean()),
                    "std": float(series.std(ddof=0)),
                    "first_seen_run_id": run_id,
                    "last_materialized_run_id": run_id,
                }
            )
    return rows


def _materialize_scope_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    code: str,
    timeframe: str,
    run_id: str,
    materialized: dict[str, list[dict[str, object]]],
) -> dict[str, int]:
    counts: dict[str, int] = {
        "pivot_row_count": len(materialized["pivots"]),
        "wave_row_count": len(materialized["waves"]),
        "extreme_row_count": len(materialized["extremes"]),
        "state_row_count": len(materialized["states"]),
        "stats_row_count": len(materialized["stats"]),
        "pivot_inserted_count": 0,
        "pivot_reused_count": 0,
        "pivot_rematerialized_count": 0,
        "wave_inserted_count": 0,
        "wave_reused_count": 0,
        "wave_rematerialized_count": 0,
        "extreme_inserted_count": 0,
        "extreme_reused_count": 0,
        "extreme_rematerialized_count": 0,
        "state_inserted_count": 0,
        "state_reused_count": 0,
        "state_rematerialized_count": 0,
        "stats_inserted_count": 0,
        "stats_reused_count": 0,
        "stats_rematerialized_count": 0,
    }
    replace_specs = [
        ("pivot", MALF_PIVOT_LEDGER_TABLE, "pivot_nk", materialized["pivots"], "asset_type = ? AND code = ? AND timeframe = ?", [asset_type, code, timeframe]),
        ("wave", MALF_WAVE_LEDGER_TABLE, "wave_nk", materialized["waves"], "asset_type = ? AND code = ? AND timeframe = ?", [asset_type, code, timeframe]),
        ("extreme", MALF_EXTREME_PROGRESS_LEDGER_TABLE, "extreme_nk", materialized["extremes"], "asset_type = ? AND code = ? AND timeframe = ?", [asset_type, code, timeframe]),
        ("state", MALF_STATE_SNAPSHOT_TABLE, "snapshot_nk", materialized["states"], "asset_type = ? AND code = ? AND timeframe = ?", [asset_type, code, timeframe]),
        ("stats", MALF_SAME_LEVEL_STATS_TABLE, "stats_nk", materialized["stats"], "universe = ? AND timeframe = ?", [f"{asset_type}:{code}", timeframe]),
    ]
    for prefix, table_name, nk_column, rows, where_clause, where_params in replace_specs:
        replace_counts = _replace_scope_rows(
            connection,
            table_name=table_name,
            nk_column=nk_column,
            rows=rows,
            where_clause=where_clause,
            where_params=where_params,
        )
        counts[f"{prefix}_inserted_count"] = replace_counts["inserted_count"]
        counts[f"{prefix}_reused_count"] = replace_counts["reused_count"]
        counts[f"{prefix}_rematerialized_count"] = replace_counts["rematerialized_count"]
    return counts


def _replace_scope_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    nk_column: str,
    rows: list[dict[str, object]],
    where_clause: str,
    where_params: list[object],
) -> dict[str, int]:
    existing_rows = connection.execute(
        f"SELECT * FROM {table_name} WHERE {where_clause}",
        where_params,
    ).fetchdf()
    existing_map: dict[str, dict[str, object]] = {}
    if not existing_rows.empty:
        for row in existing_rows.to_dict(orient="records"):
            existing_map[str(row[nk_column])] = row
    inserted_count = 0
    reused_count = 0
    rematerialized_count = 0
    for row in rows:
        nk = str(row[nk_column])
        existing_row = existing_map.get(nk)
        if existing_row is None:
            inserted_count += 1
            continue
        row["first_seen_run_id"] = existing_row.get("first_seen_run_id") or row.get("first_seen_run_id")
        if _normalize_row_for_compare(existing_row) == _normalize_row_for_compare(row):
            reused_count += 1
        else:
            rematerialized_count += 1
    connection.execute(f"DELETE FROM {table_name} WHERE {where_clause}", where_params)
    if rows:
        frame = pd.DataFrame(rows)
        temp_name = f"tmp_{table_name}"
        connection.register(temp_name, frame)
        try:
            connection.execute(
                f"""
                INSERT INTO {table_name} ({', '.join(frame.columns)})
                SELECT {', '.join(frame.columns)}
                FROM {temp_name}
                """
            )
        finally:
            connection.unregister(temp_name)
    return {"inserted_count": inserted_count, "reused_count": reused_count, "rematerialized_count": rematerialized_count}


def _upsert_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    *,
    asset_type: str,
    code: str,
    timeframe: str,
    last_completed_bar_dt: date | None,
    tail_start_bar_dt: date | None,
    tail_confirm_until_dt: date | None,
    last_wave_id: int,
    last_run_id: str,
) -> None:
    existing = connection.execute(
        f"""
        SELECT asset_type
        FROM {MALF_CANONICAL_CHECKPOINT_TABLE}
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [asset_type, code, timeframe],
    ).fetchone()
    if existing is None:
        connection.execute(
            f"""
            INSERT INTO {MALF_CANONICAL_CHECKPOINT_TABLE} (
                asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt,
                tail_confirm_until_dt, last_wave_id, last_run_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [asset_type, code, timeframe, last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, last_wave_id, last_run_id],
        )
        return
    connection.execute(
        f"""
        UPDATE {MALF_CANONICAL_CHECKPOINT_TABLE}
        SET last_completed_bar_dt = ?,
            tail_start_bar_dt = ?,
            tail_confirm_until_dt = ?,
            last_wave_id = ?,
            last_run_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE asset_type = ?
          AND code = ?
          AND timeframe = ?
        """,
        [last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, last_wave_id, last_run_id, asset_type, code, timeframe],
    )


def _insert_canonical_run_row(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    runner_name: str,
    runner_version: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
    bounded_scope_count: int,
    pivot_confirmation_window: int,
    source_price_table: str,
    source_adjust_method: str,
    canonical_contract_version: str,
    timeframe_list: tuple[str, ...],
) -> None:
    connection.execute(
        f"""
        INSERT INTO {MALF_CANONICAL_RUN_TABLE} (
            run_id, runner_name, runner_version, run_status, signal_start_date, signal_end_date,
            bounded_scope_count, pivot_confirmation_window, source_price_table, source_adjust_method,
            canonical_contract_version, timeframe_list_json
        )
        VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            runner_name,
            runner_version,
            signal_start_date,
            signal_end_date,
            bounded_scope_count,
            pivot_confirmation_window,
            source_price_table,
            source_adjust_method,
            canonical_contract_version,
            json.dumps(list(timeframe_list), ensure_ascii=False),
        ],
    )


def _mark_canonical_run_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    summary: MalfCanonicalBuildSummary,
) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_CANONICAL_RUN_TABLE}
        SET run_status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        [json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True), run_id],
    )


def _mark_canonical_run_failed(connection: duckdb.DuckDBPyConnection, *, run_id: str) -> None:
    connection.execute(
        f"""
        UPDATE {MALF_CANONICAL_RUN_TABLE}
        SET run_status = 'failed',
            completed_at = CURRENT_TIMESTAMP
        WHERE run_id = ?
        """,
        [run_id],
    )


def _new_wave_row(
    *,
    asset_type: str,
    code: str,
    timeframe: str,
    wave_id: int,
    direction: str,
    major_state: str,
    reversal_stage: str,
    start_bar_dt: date,
    start_pivot_nk: str | None,
    run_id: str,
) -> dict[str, object]:
    return {
        "wave_nk": _build_wave_nk(asset_type, code, timeframe, wave_id),
        "asset_type": asset_type,
        "code": code,
        "timeframe": timeframe,
        "wave_id": wave_id,
        "direction": direction,
        "major_state": major_state,
        "reversal_stage": reversal_stage,
        "start_bar_dt": start_bar_dt,
        "end_bar_dt": None,
        "active_flag": True,
        "start_pivot_nk": start_pivot_nk,
        "end_pivot_nk": None,
        "hh_count": 0,
        "ll_count": 0,
        "bar_count": 0,
        "wave_high": None,
        "wave_low": None,
        "range_ratio": None,
        "first_seen_run_id": run_id,
        "last_materialized_run_id": run_id,
    }


def _finalize_wave(
    wave_row: dict[str, object],
    *,
    wave_rows: list[dict[str, object]],
    end_bar_dt: date,
    active_flag: bool,
    hh_count: int,
    ll_count: int,
    bar_count: int,
    wave_high: float,
    wave_low: float,
) -> None:
    finalized = dict(wave_row)
    finalized["end_bar_dt"] = end_bar_dt
    finalized["active_flag"] = active_flag
    finalized["hh_count"] = hh_count
    finalized["ll_count"] = ll_count
    finalized["bar_count"] = bar_count
    finalized["wave_high"] = wave_high
    finalized["wave_low"] = wave_low
    finalized["range_ratio"] = _calculate_range_ratio(wave_high, wave_low, wave_high if wave_high != 0 else 1.0)
    wave_rows.append(finalized)


def _derive_major_state(*, mode: str, hh_count: int, ll_count: int) -> str:
    if mode == "up":
        return "牛顺" if hh_count > 0 else "熊逆"
    return "熊顺" if ll_count > 0 else "牛逆"


def _calculate_range_ratio(wave_high: float, wave_low: float, denominator: float) -> float:
    base = abs(float(denominator)) if denominator not in (None, 0.0) else max(abs(wave_high), 1.0)
    return float((wave_high - wave_low) / base)


def _series_quantile(series: pd.Series, quantile: float) -> float | None:
    if series.empty:
        return None
    return float(series.quantile(quantile))


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


def _normalize_timeframes(timeframes: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if not timeframes:
        return DEFAULT_TIMEFRAMES
    normalized = tuple(dict.fromkeys(str(value).strip().upper() for value in timeframes if str(value).strip()))
    invalid = [value for value in normalized if value not in SUPPORTED_TIMEFRAMES]
    if invalid:
        raise ValueError(f"Unsupported timeframes: {invalid}")
    return normalized


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


def _normalize_row_for_compare(row: dict[str, object]) -> dict[str, object]:
    ignored = {"first_seen_run_id", "last_materialized_run_id", "created_at", "updated_at"}
    normalized: dict[str, object] = {}
    for key, value in row.items():
        if key in ignored:
            continue
        if pd.isna(value):
            normalized[key] = None
        elif isinstance(value, (datetime, pd.Timestamp)):
            normalized[key] = value.date().isoformat()
        elif isinstance(value, date):
            normalized[key] = value.isoformat()
        elif isinstance(value, float):
            normalized[key] = round(float(value), 12)
        else:
            normalized[key] = value
    return normalized


def _to_python_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date()
    return pd.Timestamp(value).date()


def _coerce_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value)).date()


def _build_run_id(*, prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}"


def _build_scope_nk(*, asset_type: str, code: str, timeframe: str) -> str:
    return f"{asset_type}|{code}|{timeframe}"


def _build_queue_nk(*, scope_nk: str, dirty_reason: str) -> str:
    return f"{scope_nk}|{dirty_reason}"


def _build_pivot_nk(asset_type: str, code: str, timeframe: str, pivot_type: str, pivot_bar_dt: date) -> str:
    return f"{asset_type}|{code}|{timeframe}|{pivot_type}|{pivot_bar_dt.isoformat()}"


def _build_wave_nk(asset_type: str, code: str, timeframe: str, wave_id: int) -> str:
    return f"{asset_type}|{code}|{timeframe}|wave|{wave_id}"


def _build_extreme_nk(asset_type: str, code: str, timeframe: str, wave_id: int, extreme_seq: int) -> str:
    return f"{asset_type}|{code}|{timeframe}|wave|{wave_id}|extreme|{extreme_seq}"


def _build_snapshot_nk(asset_type: str, code: str, timeframe: str, asof_bar_dt: date) -> str:
    return f"{asset_type}|{code}|{timeframe}|snapshot|{asof_bar_dt.isoformat()}"


def _build_stats_nk(universe: str, timeframe: str, major_state: str, metric_name: str, sample_version: str) -> str:
    return f"{universe}|{timeframe}|{major_state}|{metric_name}|{sample_version}"


def _write_summary(summary: dict[str, object], summary_path: Path | None) -> None:
    if summary_path is None:
        return
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
