"""承接 canonical MALF runner 的结构派生、落表与 checkpoint 写回。"""

from __future__ import annotations

import json
from datetime import date

import duckdb
import pandas as pd

from mlq.malf.bootstrap import (
    MALF_CANONICAL_CHECKPOINT_TABLE,
    MALF_CANONICAL_RUN_TABLE,
    MALF_EXTREME_PROGRESS_LEDGER_TABLE,
    MALF_PIVOT_LEDGER_TABLE,
    MALF_SAME_LEVEL_STATS_TABLE,
    MALF_STATE_SNAPSHOT_TABLE,
    MALF_WAVE_LEDGER_TABLE,
)
from mlq.malf.canonical_shared import (
    MalfCanonicalBuildSummary,
    _Pivot,
    _build_extreme_nk,
    _build_pivot_nk,
    _build_snapshot_nk,
    _build_stats_nk,
    _build_wave_nk,
    _normalize_row_for_compare,
    _series_quantile,
    _to_python_date,
)


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
                    switch_mode(
                        "down",
                        trade_date,
                        last_valid_hl.pivot_price if last_valid_hl else None,
                        last_valid_hl.pivot_nk if last_valid_hl else None,
                    )
                    active_wave_high = bar_high
                    active_wave_low = bar_low
            elif mode == "down" and reversal_guard_price is not None:
                if bar_high <= reversal_guard_price:
                    reversal_stage = "hold"
                elif bar_high > reversal_guard_price:
                    switch_mode(
                        "up",
                        trade_date,
                        last_valid_lh.pivot_price if last_valid_lh else None,
                        last_valid_lh.pivot_nk if last_valid_lh else None,
                    )
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
    # 首次建仓时如果一个 scope 还没有形成 completed wave，严格只看 completed
    # 会让 same_level_stats 完全空表。这里允许在“零 completed wave”的边界下，
    # 用当前 active wave 启动最小统计，以保证 canonical stats 在首轮也有正式落表事实。
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
    existing_rows = connection.execute(f"SELECT * FROM {table_name} WHERE {where_clause}", where_params).fetchdf()
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
