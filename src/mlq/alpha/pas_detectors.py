"""alpha PAS 五触发 detector 逻辑。"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from mlq.alpha.pas_shared import (
    DEFAULT_ALPHA_PAS_FAMILY_CODE_BY_TRIGGER,
    PAS_MAX_HISTORY_BARS,
    _DetectorScopeRow,
)


EPSILON = 1e-9
PAS_PATTERN_REQUIRED_HISTORY_DAYS = {
    "BOF": 21,
    "BPB": 26,
    "PB": 41,
    "TST": 61,
    "CPB": 41,
}


def evaluate_pas_triggers(
    *,
    scope_row: _DetectorScopeRow,
    history: pd.DataFrame,
) -> list[dict[str, object]]:
    """对单个 structure/filter scope 评估 PAS 五触发。"""

    evaluations = [
        _evaluate_bof(code=scope_row.instrument, signal_date=scope_row.signal_date, history=history),
        _evaluate_tst(code=scope_row.instrument, signal_date=scope_row.signal_date, history=history),
        _evaluate_pb(code=scope_row.instrument, signal_date=scope_row.signal_date, history=history),
        _evaluate_cpb(code=scope_row.instrument, signal_date=scope_row.signal_date, history=history),
        _evaluate_bpb(code=scope_row.instrument, signal_date=scope_row.signal_date, history=history),
    ]
    for item in evaluations:
        trigger_type = str(item["trigger_type"])
        item["family_code"] = DEFAULT_ALPHA_PAS_FAMILY_CODE_BY_TRIGGER[trigger_type]
    return evaluations


def _clip(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return float(np.clip(value, lower, upper))


def _sort_history(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values("date").reset_index(drop=True).tail(PAS_MAX_HISTORY_BARS).reset_index(drop=True)


def _required_history_days(pattern: str) -> int:
    normalized = pattern.strip().upper()
    if normalized not in PAS_PATTERN_REQUIRED_HISTORY_DAYS:
        raise ValueError(f"Unsupported PAS pattern: {pattern}")
    return PAS_PATTERN_REQUIRED_HISTORY_DAYS[normalized]


def _base_trace(
    *,
    code: str,
    signal_date: date,
    pattern: str,
    history_days: int,
    min_history_days: int,
) -> dict[str, object]:
    normalized = pattern.strip().upper()
    return {
        "code": code,
        "signal_date": signal_date.isoformat(),
        "pattern": normalized,
        "trigger_type": normalized.lower(),
        "pattern_code": normalized,
        "triggered": False,
        "skip_reason": None,
        "detect_reason": None,
        "history_days": int(history_days),
        "min_history_days": int(min_history_days),
        "strength": 0.0,
    }


def _insufficient_history_trace(
    *,
    code: str,
    signal_date: date,
    pattern: str,
    history_days: int,
    min_history_days: int,
) -> dict[str, object]:
    trace = _base_trace(
        code=code,
        signal_date=signal_date,
        pattern=pattern,
        history_days=history_days,
        min_history_days=min_history_days,
    )
    trace["skip_reason"] = "INSUFFICIENT_HISTORY"
    trace["detect_reason"] = "INSUFFICIENT_HISTORY"
    return trace


def _missing_required_columns(df: pd.DataFrame) -> set[str]:
    return {"date", "adj_open", "adj_high", "adj_low", "adj_close", "volume", "volume_ma20"}.difference(df.columns)


def _evaluate_bof(*, code: str, signal_date: date, history: pd.DataFrame) -> dict[str, object]:
    pattern = "BOF"
    min_history_days = _required_history_days(pattern)
    trace = _base_trace(
        code=code,
        signal_date=signal_date,
        pattern=pattern,
        history_days=len(history),
        min_history_days=min_history_days,
    )
    if history.empty or len(history) < min_history_days:
        return _insufficient_history_trace(
            code=code,
            signal_date=signal_date,
            pattern=pattern,
            history_days=len(history),
            min_history_days=min_history_days,
        )
    data = _sort_history(history)
    missing_columns = _missing_required_columns(data)
    if missing_columns:
        trace["skip_reason"] = "MISSING_REQUIRED_COLUMNS"
        trace["detect_reason"] = "MISSING_REQUIRED_COLUMNS"
        return trace
    lookback = data.iloc[-21:-1]
    today = data.iloc[-1]
    lower_bound = float(lookback["adj_low"].min())
    today_low = float(today["adj_low"])
    today_close = float(today["adj_close"])
    today_open = float(today["adj_open"])
    today_high = float(today["adj_high"])
    today_volume = float(today["volume"] or 0.0)
    volume_ma20 = float(today["volume_ma20"] or 0.0)
    if today_high <= today_low:
        trace["skip_reason"] = "INVALID_RANGE"
        trace["detect_reason"] = "INVALID_RANGE"
        return trace
    cond_break = today_low < lower_bound * (1 - 0.01)
    cond_recover = today_close >= lower_bound
    close_pos = (today_close - today_low) / max(today_high - today_low, EPSILON)
    body_ratio = abs(today_close - today_open) / max(today_high - today_low, EPSILON)
    volume_ratio = today_volume / volume_ma20 if volume_ma20 > 0 else 0.0
    cond_close_pos = close_pos >= 0.6
    cond_volume = volume_ma20 > 0 and today_volume >= volume_ma20 * 1.2
    trace.update(
        {
            "lower_bound": lower_bound,
            "today_low": today_low,
            "today_close": today_close,
            "today_open": today_open,
            "today_high": today_high,
            "volume_ratio": float(volume_ratio),
            "close_pos": float(close_pos),
            "body_ratio": float(body_ratio),
        }
    )
    if not cond_break:
        trace["skip_reason"] = "NO_BREAK"
        trace["detect_reason"] = "NO_BREAK"
        return trace
    if not cond_recover:
        trace["skip_reason"] = "NO_RECOVERY"
        trace["detect_reason"] = "NO_RECOVERY"
        return trace
    if not cond_close_pos:
        trace["skip_reason"] = "LOW_CLOSE_POSITION"
        trace["detect_reason"] = "LOW_CLOSE_POSITION"
        return trace
    if not cond_volume:
        trace["skip_reason"] = "LOW_VOLUME"
        trace["detect_reason"] = "LOW_VOLUME"
        return trace
    trace["triggered"] = True
    trace["detect_reason"] = "TRIGGERED"
    trace["strength"] = _clip(0.4 * close_pos + 0.3 * min(volume_ratio / 2.0, 1.0) + 0.3 * body_ratio)
    return trace


def _evaluate_bpb(*, code: str, signal_date: date, history: pd.DataFrame) -> dict[str, object]:
    pattern = "BPB"
    min_history_days = _required_history_days(pattern)
    trace = _base_trace(
        code=code,
        signal_date=signal_date,
        pattern=pattern,
        history_days=len(history),
        min_history_days=min_history_days,
    )
    if history.empty or len(history) < min_history_days:
        return _insufficient_history_trace(
            code=code,
            signal_date=signal_date,
            pattern=pattern,
            history_days=len(history),
            min_history_days=min_history_days,
        )
    data = _sort_history(history)
    setup_window = data.iloc[-26:-6]
    pullback_window = data.iloc[-6:-1]
    today = data.iloc[-1]
    if setup_window.empty or pullback_window.empty:
        return _insufficient_history_trace(
            code=code,
            signal_date=signal_date,
            pattern=pattern,
            history_days=len(history),
            min_history_days=min_history_days,
        )
    today_low = float(today["adj_low"])
    today_close = float(today["adj_close"])
    today_open = float(today["adj_open"])
    today_high = float(today["adj_high"])
    today_volume = float(today["volume"] or 0.0)
    volume_ma20 = float(today["volume_ma20"] or 0.0)
    if today_high <= today_low:
        trace["skip_reason"] = "INVALID_RANGE"
        trace["detect_reason"] = "INVALID_RANGE"
        return trace
    breakout_ref = float(setup_window["adj_high"].max())
    breakout_peak = float(pullback_window["adj_high"].max())
    pullback_low = float(pullback_window["adj_low"].min())
    breakout_mask = (
        (pullback_window["adj_close"] > breakout_ref)
        & (pullback_window["volume"] / pullback_window["volume_ma20"].replace(0, pd.NA) >= 1.2)
    ).fillna(False)
    pullback_depth = (breakout_peak - pullback_low) / max(breakout_peak - breakout_ref, EPSILON)
    pullback_high = float(pullback_window["adj_high"].max())
    volume_ratio = today_volume / volume_ma20 if volume_ma20 > 0 else 0.0
    confirmation = (
        today_close > pullback_high
        and today_close >= breakout_ref
        and volume_ma20 > 0
        and today_volume >= volume_ma20 * 1.2
    )
    not_overextended = today_close <= breakout_peak * 1.03
    support_hold = pullback_low >= breakout_ref * (1 - 0.03)
    pullback_depth_valid = 0.25 <= pullback_depth <= 0.80
    if not bool(breakout_mask.any()):
        trace["skip_reason"] = "NO_BREAKOUT_LEG"
        trace["detect_reason"] = "NO_BREAKOUT_LEG"
        return trace
    if not support_hold:
        trace["skip_reason"] = "SUPPORT_LOST"
        trace["detect_reason"] = "SUPPORT_LOST"
        return trace
    if not pullback_depth_valid:
        trace["skip_reason"] = "PULLBACK_NOT_VALID"
        trace["detect_reason"] = "PULLBACK_NOT_VALID"
        return trace
    if not confirmation:
        trace["skip_reason"] = "NO_CONFIRMATION"
        trace["detect_reason"] = "NO_CONFIRMATION"
        return trace
    if not not_overextended:
        trace["skip_reason"] = "OVEREXTENDED_CONFIRM"
        trace["detect_reason"] = "OVEREXTENDED_CONFIRM"
        return trace
    body_ratio = abs(today_close - today_open) / max(today_high - today_low, EPSILON)
    confirm_strength = _clip((today_close - breakout_ref) / max(0.10 * breakout_ref, EPSILON))
    volume_strength = _clip(volume_ratio / 2.0)
    depth_quality = 1.0 if 0.40 <= pullback_depth <= 0.60 else 0.7
    trace["triggered"] = True
    trace["detect_reason"] = "TRIGGERED"
    trace["strength"] = _clip(0.40 * confirm_strength + 0.25 * volume_strength + 0.20 * depth_quality + 0.15 * body_ratio)
    trace["pullback_depth"] = float(pullback_depth)
    return trace


def _evaluate_pb(*, code: str, signal_date: date, history: pd.DataFrame) -> dict[str, object]:
    pattern = "PB"
    min_history_days = _required_history_days(pattern)
    trace = _base_trace(
        code=code,
        signal_date=signal_date,
        pattern=pattern,
        history_days=len(history),
        min_history_days=min_history_days,
    )
    if history.empty or len(history) < min_history_days:
        return _insufficient_history_trace(
            code=code,
            signal_date=signal_date,
            pattern=pattern,
            history_days=len(history),
            min_history_days=min_history_days,
        )
    data = _sort_history(history)
    trend_window_a = data.iloc[-41:-21]
    trend_window_b = data.iloc[-21:-6]
    pullback_window = data.iloc[-6:-1]
    today = data.iloc[-1]
    if trend_window_a.empty or trend_window_b.empty or pullback_window.empty:
        return _insufficient_history_trace(
            code=code,
            signal_date=signal_date,
            pattern=pattern,
            history_days=len(history),
            min_history_days=min_history_days,
        )
    today_high = float(today["adj_high"])
    today_low = float(today["adj_low"])
    today_close = float(today["adj_close"])
    today_volume = float(today["volume"] or 0.0)
    volume_ma20 = float(today["volume_ma20"] or 0.0)
    if today_high <= today_low:
        trace["skip_reason"] = "INVALID_RANGE"
        trace["detect_reason"] = "INVALID_RANGE"
        return trace
    trend_peak = float(trend_window_b["adj_high"].max())
    trend_floor = float(trend_window_a["adj_low"].min())
    mid_floor = float(trend_window_b["adj_low"].min())
    pullback_low = float(pullback_window["adj_low"].min())
    rebound_ref = float(pullback_window["adj_high"].max())
    trend_established = float(trend_window_b["adj_high"].max()) > float(trend_window_a["adj_high"].max()) and mid_floor > trend_floor
    pullback_depth = (trend_peak - pullback_low) / max(trend_peak - trend_floor, EPSILON)
    support_hold = pullback_low >= mid_floor * 0.98
    rebound_confirm = today_close > rebound_ref and today_close <= trend_peak * 1.03
    volume_ratio = today_volume / volume_ma20 if volume_ma20 > 0 else 0.0
    volume_confirm = volume_ma20 > 0 and today_volume >= volume_ma20 * 1.1
    if not trend_established:
        trace["skip_reason"] = "TREND_NOT_ESTABLISHED"
        trace["detect_reason"] = "TREND_NOT_ESTABLISHED"
        return trace
    if not (0.20 <= pullback_depth <= 0.50):
        trace["skip_reason"] = "PULLBACK_NOT_VALID"
        trace["detect_reason"] = "PULLBACK_NOT_VALID"
        return trace
    if not support_hold:
        trace["skip_reason"] = "SUPPORT_LOST"
        trace["detect_reason"] = "SUPPORT_LOST"
        return trace
    if not rebound_confirm:
        trace["skip_reason"] = "NO_REBOUND_CONFIRM"
        trace["detect_reason"] = "NO_REBOUND_CONFIRM"
        return trace
    if not volume_confirm:
        trace["skip_reason"] = "LOW_VOLUME"
        trace["detect_reason"] = "LOW_VOLUME"
        return trace
    rebound_strength = _clip((today_close - rebound_ref) / max(0.08 * rebound_ref, EPSILON))
    depth_quality = 1.0 if 0.25 <= pullback_depth <= 0.40 else 0.7
    trend_quality = _clip((mid_floor - trend_floor) / max(0.10 * trend_floor, EPSILON))
    volume_strength = _clip(volume_ratio / 2.0)
    trace["triggered"] = True
    trace["detect_reason"] = "TRIGGERED"
    trace["strength"] = _clip(0.35 * rebound_strength + 0.25 * depth_quality + 0.20 * trend_quality + 0.20 * volume_strength)
    trace["pullback_depth"] = float(pullback_depth)
    return trace


def _find_local_pivot_lows(window: pd.DataFrame) -> list[tuple[int, float]]:
    lows = [float(value) for value in window["adj_low"].tolist()]
    pivots: list[tuple[int, float]] = []
    for idx in range(1, len(lows) - 1):
        if lows[idx] <= lows[idx - 1] and lows[idx] < lows[idx + 1]:
            pivots.append((idx, lows[idx]))
    return pivots


def _evaluate_cpb(*, code: str, signal_date: date, history: pd.DataFrame) -> dict[str, object]:
    pattern = "CPB"
    min_history_days = _required_history_days(pattern)
    trace = _base_trace(
        code=code,
        signal_date=signal_date,
        pattern=pattern,
        history_days=len(history),
        min_history_days=min_history_days,
    )
    if history.empty or len(history) < min_history_days:
        return _insufficient_history_trace(
            code=code,
            signal_date=signal_date,
            pattern=pattern,
            history_days=len(history),
            min_history_days=min_history_days,
        )
    data = _sort_history(history)
    analysis_window = data.iloc[-41:-1].reset_index(drop=True)
    today = data.iloc[-1]
    if analysis_window.empty:
        return _insufficient_history_trace(
            code=code,
            signal_date=signal_date,
            pattern=pattern,
            history_days=len(history),
            min_history_days=min_history_days,
        )
    today_low = float(today["adj_low"])
    today_close = float(today["adj_close"])
    today_open = float(today["adj_open"])
    today_high = float(today["adj_high"])
    today_volume = float(today["volume"] or 0.0)
    volume_ma20 = float(today["volume_ma20"] or 0.0)
    if today_high <= today_low:
        trace["skip_reason"] = "INVALID_RANGE"
        trace["detect_reason"] = "INVALID_RANGE"
        return trace
    trend_peak_pos = int(analysis_window["adj_high"].idxmax())
    if trend_peak_pos < 19 or trend_peak_pos > len(analysis_window) - 9:
        trace["skip_reason"] = "TREND_PEAK_POSITION_INVALID"
        trace["detect_reason"] = "TREND_PEAK_POSITION_INVALID"
        return trace
    trend_window_a = analysis_window.iloc[trend_peak_pos - 19 : trend_peak_pos - 9].reset_index(drop=True)
    trend_window_b = analysis_window.iloc[trend_peak_pos - 9 : trend_peak_pos + 1].reset_index(drop=True)
    pullback_window = analysis_window.iloc[trend_peak_pos + 1 :].reset_index(drop=True)
    if trend_window_a.empty or trend_window_b.empty or len(pullback_window) < 8:
        trace["skip_reason"] = "PULLBACK_TOO_SHORT"
        trace["detect_reason"] = "PULLBACK_TOO_SHORT"
        return trace
    trend_floor = float(trend_window_a["adj_low"].min())
    trend_ref_high = float(trend_window_a["adj_high"].max())
    trend_peak = float(trend_window_b["adj_high"].max())
    mid_floor = float(trend_window_b["adj_low"].min())
    pullback_low = float(pullback_window["adj_low"].min())
    pullback_low_pos = int(pullback_window["adj_low"].idxmin())
    pullback_day_count = int(len(pullback_window))
    pullback_high = float(pullback_window["adj_high"].max())
    pullback_depth = (trend_peak - pullback_low) / max(trend_peak - trend_floor, EPSILON)
    trend_established = trend_peak > trend_ref_high * 1.04 and mid_floor > trend_floor * 1.03
    pivot_lows = _find_local_pivot_lows(pullback_window)
    pullback_leg_count = int(max(len(pivot_lows), 1))
    prior_internal_pivot_low = None
    for pivot_pos, pivot_low in pivot_lows:
        if pivot_pos < pullback_low_pos:
            prior_internal_pivot_low = pivot_low
    final_leg_break_prior_pivot = bool(
        prior_internal_pivot_low is not None and pullback_low < float(prior_internal_pivot_low) * (1 - 0.002)
    )
    rebound_window = pullback_window.iloc[pullback_low_pos + 1 :]
    reclaim_reference = (
        float(rebound_window["adj_high"].max())
        if not rebound_window.empty
        else float(prior_internal_pivot_low or pullback_window["adj_high"].tail(3).max())
    )
    compression_width = (pullback_high - pullback_low) / max(pullback_low, EPSILON)
    volume_ratio = today_volume / volume_ma20 if volume_ma20 > 0 else 0.0
    continuation_confirmed = (
        today_close > reclaim_reference
        and today_close >= today_open
        and today_close <= trend_peak * 1.03
        and volume_ma20 > 0
        and today_volume >= volume_ma20 * 1.1
    )
    excluded_as_compression_breakout = bool(
        pullback_leg_count < 3 and pullback_depth < 0.18 and compression_width <= 0.12
    )
    tail_after_low = pullback_window.iloc[pullback_low_pos + 1 :]
    long_duration_reset = bool(
        pullback_day_count >= 8
        and not final_leg_break_prior_pivot
        and len(tail_after_low) >= 2
        and float(tail_after_low["adj_low"].min()) >= pullback_low * 1.002
        and float(tail_after_low["adj_close"].iloc[-1]) >= float(tail_after_low["adj_close"].iloc[0])
    )
    multi_leg_trap = bool(
        pullback_leg_count >= 3
        and final_leg_break_prior_pivot
        and prior_internal_pivot_low is not None
        and today_close >= float(prior_internal_pivot_low)
    )
    complexity_subtype = "MULTI_LEG_TRAP" if multi_leg_trap else "LONG_DURATION_RESET" if long_duration_reset else None
    if not trend_established:
        trace["skip_reason"] = "TREND_NOT_ESTABLISHED"
        trace["detect_reason"] = "TREND_NOT_ESTABLISHED"
        return trace
    if pullback_depth < 0.18:
        trace["skip_reason"] = "EXCLUDED_AS_COMPRESSION_BREAKOUT" if excluded_as_compression_breakout else "PULLBACK_TOO_SHALLOW"
        trace["detect_reason"] = trace["skip_reason"]
        return trace
    if pullback_depth > 0.75:
        trace["skip_reason"] = "PULLBACK_TOO_DEEP"
        trace["detect_reason"] = "PULLBACK_TOO_DEEP"
        return trace
    if pullback_leg_count < 3 and pullback_day_count < 8:
        trace["skip_reason"] = "SIMPLE_PULLBACK"
        trace["detect_reason"] = "SIMPLE_PULLBACK"
        return trace
    if complexity_subtype is None:
        trace["skip_reason"] = "NO_COMPLEXITY_SUBTYPE"
        trace["detect_reason"] = "NO_COMPLEXITY_SUBTYPE"
        return trace
    if excluded_as_compression_breakout:
        trace["skip_reason"] = "EXCLUDED_AS_COMPRESSION_BREAKOUT"
        trace["detect_reason"] = "EXCLUDED_AS_COMPRESSION_BREAKOUT"
        return trace
    if not continuation_confirmed:
        trace["skip_reason"] = "NO_CONTINUATION_CONFIRM"
        trace["detect_reason"] = "NO_CONTINUATION_CONFIRM"
        return trace
    continuation_strength = _clip((today_close - reclaim_reference) / max(0.06 * reclaim_reference, EPSILON))
    complexity_score = max(_clip((pullback_leg_count - 2) / 2.0), _clip((pullback_day_count - 7) / 5.0))
    depth_quality = 1.0 if 0.25 <= pullback_depth <= 0.45 else 0.7 if 0.18 <= pullback_depth <= 0.60 else 0.4
    trend_quality = _clip((trend_peak - trend_ref_high) / max(0.12 * trend_ref_high, EPSILON))
    volume_strength = _clip(volume_ratio / 2.0)
    trace["triggered"] = True
    trace["detect_reason"] = "TRIGGERED"
    trace["strength"] = _clip(
        0.35 * continuation_strength
        + 0.25 * complexity_score
        + 0.20 * depth_quality
        + 0.10 * trend_quality
        + 0.10 * volume_strength
    )
    trace["complexity_subtype"] = complexity_subtype
    return trace


def _evaluate_tst(*, code: str, signal_date: date, history: pd.DataFrame) -> dict[str, object]:
    pattern = "TST"
    min_history_days = _required_history_days(pattern)
    trace = _base_trace(
        code=code,
        signal_date=signal_date,
        pattern=pattern,
        history_days=len(history),
        min_history_days=min_history_days,
    )
    if history.empty or len(history) < min_history_days:
        return _insufficient_history_trace(
            code=code,
            signal_date=signal_date,
            pattern=pattern,
            history_days=len(history),
            min_history_days=min_history_days,
        )
    data = _sort_history(history)
    structure_window = data.iloc[-61:-6]
    test_window = data.iloc[-6:-1]
    today = data.iloc[-1]
    if structure_window.empty or test_window.empty:
        return _insufficient_history_trace(
            code=code,
            signal_date=signal_date,
            pattern=pattern,
            history_days=len(history),
            min_history_days=min_history_days,
        )
    today_low = float(today["adj_low"])
    today_close = float(today["adj_close"])
    today_open = float(today["adj_open"])
    today_high = float(today["adj_high"])
    today_volume = float(today["volume"] or 0.0)
    volume_ma20 = float(today["volume_ma20"] or 0.0)
    if today_high <= today_low:
        trace["skip_reason"] = "INVALID_RANGE"
        trace["detect_reason"] = "INVALID_RANGE"
        return trace
    support_level = float(structure_window["adj_low"].min())
    test_low = float(test_window["adj_low"].min())
    test_high_ref = float(test_window["adj_high"].max())
    test_distance = abs(test_low - support_level) / max(support_level, EPSILON)
    lower_shadow_ratio = (min(today_open, today_close) - today_low) / max(today_high - today_low, EPSILON)
    volume_ratio = today_volume / volume_ma20 if volume_ma20 > 0 else 0.0
    near_support = test_distance <= 0.03
    support_hold = today_close >= support_level
    bounce_confirm = today_close > test_high_ref or (today_close > today_open and today_close > support_level * 1.01)
    rejection_candle = lower_shadow_ratio >= 0.35
    volume_confirm = volume_ma20 > 0 and today_volume >= volume_ma20 * 1.0
    if not near_support:
        trace["skip_reason"] = "SUPPORT_TOO_FAR"
        trace["detect_reason"] = "SUPPORT_TOO_FAR"
        return trace
    if not support_hold:
        trace["skip_reason"] = "SUPPORT_LOST"
        trace["detect_reason"] = "SUPPORT_LOST"
        return trace
    if not bounce_confirm:
        trace["skip_reason"] = "NO_BOUNCE_CONFIRM"
        trace["detect_reason"] = "NO_BOUNCE_CONFIRM"
        return trace
    if not rejection_candle:
        trace["skip_reason"] = "NO_REJECTION_CANDLE"
        trace["detect_reason"] = "NO_REJECTION_CANDLE"
        return trace
    if not volume_confirm:
        trace["skip_reason"] = "LOW_VOLUME"
        trace["detect_reason"] = "LOW_VOLUME"
        return trace
    support_closeness = 1.0 - _clip(test_distance / max(0.03, EPSILON))
    bounce_strength = _clip((today_close - support_level) / max(0.05 * support_level, EPSILON))
    rejection_strength = _clip(lower_shadow_ratio)
    volume_strength = _clip(volume_ratio / 1.5)
    trace["triggered"] = True
    trace["detect_reason"] = "TRIGGERED"
    trace["strength"] = _clip(0.35 * support_closeness + 0.30 * bounce_strength + 0.20 * rejection_strength + 0.15 * volume_strength)
    return trace
