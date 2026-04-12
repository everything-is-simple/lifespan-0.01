"""承载 `position bootstrap` 的候选/容量/仓位物化逻辑。"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .position_shared import PositionFormalSignalInput


@dataclass(frozen=True)
class PositionMaterializationCounts:
    """汇总一次 bounded position 物化产生的内部计数。"""

    admitted_count: int
    blocked_count: int
    family_snapshot_count: int


def register_position_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    source_signal_contract_version: str | None,
    source_signal_run_id: str | None,
) -> None:
    """登记一次 `position` materialization run 审计行。"""

    connection.execute(
        """
        INSERT INTO position_run (
            run_id,
            run_status,
            source_signal_contract_version,
            source_signal_run_id,
            run_completed_at,
            notes
        )
        SELECT ?, 'completed', ?, ?, CURRENT_TIMESTAMP, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM position_run
            WHERE run_id = ?
        )
        """,
        [
            run_id,
            source_signal_contract_version,
            source_signal_run_id,
            "minimal bootstrap materialization from alpha formal signal",
            run_id,
        ],
    )


def fetch_policy_contract(
    connection: duckdb.DuckDBPyConnection,
    policy_id: str,
) -> tuple[str, str, str]:
    """读取一次物化所绑定的 policy 契约。"""

    row = connection.execute(
        """
        SELECT policy_family, policy_version, entry_leg_role_default
        FROM position_policy_registry
        WHERE policy_id = ?
        """,
        [policy_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Unknown position policy: {policy_id}")
    return row[0], row[1], row[2]


def materialize_position_rows(
    connection: duckdb.DuckDBPyConnection,
    formal_signals: list[PositionFormalSignalInput] | tuple[PositionFormalSignalInput, ...],
    *,
    policy_id: str,
    policy_family: str,
    policy_version: str,
    entry_leg_role_default: str,
    default_single_name_cap_weight: float,
    default_portfolio_cap_weight: float,
    share_lot_size: int,
) -> PositionMaterializationCounts:
    """把 bounded `alpha formal signal` 样本写成 `position` 账本事实。"""

    admitted_count = 0
    blocked_count = 0
    family_snapshot_count = 0

    for signal in formal_signals:
        candidate_status = _resolve_candidate_status(signal)
        blocked_reason_code = _resolve_blocked_reason_code(signal, candidate_status)
        candidate_nk = _build_candidate_nk(signal, policy_id)
        context_max_position_weight = _context_max_position_weight(signal)
        remaining_single_name_capacity_weight = _resolve_single_name_capacity_weight(
            signal,
            default_single_name_cap_weight=default_single_name_cap_weight,
        )
        remaining_portfolio_capacity_weight = _resolve_portfolio_capacity_weight(
            signal,
            default_portfolio_cap_weight=default_portfolio_cap_weight,
        )
        final_allowed_position_weight = _resolve_final_allowed_position_weight(
            candidate_status=candidate_status,
            context_max_position_weight=context_max_position_weight,
            remaining_single_name_capacity_weight=remaining_single_name_capacity_weight,
            remaining_portfolio_capacity_weight=remaining_portfolio_capacity_weight,
        )
        required_reduction_weight = max(
            signal.current_position_weight - final_allowed_position_weight,
            0.0,
        )
        position_action_decision = _resolve_position_action_decision(
            candidate_status=candidate_status,
            current_position_weight=signal.current_position_weight,
            final_allowed_position_weight=final_allowed_position_weight,
            required_reduction_weight=required_reduction_weight,
        )

        target_weight = 0.0 if position_action_decision == "reject_open" else final_allowed_position_weight
        target_notional = _resolve_target_notional(signal, target_weight=target_weight)
        target_shares_before_lot = _resolve_target_shares_before_lot(
            signal,
            target_notional=target_notional,
        )
        target_shares = _apply_share_lot_floor(
            target_shares_before_lot,
            share_lot_size=share_lot_size,
        )

        _insert_position_candidate_audit(
            connection,
            candidate_nk=candidate_nk,
            signal=signal,
            policy_id=policy_id,
            candidate_status=candidate_status,
            blocked_reason_code=blocked_reason_code,
        )
        _insert_position_capacity_snapshot(
            connection,
            candidate_nk=candidate_nk,
            signal=signal,
            context_max_position_weight=context_max_position_weight,
            remaining_single_name_capacity_weight=remaining_single_name_capacity_weight,
            remaining_portfolio_capacity_weight=remaining_portfolio_capacity_weight,
            final_allowed_position_weight=final_allowed_position_weight,
            required_reduction_weight=required_reduction_weight,
        )
        _insert_position_sizing_snapshot(
            connection,
            candidate_nk=candidate_nk,
            policy_id=policy_id,
            policy_version=policy_version,
            entry_leg_role_default=entry_leg_role_default,
            signal=signal,
            position_action_decision=position_action_decision,
            target_weight=target_weight,
            target_notional=target_notional,
            target_shares=target_shares,
            final_allowed_position_weight=final_allowed_position_weight,
            required_reduction_weight=required_reduction_weight,
        )
        family_snapshot_count += _insert_policy_family_snapshot(
            connection,
            policy_family=policy_family,
            policy_version=policy_version,
            policy_id=policy_id,
            candidate_nk=candidate_nk,
            signal=signal,
            share_lot_size=share_lot_size,
            context_max_position_weight=context_max_position_weight,
            final_allowed_position_weight=final_allowed_position_weight,
            target_shares_before_lot=target_shares_before_lot,
            final_target_shares=target_shares,
        )

        if candidate_status == "admitted":
            admitted_count += 1
        else:
            blocked_count += 1

    return PositionMaterializationCounts(
        admitted_count=admitted_count,
        blocked_count=blocked_count,
        family_snapshot_count=family_snapshot_count,
    )


def _resolve_candidate_status(signal: PositionFormalSignalInput) -> str:
    if signal.trigger_admissible and signal.formal_signal_status == "admitted":
        return "admitted"
    if signal.formal_signal_status in {"blocked", "deferred"}:
        return signal.formal_signal_status
    return "blocked"


def _resolve_blocked_reason_code(signal: PositionFormalSignalInput, candidate_status: str) -> str | None:
    if candidate_status == "admitted":
        return None
    if signal.blocked_reason_code:
        return signal.blocked_reason_code
    if not signal.trigger_admissible:
        return "alpha_not_admitted"
    return f"alpha_status_{candidate_status}"


def _build_candidate_nk(signal: PositionFormalSignalInput, policy_id: str) -> str:
    return "|".join((signal.signal_nk, policy_id, signal.reference_trade_date))


def _build_capacity_snapshot_nk(candidate_nk: str) -> str:
    return f"{candidate_nk}|default"


def _build_sizing_snapshot_nk(
    candidate_nk: str,
    *,
    entry_leg_role_default: str,
    policy_version: str,
) -> str:
    return f"{candidate_nk}|{entry_leg_role_default}|{policy_version}"


def _build_family_snapshot_nk(candidate_nk: str, *, policy_family: str, policy_version: str) -> str:
    return f"{candidate_nk}|{policy_family}|{policy_version}"


def _context_max_position_weight(signal: PositionFormalSignalInput) -> float:
    if signal.lifecycle_rank_total <= 0:
        conservative_rank_ratio = 0.0
    else:
        conservative_rank_ratio = signal.lifecycle_rank_high / signal.lifecycle_rank_total
    context_code = signal.malf_context_4
    if context_code == "BULL_MAINSTREAM":
        return 0.25 * (1 - conservative_rank_ratio)
    if context_code == "BULL_COUNTERTREND":
        return 0.25 * conservative_rank_ratio
    if context_code == "BEAR_COUNTERTREND":
        return 0.25 * (1 - conservative_rank_ratio) * 0.5
    if context_code == "BEAR_MAINSTREAM":
        return 0.0
    return 0.0


def _resolve_single_name_capacity_weight(
    signal: PositionFormalSignalInput,
    *,
    default_single_name_cap_weight: float,
) -> float:
    if signal.remaining_single_name_capacity_weight is not None:
        return max(signal.remaining_single_name_capacity_weight, 0.0)
    return max(default_single_name_cap_weight - signal.current_position_weight, 0.0)


def _resolve_portfolio_capacity_weight(
    signal: PositionFormalSignalInput,
    *,
    default_portfolio_cap_weight: float,
) -> float:
    if signal.remaining_portfolio_capacity_weight is not None:
        return max(signal.remaining_portfolio_capacity_weight, 0.0)
    return max(default_portfolio_cap_weight, 0.0)


def _resolve_final_allowed_position_weight(
    *,
    candidate_status: str,
    context_max_position_weight: float,
    remaining_single_name_capacity_weight: float,
    remaining_portfolio_capacity_weight: float,
) -> float:
    if candidate_status != "admitted":
        return 0.0
    return max(
        min(
            context_max_position_weight,
            remaining_single_name_capacity_weight,
            remaining_portfolio_capacity_weight,
        ),
        0.0,
    )


def _resolve_position_action_decision(
    *,
    candidate_status: str,
    current_position_weight: float,
    final_allowed_position_weight: float,
    required_reduction_weight: float,
) -> str:
    if candidate_status != "admitted" or final_allowed_position_weight <= 0:
        return "reject_open"
    if required_reduction_weight > 0:
        return "trim_to_context_cap"
    if current_position_weight > 0 and abs(current_position_weight - final_allowed_position_weight) < 1e-12:
        return "hold_at_cap"
    return "open_up_to_context_cap"


def _resolve_target_notional(signal: PositionFormalSignalInput, *, target_weight: float) -> float:
    if signal.capital_base_value is None:
        return 0.0
    return max(target_weight, 0.0) * signal.capital_base_value


def _resolve_target_shares_before_lot(
    signal: PositionFormalSignalInput,
    *,
    target_notional: float,
) -> int:
    if signal.reference_price is None or signal.reference_price <= 0:
        return 0
    return int(target_notional / signal.reference_price)


def _apply_share_lot_floor(target_shares_before_lot: int, *, share_lot_size: int) -> int:
    if target_shares_before_lot <= 0:
        return 0
    return (target_shares_before_lot // share_lot_size) * share_lot_size


def _insert_position_candidate_audit(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    signal: PositionFormalSignalInput,
    policy_id: str,
    candidate_status: str,
    blocked_reason_code: str | None,
) -> None:
    connection.execute(
        """
        INSERT INTO position_candidate_audit (
            candidate_nk,
            signal_nk,
            instrument,
            policy_id,
            reference_trade_date,
            candidate_status,
            blocked_reason_code,
            context_code,
            audit_note,
            source_signal_run_id
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM position_candidate_audit
            WHERE candidate_nk = ?
        )
        """,
        [
            candidate_nk,
            signal.signal_nk,
            signal.instrument,
            policy_id,
            signal.reference_trade_date,
            candidate_status,
            blocked_reason_code,
            signal.malf_context_4,
            signal.audit_note,
            signal.source_signal_run_id,
            candidate_nk,
        ],
    )


def _insert_position_capacity_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    signal: PositionFormalSignalInput,
    context_max_position_weight: float,
    remaining_single_name_capacity_weight: float,
    remaining_portfolio_capacity_weight: float,
    final_allowed_position_weight: float,
    required_reduction_weight: float,
) -> None:
    capacity_snapshot_nk = _build_capacity_snapshot_nk(candidate_nk)
    capacity_source_code = (
        "formal_position_capacity"
        if signal.remaining_single_name_capacity_weight is not None
        or signal.remaining_portfolio_capacity_weight is not None
        else "bootstrap_default_capacity"
    )
    connection.execute(
        """
        INSERT INTO position_capacity_snapshot (
            capacity_snapshot_nk,
            candidate_nk,
            capacity_snapshot_role,
            current_position_weight,
            context_max_position_weight,
            remaining_single_name_capacity_weight,
            remaining_portfolio_capacity_weight,
            final_allowed_position_weight,
            required_reduction_weight,
            capacity_source_code
        )
        SELECT ?, ?, 'default', ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM position_capacity_snapshot
            WHERE capacity_snapshot_nk = ?
        )
        """,
        [
            capacity_snapshot_nk,
            candidate_nk,
            signal.current_position_weight,
            context_max_position_weight,
            remaining_single_name_capacity_weight,
            remaining_portfolio_capacity_weight,
            final_allowed_position_weight,
            required_reduction_weight,
            capacity_source_code,
            capacity_snapshot_nk,
        ],
    )


def _insert_position_sizing_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    candidate_nk: str,
    policy_id: str,
    policy_version: str,
    entry_leg_role_default: str,
    signal: PositionFormalSignalInput,
    position_action_decision: str,
    target_weight: float,
    target_notional: float,
    target_shares: int,
    final_allowed_position_weight: float,
    required_reduction_weight: float,
) -> None:
    sizing_snapshot_nk = _build_sizing_snapshot_nk(
        candidate_nk,
        entry_leg_role_default=entry_leg_role_default,
        policy_version=policy_version,
    )
    connection.execute(
        """
        INSERT INTO position_sizing_snapshot (
            sizing_snapshot_nk,
            candidate_nk,
            policy_id,
            entry_leg_role,
            position_action_decision,
            target_weight,
            target_notional,
            target_shares,
            final_allowed_position_weight,
            required_reduction_weight,
            reference_price,
            reference_trade_date
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM position_sizing_snapshot
            WHERE sizing_snapshot_nk = ?
        )
        """,
        [
            sizing_snapshot_nk,
            candidate_nk,
            policy_id,
            entry_leg_role_default,
            position_action_decision,
            target_weight,
            target_notional,
            target_shares,
            final_allowed_position_weight,
            required_reduction_weight,
            signal.reference_price,
            signal.reference_trade_date,
            sizing_snapshot_nk,
        ],
    )


def _insert_policy_family_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    policy_family: str,
    policy_version: str,
    policy_id: str,
    candidate_nk: str,
    signal: PositionFormalSignalInput,
    share_lot_size: int,
    context_max_position_weight: float,
    final_allowed_position_weight: float,
    target_shares_before_lot: int,
    final_target_shares: int,
) -> int:
    family_snapshot_nk = _build_family_snapshot_nk(
        candidate_nk,
        policy_family=policy_family,
        policy_version=policy_version,
    )
    if policy_family == "FIXED_NOTIONAL_CONTROL":
        target_notional_before_cap = _resolve_target_notional(
            signal,
            target_weight=context_max_position_weight,
        )
        target_shares_before_cap = _resolve_target_shares_before_lot(
            signal,
            target_notional=target_notional_before_cap,
        )
        connection.execute(
            """
            INSERT INTO position_funding_fixed_notional_snapshot (
                family_snapshot_nk,
                candidate_nk,
                policy_id,
                target_notional_before_cap,
                target_shares_before_cap,
                cap_trim_applied,
                final_target_shares
            )
            SELECT ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM position_funding_fixed_notional_snapshot
                WHERE family_snapshot_nk = ?
            )
            """,
            [
                family_snapshot_nk,
                candidate_nk,
                policy_id,
                target_notional_before_cap,
                target_shares_before_cap,
                final_allowed_position_weight < context_max_position_weight,
                final_target_shares,
                family_snapshot_nk,
            ],
        )
        return 1
    if policy_family == "SINGLE_LOT_CONTROL":
        lot_floor_applied = target_shares_before_lot != final_target_shares
        fallback_reason_code = None
        if final_allowed_position_weight > 0 and final_target_shares == 0:
            fallback_reason_code = "insufficient_notional_for_single_lot"
        connection.execute(
            """
            INSERT INTO position_funding_single_lot_snapshot (
                family_snapshot_nk,
                candidate_nk,
                policy_id,
                min_lot_size,
                lot_floor_applied,
                final_target_shares,
                fallback_reason_code
            )
            SELECT ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM position_funding_single_lot_snapshot
                WHERE family_snapshot_nk = ?
            )
            """,
            [
                family_snapshot_nk,
                candidate_nk,
                policy_id,
                share_lot_size,
                lot_floor_applied,
                final_target_shares,
                fallback_reason_code,
                family_snapshot_nk,
            ],
        )
        return 1
    return 0
