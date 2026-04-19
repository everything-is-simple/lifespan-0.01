"""执行 `position` 对官方 `alpha formal signal` 的正式 data-grade 物化。"""

from __future__ import annotations

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.position.bootstrap import (
    bootstrap_position_ledger,
    connect_position_ledger,
    position_ledger_path,
)
from mlq.position.position_materialization import (
    fetch_policy_contract,
    materialize_position_rows,
)
from mlq.position.position_runner_shared import (
    DEFAULT_ALPHA_FORMAL_SIGNAL_TABLE,
    DEFAULT_MARKET_BASE_ADJUST_METHOD,
    DEFAULT_MARKET_BASE_PRICE_TABLE,
    PositionFormalSignalRunnerSummary,
    build_position_run_id,
    coerce_date,
    resolve_execution_mode,
    write_summary,
)
from mlq.position.position_runner_audit import (
    claim_position_queue_rows,
    delete_position_candidate_scope,
    enqueue_position_dirty_candidates,
    insert_run_row,
    insert_run_snapshot_row,
    mark_position_queue_completed,
    mark_position_queue_failed,
    mark_run_completed,
    mark_run_failed,
    upsert_position_checkpoint,
)
from mlq.position.position_runner_support import (
    build_candidate_inputs,
    enrich_reference_prices,
    load_alpha_formal_signal_rows,
    load_candidate_scope_stats,
    resolve_materialization_action,
)
from mlq.position.position_shared import (
    PositionFormalSignalInput,
    resolve_signal_contract_version,
    resolve_signal_run_id,
)


def run_position_formal_signal_materialization(
    *,
    policy_id: str,
    capital_base_value: float,
    settings: WorkspaceRoots | None = None,
    alpha_path=None,
    market_base_path=None,
    signal_start_date=None,
    signal_end_date=None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    run_id: str | None = None,
    alpha_formal_signal_table: str = DEFAULT_ALPHA_FORMAL_SIGNAL_TABLE,
    market_price_table: str = DEFAULT_MARKET_BASE_PRICE_TABLE,
    adjust_method: str = DEFAULT_MARKET_BASE_ADJUST_METHOD,
    allow_same_day_price_fallback: bool = False,
    summary_path=None,
    use_checkpoint_queue: bool | None = None,
) -> PositionFormalSignalRunnerSummary:
    """从官方 `alpha formal signal` 读取样本并落入正式 `position` 历史账本。"""

    workspace = settings or default_settings()
    normalized_instruments = tuple(sorted({instrument for instrument in instruments or () if instrument}))
    normalized_limit = max(int(limit), 1)
    normalized_start_date = coerce_date(signal_start_date)
    normalized_end_date = coerce_date(signal_end_date)
    execution_mode = resolve_execution_mode(
        use_checkpoint_queue=use_checkpoint_queue,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
    )

    resolved_alpha_path = alpha_path or workspace.databases.alpha
    resolved_market_base_path = market_base_path or workspace.databases.market_base
    resolved_position_path = position_ledger_path(workspace)
    materialization_run_id = run_id or build_position_run_id()

    alpha_rows = load_alpha_formal_signal_rows(
        alpha_path=resolved_alpha_path,
        alpha_formal_signal_table=alpha_formal_signal_table,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
        instruments=normalized_instruments,
        limit=normalized_limit,
    )
    enriched_signals, missing_reference_price_count = enrich_reference_prices(
        alpha_rows=alpha_rows,
        market_base_path=resolved_market_base_path,
        market_price_table=market_price_table,
        adjust_method=adjust_method,
        capital_base_value=capital_base_value,
        allow_same_day_price_fallback=allow_same_day_price_fallback,
    )

    if execution_mode == "checkpoint_queue":
        return _run_position_checkpoint_queue_build(
            workspace=workspace,
            policy_id=policy_id,
            run_id=materialization_run_id,
            enriched_signals=enriched_signals,
            missing_reference_price_count=missing_reference_price_count,
            alpha_rows=alpha_rows,
            resolved_alpha_path=str(resolved_alpha_path),
            resolved_market_base_path=str(resolved_market_base_path),
            resolved_position_path=str(resolved_position_path),
            alpha_formal_signal_table=alpha_formal_signal_table,
            market_price_table=market_price_table,
            adjust_method=adjust_method,
            limit=normalized_limit,
            summary_path=summary_path,
        )
    return _run_position_bounded_build(
        workspace=workspace,
        policy_id=policy_id,
        run_id=materialization_run_id,
        enriched_signals=enriched_signals,
        missing_reference_price_count=missing_reference_price_count,
        alpha_rows=alpha_rows,
        resolved_alpha_path=str(resolved_alpha_path),
        resolved_market_base_path=str(resolved_market_base_path),
        resolved_position_path=str(resolved_position_path),
        alpha_formal_signal_table=alpha_formal_signal_table,
        market_price_table=market_price_table,
        adjust_method=adjust_method,
        summary_path=summary_path,
    )


def _run_position_bounded_build(
    *,
    workspace: WorkspaceRoots,
    policy_id: str,
    run_id: str,
    enriched_signals: list[PositionFormalSignalInput],
    missing_reference_price_count: int,
    alpha_rows: list[dict[str, object]],
    resolved_alpha_path: str,
    resolved_market_base_path: str,
    resolved_position_path: str,
    alpha_formal_signal_table: str,
    market_price_table: str,
    adjust_method: str,
    summary_path,
) -> PositionFormalSignalRunnerSummary:
    workspace.ensure_directories()
    position_connection = connect_position_ledger(workspace)
    try:
        bootstrap_position_ledger(workspace, connection=position_connection)
        insert_run_row(
            position_connection,
            run_id=run_id,
            policy_id=policy_id,
            execution_mode="bounded",
            bounded_signal_count=len(enriched_signals),
            source_signal_contract_version=resolve_signal_contract_version(enriched_signals),
            source_signal_run_id=resolve_signal_run_id(enriched_signals),
        )
        counts = _materialize_candidate_scope(
            connection=position_connection,
            policy_id=policy_id,
            run_id=run_id,
            candidates=build_candidate_inputs(position_connection, enriched_signals, policy_id=policy_id),
            queue_rows=None,
        )
        summary = PositionFormalSignalRunnerSummary(
            policy_id=policy_id,
            execution_mode="bounded",
            position_run_id=run_id,
            alpha_signal_count=len(alpha_rows),
            enriched_signal_count=len(enriched_signals),
            missing_reference_price_count=missing_reference_price_count,
            candidate_count=counts["candidate_count"],
            admitted_count=counts["admitted_count"],
            blocked_count=counts["blocked_count"],
            risk_budget_count=counts["risk_budget_count"],
            sizing_count=counts["sizing_count"],
            family_snapshot_count=counts["family_snapshot_count"],
            entry_leg_count=counts["entry_leg_count"],
            exit_plan_count=counts["exit_plan_count"],
            exit_leg_count=counts["exit_leg_count"],
            inserted_count=counts["inserted_count"],
            reused_count=counts["reused_count"],
            rematerialized_count=counts["rematerialized_count"],
            queue_enqueued_count=0,
            queue_claimed_count=0,
            checkpoint_upserted_count=counts["checkpoint_upserted_count"],
            alpha_ledger_path=resolved_alpha_path,
            market_base_path=resolved_market_base_path,
            position_ledger_path=resolved_position_path,
            alpha_formal_signal_table=alpha_formal_signal_table,
            market_price_table=market_price_table,
            adjust_method=adjust_method,
        )
        mark_run_completed(position_connection, run_id=run_id, summary=summary)
        write_summary(summary, summary_path)
        return summary
    except Exception:
        mark_run_failed(position_connection, run_id=run_id)
        raise
    finally:
        position_connection.close()


def _run_position_checkpoint_queue_build(
    *,
    workspace: WorkspaceRoots,
    policy_id: str,
    run_id: str,
    enriched_signals: list[PositionFormalSignalInput],
    missing_reference_price_count: int,
    alpha_rows: list[dict[str, object]],
    resolved_alpha_path: str,
    resolved_market_base_path: str,
    resolved_position_path: str,
    alpha_formal_signal_table: str,
    market_price_table: str,
    adjust_method: str,
    limit: int,
    summary_path,
) -> PositionFormalSignalRunnerSummary:
    workspace.ensure_directories()
    position_connection = connect_position_ledger(workspace)
    try:
        bootstrap_position_ledger(workspace, connection=position_connection)
        candidate_inputs = build_candidate_inputs(position_connection, enriched_signals, policy_id=policy_id)
        insert_run_row(
            position_connection,
            run_id=run_id,
            policy_id=policy_id,
            execution_mode="checkpoint_queue",
            bounded_signal_count=len(candidate_inputs),
            source_signal_contract_version=resolve_signal_contract_version(enriched_signals),
            source_signal_run_id=resolve_signal_run_id(enriched_signals),
        )
        queue_enqueued_count = enqueue_position_dirty_candidates(
            position_connection,
            candidates=candidate_inputs,
            run_id=run_id,
        )
        claimed_queue_rows = claim_position_queue_rows(
            position_connection,
            run_id=run_id,
            candidate_nks={candidate.candidate_nk for candidate in candidate_inputs},
            limit=limit,
        )
        candidate_map = {candidate.candidate_nk: candidate for candidate in candidate_inputs}
        claimed_candidates = [
            candidate_map[str(queue_row["candidate_nk"])]
            for queue_row in claimed_queue_rows
            if str(queue_row["candidate_nk"]) in candidate_map
        ]
        counts = _materialize_candidate_scope(
            connection=position_connection,
            policy_id=policy_id,
            run_id=run_id,
            candidates=claimed_candidates,
            queue_rows={str(row["candidate_nk"]): row for row in claimed_queue_rows},
        )
        summary = PositionFormalSignalRunnerSummary(
            policy_id=policy_id,
            execution_mode="checkpoint_queue",
            position_run_id=run_id,
            alpha_signal_count=len(alpha_rows),
            enriched_signal_count=len(enriched_signals),
            missing_reference_price_count=missing_reference_price_count,
            candidate_count=counts["candidate_count"],
            admitted_count=counts["admitted_count"],
            blocked_count=counts["blocked_count"],
            risk_budget_count=counts["risk_budget_count"],
            sizing_count=counts["sizing_count"],
            family_snapshot_count=counts["family_snapshot_count"],
            entry_leg_count=counts["entry_leg_count"],
            exit_plan_count=counts["exit_plan_count"],
            exit_leg_count=counts["exit_leg_count"],
            inserted_count=counts["inserted_count"],
            reused_count=counts["reused_count"],
            rematerialized_count=counts["rematerialized_count"],
            queue_enqueued_count=queue_enqueued_count,
            queue_claimed_count=len(claimed_candidates),
            checkpoint_upserted_count=counts["checkpoint_upserted_count"],
            alpha_ledger_path=resolved_alpha_path,
            market_base_path=resolved_market_base_path,
            position_ledger_path=resolved_position_path,
            alpha_formal_signal_table=alpha_formal_signal_table,
            market_price_table=market_price_table,
            adjust_method=adjust_method,
        )
        mark_run_completed(position_connection, run_id=run_id, summary=summary)
        write_summary(summary, summary_path)
        return summary
    except Exception:
        mark_run_failed(position_connection, run_id=run_id)
        raise
    finally:
        position_connection.close()


def _materialize_candidate_scope(
    *,
    connection: duckdb.DuckDBPyConnection,
    policy_id: str,
    run_id: str,
    candidates,
    queue_rows,
) -> dict[str, int]:
    policy_contract = fetch_policy_contract(connection, policy_id)
    counts = {
        "candidate_count": 0,
        "admitted_count": 0,
        "blocked_count": 0,
        "risk_budget_count": 0,
        "sizing_count": 0,
        "family_snapshot_count": 0,
        "entry_leg_count": 0,
        "exit_plan_count": 0,
        "exit_leg_count": 0,
        "inserted_count": 0,
        "reused_count": 0,
        "rematerialized_count": 0,
        "checkpoint_upserted_count": 0,
    }
    for candidate in candidates:
        queue_row = None if queue_rows is None else queue_rows.get(candidate.candidate_nk)
        try:
            action = resolve_materialization_action(
                connection,
                candidate_nk=candidate.candidate_nk,
                checkpoint_nk=candidate.checkpoint_nk,
                source_signal_fingerprint=candidate.source_signal_fingerprint,
            )
            if action == "rematerialized":
                delete_position_candidate_scope(connection, candidate_nk=candidate.candidate_nk)
            if action != "reused":
                materialize_position_rows(
                    connection,
                    [candidate.signal],
                    policy_id=policy_id,
                    policy_contract=policy_contract,
                    default_single_name_cap_weight=0.25,
                    default_portfolio_cap_weight=0.50,
                    share_lot_size=100,
                )
            candidate_stats = load_candidate_scope_stats(connection, candidate_nk=candidate.candidate_nk)
            upsert_position_checkpoint(
                connection,
                checkpoint_nk=candidate.checkpoint_nk,
                candidate_nk=candidate.candidate_nk,
                instrument=candidate.signal.instrument,
                checkpoint_scope=policy_id,
                last_signal_nk=candidate.signal.signal_nk,
                last_reference_trade_date=coerce_date(candidate.signal.reference_trade_date),
                last_source_signal_fingerprint=candidate.source_signal_fingerprint,
                last_run_id=run_id,
            )
            counts["checkpoint_upserted_count"] += 1
            insert_run_snapshot_row(
                connection,
                run_id=run_id,
                candidate_nk=candidate.candidate_nk,
                signal_nk=candidate.signal.signal_nk,
                reference_trade_date=coerce_date(candidate.signal.reference_trade_date),
                materialization_action=action,
                queue_row=queue_row,
                candidate_status=candidate_stats["candidate_status"],
                position_action_decision=candidate_stats["position_action_decision"],
            )
            if queue_row is not None:
                mark_position_queue_completed(
                    connection,
                    queue_nk=str(queue_row["queue_nk"]),
                    run_id=run_id,
                )
            counts["candidate_count"] += 1
            counts["risk_budget_count"] += int(candidate_stats["risk_budget_count"])
            counts["sizing_count"] += int(candidate_stats["sizing_count"])
            counts["family_snapshot_count"] += int(candidate_stats["family_snapshot_count"])
            counts["entry_leg_count"] += int(candidate_stats["entry_leg_count"])
            counts["exit_plan_count"] += int(candidate_stats["exit_plan_count"])
            counts["exit_leg_count"] += int(candidate_stats["exit_leg_count"])
            if candidate_stats["candidate_status"] == "admitted":
                counts["admitted_count"] += 1
            else:
                counts["blocked_count"] += 1
            counts[f"{action}_count"] += 1
        except Exception:
            if queue_row is not None:
                mark_position_queue_failed(
                    connection,
                    queue_nk=str(queue_row["queue_nk"]),
                    run_id=run_id,
                )
            raise
    return counts
