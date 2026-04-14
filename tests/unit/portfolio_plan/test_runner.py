"""覆盖 `portfolio_plan` data-grade runner 的单测。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pytest

from mlq.core.paths import default_settings
from mlq.portfolio_plan import (
    DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION,
    portfolio_plan_ledger_path,
    run_portfolio_plan_build,
)
from mlq.position import bootstrap_position_ledger, position_ledger_path


def _clear_workspace_env(monkeypatch) -> None:
    for env_name in (
        "LIFESPAN_REPO_ROOT",
        "LIFESPAN_DATA_ROOT",
        "LIFESPAN_TEMP_ROOT",
        "LIFESPAN_REPORT_ROOT",
        "LIFESPAN_VALIDATED_ROOT",
    ):
        monkeypatch.delenv(env_name, raising=False)


def _bootstrap_repo_root(tmp_path: Path) -> Path:
    repo_root = tmp_path / "lifespan-0.01"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text(
        "[project]\nname='lifespan-0.01'\n",
        encoding="utf-8",
    )
    return repo_root


def _seed_position_bridge_rows(settings, rows: list[dict[str, object]]) -> None:
    bootstrap_position_ledger(settings=settings)
    conn = duckdb.connect(str(position_ledger_path(settings)))
    try:
        for index, row in enumerate(rows, start=1):
            conn.execute(
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["candidate_nk"],
                    row.get("signal_nk", row["candidate_nk"]),
                    row["instrument"],
                    row["policy_id"],
                    row["reference_trade_date"],
                    row["candidate_status"],
                    row.get("blocked_reason_code"),
                    row.get("context_code", "BULL_MAINSTREAM"),
                    row.get("audit_note"),
                    row.get("source_signal_run_id"),
                ],
            )
            conn.execute(
                """
                INSERT INTO position_capacity_snapshot (
                    capacity_snapshot_nk,
                    candidate_nk,
                    capacity_snapshot_role,
                    current_position_weight,
                    risk_budget_weight,
                    context_max_position_weight,
                    single_name_cap_weight,
                    portfolio_cap_weight,
                    remaining_single_name_capacity_weight,
                    remaining_portfolio_capacity_weight,
                    final_allowed_position_weight,
                    required_reduction_weight,
                    binding_cap_code,
                    capacity_source_code
                )
                VALUES (?, ?, 'default', 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    f"{row['candidate_nk']}|capacity|{index}",
                    row["candidate_nk"],
                    row.get("risk_budget_weight", row["final_allowed_position_weight"]),
                    row.get(
                        "context_max_position_weight",
                        row["final_allowed_position_weight"],
                    ),
                    row.get(
                        "single_name_cap_weight",
                        row["final_allowed_position_weight"],
                    ),
                    row.get(
                        "portfolio_cap_weight",
                        row["final_allowed_position_weight"],
                    ),
                    row.get(
                        "remaining_single_name_capacity_weight",
                        row["final_allowed_position_weight"],
                    ),
                    row.get(
                        "remaining_portfolio_capacity_weight",
                        row["final_allowed_position_weight"],
                    ),
                    row["final_allowed_position_weight"],
                    row.get("required_reduction_weight", 0.0),
                    row.get("binding_cap_code", "no_binding_cap"),
                    row.get("capacity_source_code", "unit_test_seed"),
                ],
            )
            conn.execute(
                """
                INSERT INTO position_sizing_snapshot (
                    sizing_snapshot_nk,
                    candidate_nk,
                    policy_id,
                    entry_leg_role,
                    schedule_stage,
                    schedule_lag_days,
                    position_action_decision,
                    target_weight,
                    target_notional,
                    target_shares,
                    final_allowed_position_weight,
                    required_reduction_weight,
                    reference_price,
                    reference_trade_date
                )
                VALUES (?, ?, ?, 'base_entry', ?, ?, ?, ?, 0, 0, ?, ?, 10.0, ?)
                """,
                [
                    f"{row['candidate_nk']}|sizing|{index}",
                    row["candidate_nk"],
                    row["policy_id"],
                    row.get("schedule_stage", "t+1"),
                    row.get("schedule_lag_days", 1),
                    row["position_action_decision"],
                    row["final_allowed_position_weight"],
                    row["final_allowed_position_weight"],
                    row.get("required_reduction_weight", 0.0),
                    row["reference_trade_date"],
                ],
            )
    finally:
        conn.close()


def test_run_portfolio_plan_build_bootstrap_freezes_v2_ledgers_and_natural_keys(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_position_bridge_rows(
        settings,
        [
            {
                "candidate_nk": "cand-001",
                "instrument": "000001.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-09",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.15,
            },
            {
                "candidate_nk": "cand-002",
                "instrument": "000002.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-09",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.10,
            },
            {
                "candidate_nk": "cand-003",
                "instrument": "000003.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-09",
                "candidate_status": "blocked",
                "position_action_decision": "reject_open",
                "final_allowed_position_weight": 0.08,
                "blocked_reason_code": "position_precheck_failed",
            },
            {
                "candidate_nk": "cand-004",
                "instrument": "000004.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-09",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.07,
            },
            {
                "candidate_nk": "cand-005",
                "instrument": "000005.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-09",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.06,
                "schedule_stage": "t+2",
                "schedule_lag_days": 2,
            },
        ],
    )

    summary = run_portfolio_plan_build(
        settings=settings,
        portfolio_id="main_book",
        portfolio_gross_cap_weight=0.20,
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="portfolio-plan-test-001",
        limit=10,
    )

    assert summary.execution_mode == "bootstrap"
    assert summary.bounded_candidate_count == 5
    assert summary.processed_candidate_count == 5
    assert summary.admitted_count == 1
    assert summary.trimmed_count == 1
    assert summary.blocked_count == 2
    assert summary.deferred_count == 1
    assert summary.inserted_count == 5
    assert summary.queue_enqueued_count == 0
    assert summary.queue_claimed_count == 0
    assert summary.checkpoint_upserted_count == 6
    assert summary.freshness_status == "fresh"
    assert summary.portfolio_gross_used_weight == 0.20
    assert summary.portfolio_gross_remaining_weight == 0.0

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_status, execution_mode, bounded_candidate_count, admitted_count,
                   blocked_count, trimmed_count, deferred_count,
                   queue_enqueued_count, queue_claimed_count, checkpoint_upserted_count,
                   freshness_updated_count
            FROM portfolio_plan_run
            WHERE run_id = 'portfolio-plan-test-001'
            """
        ).fetchone()
        checkpoint_rows = conn.execute(
            """
            SELECT checkpoint_nk, checkpoint_scope, last_completed_reference_trade_date,
                   last_completed_candidate_nk
            FROM portfolio_plan_checkpoint
            ORDER BY checkpoint_nk
            """
        ).fetchall()
        freshness_row = conn.execute(
            """
            SELECT latest_reference_trade_date, expected_reference_trade_date,
                   freshness_status, last_success_run_id
            FROM portfolio_plan_freshness_audit
            WHERE portfolio_id = 'main_book'
            """
        ).fetchone()
        decision_rows = conn.execute(
            """
            SELECT candidate_nk, decision_status, decision_reason_code,
                   decision_rank, trade_readiness_status
            FROM portfolio_plan_candidate_decision
            ORDER BY candidate_nk
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", "bootstrap", 5, 1, 2, 1, 1, 0, 0, 6, 1)
    assert len(checkpoint_rows) == 6
    assert freshness_row == (
        date(2026, 4, 9),
        date(2026, 4, 9),
        "fresh",
        "portfolio-plan-test-001",
    )
    assert decision_rows == [
        ("cand-001", "admitted", "admitted_without_trim", 1, "trade_ready"),
        ("cand-002", "trimmed", "trimmed_by_portfolio_capacity", 2, "trade_ready"),
        ("cand-003", "blocked", "position_precheck_failed", 3, "blocked"),
        ("cand-004", "blocked", "portfolio_capacity_exhausted", 4, "blocked"),
        ("cand-005", "deferred", "await_future_schedule_stage", 5, "await_schedule"),
    ]


def test_run_portfolio_plan_build_incremental_queue_checkpoint_and_reuse(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_position_bridge_rows(
        settings,
        [
            {
                "candidate_nk": "cand-101",
                "instrument": "000101.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-09",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.10,
            }
        ],
    )

    first_summary = run_portfolio_plan_build(
        settings=settings,
        portfolio_id="main_book",
        portfolio_gross_cap_weight=0.20,
        run_id="portfolio-plan-test-002a",
        limit=10,
    )
    second_summary = run_portfolio_plan_build(
        settings=settings,
        portfolio_id="main_book",
        portfolio_gross_cap_weight=0.20,
        run_id="portfolio-plan-test-002b",
        limit=10,
    )

    assert first_summary.execution_mode == "incremental"
    assert first_summary.queue_enqueued_count == 1
    assert first_summary.queue_claimed_count == 1
    assert first_summary.inserted_count == 1
    assert first_summary.checkpoint_upserted_count == 2
    assert first_summary.freshness_status == "fresh"
    assert second_summary.execution_mode == "incremental"
    assert second_summary.queue_enqueued_count == 0
    assert second_summary.queue_claimed_count == 0
    assert second_summary.processed_candidate_count == 0
    assert second_summary.inserted_count == 0
    assert second_summary.reused_count == 0
    assert second_summary.rematerialized_count == 0
    assert second_summary.checkpoint_upserted_count == 0
    assert second_summary.freshness_status == "fresh"

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        queue_row = conn.execute(
            """
            SELECT queue_status, queue_reason, last_success_run_id
            FROM portfolio_plan_work_queue
            WHERE candidate_nk = 'cand-101'
            """
        ).fetchone()
        checkpoint_rows = conn.execute(
            """
            SELECT checkpoint_scope, last_completed_candidate_nk
            FROM portfolio_plan_checkpoint
            ORDER BY checkpoint_scope, checkpoint_nk
            """
        ).fetchall()
        freshness_row = conn.execute(
            """
            SELECT freshness_status, last_success_run_id
            FROM portfolio_plan_freshness_audit
            WHERE portfolio_id = 'main_book'
            """
        ).fetchone()
    finally:
        conn.close()

    assert queue_row == ("completed", "bootstrap_missing_checkpoint", "portfolio-plan-test-002a")
    assert checkpoint_rows == [
        ("candidate", "cand-101"),
        ("portfolio_gross", "cand-101"),
    ]
    assert freshness_row == ("fresh", "portfolio-plan-test-002b")


def test_run_portfolio_plan_build_replay_expands_to_full_date_scope(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_position_bridge_rows(
        settings,
        [
            {
                "candidate_nk": "cand-201",
                "instrument": "000201.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-09",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.15,
            },
            {
                "candidate_nk": "cand-202",
                "instrument": "000202.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-09",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.10,
            },
        ],
    )

    first_summary = run_portfolio_plan_build(
        settings=settings,
        portfolio_id="main_book",
        portfolio_gross_cap_weight=0.20,
        run_id="portfolio-plan-test-003a",
        limit=10,
    )
    assert first_summary.queue_enqueued_count == 2
    assert first_summary.queue_claimed_count == 2

    conn = duckdb.connect(str(position_ledger_path(settings)))
    try:
        conn.execute(
            """
            UPDATE position_capacity_snapshot
            SET final_allowed_position_weight = 0.08
            WHERE candidate_nk = 'cand-201'
            """
        )
        conn.execute(
            """
            UPDATE position_sizing_snapshot
            SET final_allowed_position_weight = 0.08,
                target_weight = 0.08
            WHERE candidate_nk = 'cand-201'
            """
        )
    finally:
        conn.close()

    replay_summary = run_portfolio_plan_build(
        settings=settings,
        portfolio_id="main_book",
        portfolio_gross_cap_weight=0.20,
        candidate_nks=["cand-201"],
        replay_mode=True,
        run_id="portfolio-plan-test-003b",
        limit=10,
    )

    assert replay_summary.execution_mode == "replay"
    assert replay_summary.bounded_candidate_count == 1
    assert replay_summary.processed_candidate_count == 2
    assert replay_summary.queue_enqueued_count == 1
    assert replay_summary.queue_claimed_count == 1
    assert replay_summary.rematerialized_count == 2

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        decision_rows = conn.execute(
            """
            SELECT candidate_nk, admitted_weight, last_materialized_run_id
            FROM portfolio_plan_candidate_decision
            ORDER BY candidate_nk
            """
        ).fetchall()
        run_snapshot_rows = conn.execute(
            """
            SELECT candidate_nk, queue_nk, materialization_action
            FROM portfolio_plan_run_snapshot
            WHERE run_id = 'portfolio-plan-test-003b'
            ORDER BY candidate_nk
            """
        ).fetchall()
    finally:
        conn.close()

    assert decision_rows == [
        ("cand-201", 0.08, "portfolio-plan-test-003b"),
        ("cand-202", 0.10, "portfolio-plan-test-003b"),
    ]
    assert run_snapshot_rows == [
        ("cand-201", "main_book|cand-201|2026-04-09", "rematerialized"),
        ("cand-202", None, "rematerialized"),
    ]


def test_run_portfolio_plan_build_marks_freshness_stale_on_partial_incremental_claim(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_position_bridge_rows(
        settings,
        [
            {
                "candidate_nk": "cand-301",
                "instrument": "000301.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-08",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.08,
            },
            {
                "candidate_nk": "cand-302",
                "instrument": "000302.SZ",
                "policy_id": "fixed_notional_full_exit_v1",
                "reference_trade_date": "2026-04-09",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.07,
            },
        ],
    )

    summary = run_portfolio_plan_build(
        settings=settings,
        portfolio_id="main_book",
        portfolio_gross_cap_weight=0.20,
        run_id="portfolio-plan-test-004",
        limit=1,
    )

    assert summary.execution_mode == "incremental"
    assert summary.bounded_candidate_count == 2
    assert summary.queue_enqueued_count == 2
    assert summary.queue_claimed_count == 1
    assert summary.processed_candidate_count == 1
    assert summary.latest_reference_trade_date == "2026-04-08"
    assert summary.expected_reference_trade_date == "2026-04-09"
    assert summary.freshness_status == "stale"

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        freshness_row = conn.execute(
            """
            SELECT latest_reference_trade_date, expected_reference_trade_date, freshness_status
            FROM portfolio_plan_freshness_audit
            WHERE portfolio_id = 'main_book'
            """
        ).fetchone()
        queue_rows = conn.execute(
            """
            SELECT candidate_nk, queue_status
            FROM portfolio_plan_work_queue
            ORDER BY candidate_nk
            """
        ).fetchall()
    finally:
        conn.close()

    assert freshness_row == (date(2026, 4, 8), date(2026, 4, 9), "stale")
    assert queue_rows == [
        ("cand-301", "completed"),
        ("cand-302", "pending"),
    ]
