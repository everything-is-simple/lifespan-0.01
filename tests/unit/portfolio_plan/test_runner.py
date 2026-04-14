"""覆盖正式 bounded 的 `position -> portfolio_plan` v2 物化。"""

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
    (repo_root / "pyproject.toml").write_text("[project]\nname='lifespan-0.01'\n", encoding="utf-8")
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
                    row.get("context_max_position_weight", row["final_allowed_position_weight"]),
                    row.get("single_name_cap_weight", row["final_allowed_position_weight"]),
                    row.get("portfolio_cap_weight", row["final_allowed_position_weight"]),
                    row.get("remaining_single_name_capacity_weight", row["final_allowed_position_weight"]),
                    row.get("remaining_portfolio_capacity_weight", row["final_allowed_position_weight"]),
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


def test_run_portfolio_plan_build_freezes_v2_ledgers_and_natural_keys(
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

    assert summary.bounded_candidate_count == 5
    assert summary.admitted_count == 1
    assert summary.trimmed_count == 1
    assert summary.blocked_count == 2
    assert summary.deferred_count == 1
    assert summary.inserted_count == 5
    assert summary.portfolio_gross_used_weight == 0.20
    assert summary.portfolio_gross_remaining_weight == 0.0
    assert summary.blocked_total_weight == pytest.approx(0.15)
    assert summary.deferred_total_weight == pytest.approx(0.06)
    assert summary.decision_reason_counts == {
        "admitted_without_trim": 1,
        "await_future_schedule_stage": 1,
        "portfolio_capacity_exhausted": 1,
        "position_precheck_failed": 1,
        "trimmed_by_portfolio_capacity": 1,
    }
    assert summary.trade_readiness_counts == {
        "await_schedule": 1,
        "blocked": 2,
        "trade_ready": 2,
    }

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_status, bounded_candidate_count, admitted_count, blocked_count, trimmed_count, deferred_count
            FROM portfolio_plan_run
            WHERE run_id = 'portfolio-plan-test-001'
            """
        ).fetchone()
        decision_rows = conn.execute(
            """
            SELECT candidate_decision_nk, candidate_nk, decision_status, decision_reason_code,
                   decision_rank, trade_readiness_status, source_binding_cap_code,
                   capacity_before_weight, capacity_after_weight,
                   requested_weight, admitted_weight, trimmed_weight
            FROM portfolio_plan_candidate_decision
            ORDER BY candidate_nk
            """
        ).fetchall()
        capacity_row = conn.execute(
            """
            SELECT capacity_snapshot_nk, capacity_scope, requested_candidate_count, admitted_candidate_count,
                   blocked_candidate_count, trimmed_candidate_count, deferred_candidate_count,
                   requested_total_weight, blocked_total_weight, deferred_total_weight,
                   binding_constraint_code, capacity_decision_reason_code,
                   portfolio_gross_used_weight, portfolio_gross_remaining_weight
            FROM portfolio_plan_capacity_snapshot
            """
        ).fetchone()
        capacity_summary_json = conn.execute(
            """
            SELECT capacity_reason_summary_json
            FROM portfolio_plan_capacity_snapshot
            """
        ).fetchone()[0]
        allocation_rows = conn.execute(
            """
            SELECT allocation_snapshot_nk, candidate_nk, allocation_scene, final_allocated_weight, plan_status,
                   decision_reason_code, decision_rank, trade_readiness_status, schedule_stage
            FROM portfolio_plan_allocation_snapshot
            ORDER BY candidate_nk
            """
        ).fetchall()
        snapshot_rows = conn.execute(
            """
            SELECT candidate_nk, candidate_decision_nk, capacity_snapshot_nk, allocation_snapshot_nk,
                   plan_status, decision_reason_code, decision_rank, trade_readiness_status
            FROM portfolio_plan_snapshot
            ORDER BY candidate_nk
            """
        ).fetchall()
    finally:
        conn.close()

    contract_version = DEFAULT_PORTFOLIO_PLAN_CONTRACT_VERSION
    assert run_row == ("completed", 5, 1, 2, 1, 1)
    assert [row[:4] for row in decision_rows] == [
        (
            f"cand-001|main_book|2026-04-09|{contract_version}",
            "cand-001",
            "admitted",
            "admitted_without_trim",
        ),
        (
            f"cand-002|main_book|2026-04-09|{contract_version}",
            "cand-002",
            "trimmed",
            "trimmed_by_portfolio_capacity",
        ),
        (
            f"cand-003|main_book|2026-04-09|{contract_version}",
            "cand-003",
            "blocked",
            "position_precheck_failed",
        ),
        (
            f"cand-004|main_book|2026-04-09|{contract_version}",
            "cand-004",
            "blocked",
            "portfolio_capacity_exhausted",
        ),
        (
            f"cand-005|main_book|2026-04-09|{contract_version}",
            "cand-005",
            "deferred",
            "await_future_schedule_stage",
        ),
    ]
    assert decision_rows[0][4:7] == (1, "trade_ready", "no_binding_cap")
    assert decision_rows[0][7] == pytest.approx(0.20)
    assert decision_rows[0][8] == pytest.approx(0.05)
    assert decision_rows[0][9] == pytest.approx(0.15)
    assert decision_rows[0][10] == pytest.approx(0.15)
    assert decision_rows[0][11] == pytest.approx(0.0)
    assert decision_rows[1][4:7] == (2, "trade_ready", "no_binding_cap")
    assert decision_rows[1][7] == pytest.approx(0.05)
    assert decision_rows[1][8] == pytest.approx(0.0)
    assert decision_rows[1][9] == pytest.approx(0.10)
    assert decision_rows[1][10] == pytest.approx(0.05)
    assert decision_rows[1][11] == pytest.approx(0.05)
    assert decision_rows[2][4:7] == (3, "blocked", "no_binding_cap")
    assert decision_rows[2][7] == pytest.approx(0.0)
    assert decision_rows[2][8] == pytest.approx(0.0)
    assert decision_rows[2][9] == pytest.approx(0.08)
    assert decision_rows[2][10] == pytest.approx(0.0)
    assert decision_rows[2][11] == pytest.approx(0.0)
    assert decision_rows[3][4:7] == (4, "blocked", "no_binding_cap")
    assert decision_rows[3][7] == pytest.approx(0.0)
    assert decision_rows[3][8] == pytest.approx(0.0)
    assert decision_rows[3][9] == pytest.approx(0.07)
    assert decision_rows[3][10] == pytest.approx(0.0)
    assert decision_rows[3][11] == pytest.approx(0.0)
    assert decision_rows[4][4:7] == (5, "await_schedule", "no_binding_cap")
    assert decision_rows[4][7] == pytest.approx(0.0)
    assert decision_rows[4][8] == pytest.approx(0.0)
    assert decision_rows[4][9] == pytest.approx(0.06)
    assert decision_rows[4][10] == pytest.approx(0.0)
    assert decision_rows[4][11] == pytest.approx(0.0)
    assert capacity_row[:7] == (
        f"main_book|portfolio_gross|2026-04-09|{contract_version}",
        "portfolio_gross",
        5,
        1,
        2,
        1,
        1,
    )
    assert capacity_row[7] == pytest.approx(0.46)
    assert capacity_row[8] == pytest.approx(0.15)
    assert capacity_row[9] == pytest.approx(0.06)
    assert capacity_row[10:] == (
        "portfolio_gross_cap",
        "portfolio_capacity_binding",
        0.20,
        0.0,
    )
    capacity_summary = json.loads(capacity_summary_json)
    assert capacity_summary["status_counts"] == {
        "admitted": 1,
        "blocked": 2,
        "deferred": 1,
        "trimmed": 1,
    }
    assert capacity_summary["decision_reason_counts"]["await_future_schedule_stage"] == 1
    assert capacity_summary["trimmed_candidates"][0]["candidate_nk"] == "cand-002"
    assert [row[:3] for row in allocation_rows] == [
        (
            f"cand-001|main_book|trade_ready|2026-04-09|{contract_version}",
            "cand-001",
            "trade_ready",
        ),
        (
            f"cand-002|main_book|trade_ready|2026-04-09|{contract_version}",
            "cand-002",
            "trade_ready",
        ),
        (
            f"cand-003|main_book|trade_ready|2026-04-09|{contract_version}",
            "cand-003",
            "trade_ready",
        ),
        (
            f"cand-004|main_book|trade_ready|2026-04-09|{contract_version}",
            "cand-004",
            "trade_ready",
        ),
        (
            f"cand-005|main_book|trade_ready|2026-04-09|{contract_version}",
            "cand-005",
            "trade_ready",
        ),
    ]
    assert allocation_rows[0][3] == pytest.approx(0.15)
    assert allocation_rows[0][4:] == ("admitted", "admitted_without_trim", 1, "trade_ready", "t+1")
    assert allocation_rows[1][3] == pytest.approx(0.05)
    assert allocation_rows[1][4:] == ("trimmed", "trimmed_by_portfolio_capacity", 2, "trade_ready", "t+1")
    assert allocation_rows[2][3] == pytest.approx(0.0)
    assert allocation_rows[2][4:] == ("blocked", "position_precheck_failed", 3, "blocked", "t+1")
    assert allocation_rows[3][3] == pytest.approx(0.0)
    assert allocation_rows[3][4:] == ("blocked", "portfolio_capacity_exhausted", 4, "blocked", "t+1")
    assert allocation_rows[4][3] == pytest.approx(0.0)
    assert allocation_rows[4][4:] == ("deferred", "await_future_schedule_stage", 5, "await_schedule", "t+2")
    assert snapshot_rows == [
        (
            "cand-001",
            f"cand-001|main_book|2026-04-09|{contract_version}",
            f"main_book|portfolio_gross|2026-04-09|{contract_version}",
            f"cand-001|main_book|trade_ready|2026-04-09|{contract_version}",
            "admitted",
            "admitted_without_trim",
            1,
            "trade_ready",
        ),
        (
            "cand-002",
            f"cand-002|main_book|2026-04-09|{contract_version}",
            f"main_book|portfolio_gross|2026-04-09|{contract_version}",
            f"cand-002|main_book|trade_ready|2026-04-09|{contract_version}",
            "trimmed",
            "trimmed_by_portfolio_capacity",
            2,
            "trade_ready",
        ),
        (
            "cand-003",
            f"cand-003|main_book|2026-04-09|{contract_version}",
            f"main_book|portfolio_gross|2026-04-09|{contract_version}",
            f"cand-003|main_book|trade_ready|2026-04-09|{contract_version}",
            "blocked",
            "position_precheck_failed",
            3,
            "blocked",
        ),
        (
            "cand-004",
            f"cand-004|main_book|2026-04-09|{contract_version}",
            f"main_book|portfolio_gross|2026-04-09|{contract_version}",
            f"cand-004|main_book|trade_ready|2026-04-09|{contract_version}",
            "blocked",
            "portfolio_capacity_exhausted",
            4,
            "blocked",
        ),
        (
            "cand-005",
            f"cand-005|main_book|2026-04-09|{contract_version}",
            f"main_book|portfolio_gross|2026-04-09|{contract_version}",
            f"cand-005|main_book|trade_ready|2026-04-09|{contract_version}",
            "deferred",
            "await_future_schedule_stage",
            5,
            "await_schedule",
        ),
    ]


def test_run_portfolio_plan_build_marks_reused_and_rematerialized(
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

    assert first_summary.inserted_count == 1
    assert second_summary.reused_count == 1

    conn = duckdb.connect(str(position_ledger_path(settings)))
    try:
        conn.execute(
            """
            UPDATE position_capacity_snapshot
            SET final_allowed_position_weight = 0.12
            WHERE candidate_nk = 'cand-101'
            """
        )
        conn.execute(
            """
            UPDATE position_sizing_snapshot
            SET final_allowed_position_weight = 0.12,
                target_weight = 0.12
            WHERE candidate_nk = 'cand-101'
            """
        )
    finally:
        conn.close()

    third_summary = run_portfolio_plan_build(
        settings=settings,
        portfolio_id="main_book",
        portfolio_gross_cap_weight=0.20,
        run_id="portfolio-plan-test-002c",
        limit=10,
    )

    assert third_summary.rematerialized_count == 1

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        decision_row = conn.execute(
            """
            SELECT admitted_weight, last_materialized_run_id
            FROM portfolio_plan_candidate_decision
            WHERE candidate_nk = 'cand-101'
            """
        ).fetchone()
        allocation_row = conn.execute(
            """
            SELECT final_allocated_weight, last_materialized_run_id
            FROM portfolio_plan_allocation_snapshot
            WHERE candidate_nk = 'cand-101'
            """
        ).fetchone()
        action_rows = conn.execute(
            """
            SELECT run_id, materialization_action
            FROM portfolio_plan_run_snapshot
            ORDER BY run_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert decision_row == (0.12, "portfolio-plan-test-002c")
    assert allocation_row == (0.12, "portfolio-plan-test-002c")
    assert action_rows == [
        ("portfolio-plan-test-002a", "inserted"),
        ("portfolio-plan-test-002b", "reused"),
        ("portfolio-plan-test-002c", "rematerialized"),
    ]


def test_run_portfolio_plan_build_uses_reference_trade_date_window(
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
                "reference_trade_date": "2026-04-08",
                "candidate_status": "admitted",
                "position_action_decision": "open_up_to_context_cap",
                "final_allowed_position_weight": 0.08,
            },
            {
                "candidate_nk": "cand-202",
                "instrument": "000202.SZ",
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
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="portfolio-plan-test-003",
        limit=10,
    )

    assert summary.bounded_candidate_count == 1

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        snapshot_rows = conn.execute(
            """
            SELECT candidate_nk, reference_trade_date
            FROM portfolio_plan_snapshot
            """
        ).fetchall()
        capacity_rows = conn.execute(
            """
            SELECT reference_trade_date
            FROM portfolio_plan_capacity_snapshot
            """
        ).fetchall()
    finally:
        conn.close()

    assert snapshot_rows == [("cand-202", date(2026, 4, 9))]
    assert capacity_rows == [(date(2026, 4, 9),)]
