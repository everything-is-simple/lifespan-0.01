"""Cover official bounded `position -> portfolio_plan` materialization."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from mlq.core.paths import default_settings
from mlq.portfolio_plan import portfolio_plan_ledger_path, run_portfolio_plan_build
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
                    context_max_position_weight,
                    remaining_single_name_capacity_weight,
                    remaining_portfolio_capacity_weight,
                    final_allowed_position_weight,
                    required_reduction_weight,
                    capacity_source_code
                )
                VALUES (?, ?, 'default', 0, ?, ?, ?, ?, ?, 'unit_test_seed')
                """,
                [
                    f"{row['candidate_nk']}|capacity|{index}",
                    row["candidate_nk"],
                    row.get("context_max_position_weight", row["final_allowed_position_weight"]),
                    row.get("remaining_single_name_capacity_weight", row["final_allowed_position_weight"]),
                    row.get("remaining_portfolio_capacity_weight", row["final_allowed_position_weight"]),
                    row["final_allowed_position_weight"],
                    row.get("required_reduction_weight", 0.0),
                ],
            )
            conn.execute(
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
                VALUES (?, ?, ?, 'base_entry', ?, ?, 0, 0, ?, ?, 10.0, ?)
                """,
                [
                    f"{row['candidate_nk']}|sizing|{index}",
                    row["candidate_nk"],
                    row["policy_id"],
                    row["position_action_decision"],
                    row["final_allowed_position_weight"],
                    row["final_allowed_position_weight"],
                    row.get("required_reduction_weight", 0.0),
                    row["reference_trade_date"],
                ],
            )
    finally:
        conn.close()


def test_run_portfolio_plan_build_materializes_admitted_trimmed_and_blocked_rows(
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

    assert summary.bounded_candidate_count == 4
    assert summary.admitted_count == 1
    assert summary.trimmed_count == 1
    assert summary.blocked_count == 2
    assert summary.inserted_count == 4
    assert summary.portfolio_gross_used_weight == 0.20
    assert summary.portfolio_gross_remaining_weight == 0.0

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_status, bounded_candidate_count, admitted_count, blocked_count, trimmed_count
            FROM portfolio_plan_run
            WHERE run_id = 'portfolio-plan-test-001'
            """
        ).fetchone()
        snapshot_rows = conn.execute(
            """
            SELECT candidate_nk, plan_status, admitted_weight, trimmed_weight, blocking_reason_code,
                   portfolio_gross_used_weight, portfolio_gross_remaining_weight
            FROM portfolio_plan_snapshot
            ORDER BY candidate_nk
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", 4, 1, 2, 1)
    assert snapshot_rows[0][0:5] == ("cand-001", "admitted", 0.15, 0.0, None)
    assert snapshot_rows[0][5] == pytest.approx(0.15)
    assert snapshot_rows[0][6] == pytest.approx(0.05)
    assert snapshot_rows[1][0:2] == ("cand-002", "trimmed")
    assert snapshot_rows[1][2] == pytest.approx(0.05)
    assert snapshot_rows[1][3] == pytest.approx(0.05)
    assert snapshot_rows[1][4] is None
    assert snapshot_rows[1][5] == pytest.approx(0.20)
    assert snapshot_rows[1][6] == pytest.approx(0.0)
    assert snapshot_rows[2] == (
        "cand-003",
        "blocked",
        0.0,
        0.0,
        "position_candidate_blocked",
        0.20,
        0.0,
    )
    assert snapshot_rows[3] == (
        "cand-004",
        "blocked",
        0.0,
        0.0,
        "portfolio_capacity_exhausted",
        0.20,
        0.0,
    )


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
        snapshot_row = conn.execute(
            """
            SELECT admitted_weight, last_materialized_run_id
            FROM portfolio_plan_snapshot
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

    assert snapshot_row == (0.12, "portfolio-plan-test-002c")
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
    finally:
        conn.close()

    assert snapshot_rows == [("cand-202", date(2026, 4, 9))]
