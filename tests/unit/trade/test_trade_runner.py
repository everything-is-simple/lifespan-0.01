"""覆盖正式 bounded 的 `portfolio_plan -> trade_runtime` 物化。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.portfolio_plan import bootstrap_portfolio_plan_ledger, portfolio_plan_ledger_path
from mlq.trade import run_trade_runtime_build, trade_runtime_ledger_path


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


def _seed_portfolio_plan_rows(settings, rows: list[dict[str, object]]) -> None:
    bootstrap_portfolio_plan_ledger(settings=settings)
    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)))
    try:
        for index, row in enumerate(rows, start=1):
            conn.execute(
                """
                INSERT INTO portfolio_plan_snapshot (
                    plan_snapshot_nk,
                    candidate_nk,
                    portfolio_id,
                    instrument,
                    reference_trade_date,
                    position_action_decision,
                    requested_weight,
                    admitted_weight,
                    trimmed_weight,
                    plan_status,
                    blocking_reason_code,
                    portfolio_gross_cap_weight,
                    portfolio_gross_used_weight,
                    portfolio_gross_remaining_weight,
                    candidate_decision_nk,
                    capacity_snapshot_nk,
                    allocation_snapshot_nk,
                    portfolio_plan_contract_version,
                    first_seen_run_id,
                    last_materialized_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1.0, 0.0, 1.0, ?, ?, ?, 'portfolio-plan-v1', ?, ?)
                """,
                [
                    row["plan_snapshot_nk"],
                    row["candidate_nk"],
                    row.get("portfolio_id", "main_book"),
                    row["instrument"],
                    row["reference_trade_date"],
                    row.get("position_action_decision", "open_up_to_context_cap"),
                    row.get("requested_weight", row.get("admitted_weight", 0.0)),
                    row.get("admitted_weight", 0.0),
                    row.get("trimmed_weight", 0.0),
                    row["plan_status"],
                    row.get("blocking_reason_code"),
                    row.get(
                        "candidate_decision_nk",
                        f"{row['candidate_nk']}|decision|portfolio-plan-v1",
                    ),
                    row.get(
                        "capacity_snapshot_nk",
                        f"{row.get('portfolio_id', 'main_book')}|portfolio_gross|{row['reference_trade_date']}|portfolio-plan-v1",
                    ),
                    row.get(
                        "allocation_snapshot_nk",
                        f"{row['candidate_nk']}|allocation|portfolio-plan-v1",
                    ),
                    f"plan-run-seed-{index}",
                    f"plan-run-seed-{index}",
                ],
            )
    finally:
        conn.close()


def _seed_market_base_prices(market_base_path: Path, rows: list[tuple[object, ...]]) -> None:
    market_base_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(market_base_path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                code TEXT NOT NULL,
                trade_date DATE NOT NULL,
                adjust_method TEXT NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE
            )
            """
        )
        for row in rows:
            conn.execute("INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, ?, ?, ?, ?)", row)
    finally:
        conn.close()


def test_run_trade_runtime_build_materializes_planned_entry_blocked_and_carry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_portfolio_plan_rows(
        settings,
        [
            {
                "plan_snapshot_nk": "plan-001",
                "candidate_nk": "cand-001",
                "instrument": "000001.SZ",
                "reference_trade_date": "2026-04-09",
                "requested_weight": 0.12,
                "admitted_weight": 0.12,
                "trimmed_weight": 0.0,
                "plan_status": "admitted",
            },
            {
                "plan_snapshot_nk": "plan-002",
                "candidate_nk": "cand-002",
                "instrument": "000002.SZ",
                "reference_trade_date": "2026-04-09",
                "requested_weight": 0.08,
                "admitted_weight": 0.0,
                "trimmed_weight": 0.0,
                "plan_status": "blocked",
                "blocking_reason_code": "portfolio_capacity_exhausted",
            },
            {
                "plan_snapshot_nk": "plan-003",
                "candidate_nk": "cand-003",
                "instrument": "000003.SZ",
                "reference_trade_date": "2026-04-09",
                "requested_weight": 0.0,
                "admitted_weight": 0.0,
                "trimmed_weight": 0.0,
                "plan_status": "blocked",
                "blocking_reason_code": "missing_trade_day_after_signal",
            },
        ],
    )
    _seed_market_base_prices(
        settings.databases.market_base,
        [
            ("000001.SZ", "2026-04-10", "none", 10.1, 10.8, 9.9, 10.5),
            ("000002.SZ", "2026-04-10", "none", 20.1, 20.9, 19.8, 20.4),
            ("000003.SZ", "2026-04-09", "none", 30.0, 30.9, 29.8, 30.5),
        ],
    )

    first_summary = run_trade_runtime_build(
        settings=settings,
        portfolio_id="main_book",
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="trade-run-test-001a",
        limit=10,
    )
    second_summary = run_trade_runtime_build(
        settings=settings,
        portfolio_id="main_book",
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="trade-run-test-001b",
        limit=10,
    )
    third_summary = run_trade_runtime_build(
        settings=settings,
        portfolio_id="main_book",
        signal_start_date="2026-04-10",
        signal_end_date="2026-04-10",
        instruments=["000001.SZ"],
        run_id="trade-run-test-001c",
        limit=10,
    )

    assert first_summary.bounded_plan_count == 3
    assert first_summary.planned_entry_count == 1
    assert first_summary.blocked_upstream_count == 2
    assert first_summary.execution_plan_inserted_count == 3
    assert second_summary.execution_plan_reused_count == 3
    assert third_summary.bounded_plan_count == 0
    assert third_summary.planned_carry_count == 1

    conn = duckdb.connect(str(trade_runtime_ledger_path(settings)), read_only=True)
    try:
        run_rows = conn.execute(
            """
            SELECT run_id, run_status, bounded_plan_count, planned_entry_count, blocked_upstream_count, carried_open_leg_count
            FROM trade_run
            ORDER BY run_id
            """
        ).fetchall()
        execution_rows = conn.execute(
            """
            SELECT execution_plan_nk, plan_snapshot_nk, instrument, execution_action, execution_status,
                   planned_entry_trade_date, planned_entry_weight, carry_source_status
            FROM trade_execution_plan
            ORDER BY execution_plan_nk
            """
        ).fetchall()
        leg_rows = conn.execute(
            """
            SELECT position_leg_nk, instrument, entry_trade_date, entry_weight, remaining_weight, leg_status
            FROM trade_position_leg
            ORDER BY position_leg_nk
            """
        ).fetchall()
        carry_rows = conn.execute(
            """
            SELECT carry_snapshot_nk, snapshot_date, instrument, current_position_weight, open_leg_count, carry_source_status
            FROM trade_carry_snapshot
            ORDER BY snapshot_date, instrument
            """
        ).fetchall()
        run_execution_actions = conn.execute(
            """
            SELECT run_id, materialization_action
            FROM trade_run_execution_plan
            ORDER BY run_id, execution_plan_nk
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_rows == [
        ("trade-run-test-001a", "completed", 3, 1, 2, 1),
        ("trade-run-test-001b", "completed", 3, 1, 2, 1),
        ("trade-run-test-001c", "completed", 0, 0, 0, 1),
    ]
    assert execution_rows == [
        (
            "carry::main_book|000001.SZ|2026-04-09|trade-runtime-v1|2026-04-10|trade-runtime-v1",
            "carry::main_book|000001.SZ|2026-04-09|trade-runtime-v1",
            "000001.SZ",
            "carry_forward",
            "planned_carry",
            date(2026, 4, 10),
            0.0,
            "retained_open_leg_ready",
        ),
        (
            "plan-001|2026-04-10|trade-runtime-v1",
            "plan-001",
            "000001.SZ",
            "enter",
            "planned_entry",
            date(2026, 4, 10),
            0.12,
            "no_prior_trade_run",
        ),
        (
            "plan-002|2026-04-10|trade-runtime-v1",
            "plan-002",
            "000002.SZ",
            "block_upstream",
            "blocked_upstream",
            date(2026, 4, 10),
            0.0,
            "no_prior_trade_run",
        ),
        (
            "plan-003|2026-04-09|trade-runtime-v1",
            "plan-003",
            "000003.SZ",
            "block_upstream",
            "blocked_upstream",
            date(2026, 4, 9),
            0.0,
            "no_prior_trade_run",
        ),
    ]
    assert leg_rows == [
        (
            "plan-001|2026-04-10|trade-runtime-v1|core",
            "000001.SZ",
            date(2026, 4, 10),
            0.12,
            0.12,
            "open",
        )
    ]
    assert carry_rows == [
        (
            "main_book|000001.SZ|2026-04-09|trade-runtime-v1",
            date(2026, 4, 9),
            "000001.SZ",
            0.12,
            1,
            "retained_open_leg_ready",
        ),
        (
            "main_book|000002.SZ|2026-04-09|trade-runtime-v1",
            date(2026, 4, 9),
            "000002.SZ",
            0.0,
            0,
            "no_prior_trade_run",
        ),
        (
            "main_book|000003.SZ|2026-04-09|trade-runtime-v1",
            date(2026, 4, 9),
            "000003.SZ",
            0.0,
            0,
            "no_prior_trade_run",
        ),
        (
            "main_book|000001.SZ|2026-04-10|trade-runtime-v1",
            date(2026, 4, 10),
            "000001.SZ",
            0.12,
            1,
            "retained_open_leg_ready",
        ),
    ]
    assert run_execution_actions == [
        ("trade-run-test-001a", "inserted"),
        ("trade-run-test-001a", "inserted"),
        ("trade-run-test-001a", "inserted"),
        ("trade-run-test-001b", "reused"),
        ("trade-run-test-001b", "reused"),
        ("trade-run-test-001b", "reused"),
        ("trade-run-test-001c", "inserted"),
    ]


def test_run_trade_runtime_build_marks_rematerialized_when_plan_weight_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_portfolio_plan_rows(
        settings,
        [
            {
                "plan_snapshot_nk": "plan-101",
                "candidate_nk": "cand-101",
                "instrument": "000101.SZ",
                "reference_trade_date": "2026-04-09",
                "requested_weight": 0.10,
                "admitted_weight": 0.10,
                "trimmed_weight": 0.0,
                "plan_status": "admitted",
            }
        ],
    )
    _seed_market_base_prices(
        settings.databases.market_base,
        [
            ("000101.SZ", "2026-04-10", "none", 11.0, 11.5, 10.8, 11.2),
        ],
    )

    first_summary = run_trade_runtime_build(
        settings=settings,
        portfolio_id="main_book",
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="trade-run-test-002a",
        limit=10,
    )
    second_summary = run_trade_runtime_build(
        settings=settings,
        portfolio_id="main_book",
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="trade-run-test-002b",
        limit=10,
    )

    assert first_summary.execution_plan_inserted_count == 1
    assert second_summary.execution_plan_reused_count == 1

    conn = duckdb.connect(str(portfolio_plan_ledger_path(settings)))
    try:
        conn.execute(
            """
            UPDATE portfolio_plan_snapshot
            SET admitted_weight = 0.15,
                requested_weight = 0.15,
                last_materialized_run_id = 'plan-run-updated-001'
            WHERE plan_snapshot_nk = 'plan-101'
            """
        )
    finally:
        conn.close()

    third_summary = run_trade_runtime_build(
        settings=settings,
        portfolio_id="main_book",
        signal_start_date="2026-04-09",
        signal_end_date="2026-04-09",
        run_id="trade-run-test-002c",
        limit=10,
    )

    assert third_summary.execution_plan_rematerialized_count == 1
    assert third_summary.position_leg_rematerialized_count == 1

    conn = duckdb.connect(str(trade_runtime_ledger_path(settings)), read_only=True)
    try:
        execution_row = conn.execute(
            """
            SELECT planned_entry_weight, last_materialized_run_id
            FROM trade_execution_plan
            WHERE execution_plan_nk = 'plan-101|2026-04-10|trade-runtime-v1'
            """
        ).fetchone()
        leg_row = conn.execute(
            """
            SELECT entry_weight, last_materialized_run_id
            FROM trade_position_leg
            WHERE position_leg_nk = 'plan-101|2026-04-10|trade-runtime-v1|core'
            """
        ).fetchone()
        carry_row = conn.execute(
            """
            SELECT current_position_weight
            FROM trade_carry_snapshot
            WHERE carry_snapshot_nk = 'main_book|000101.SZ|2026-04-09|trade-runtime-v1'
            """
        ).fetchone()
        actions = conn.execute(
            """
            SELECT run_id, materialization_action
            FROM trade_run_execution_plan
            ORDER BY run_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert execution_row == (0.15, "trade-run-test-002c")
    assert leg_row == (0.15, "trade-run-test-002c")
    assert carry_row == (0.15,)
    assert actions == [
        ("trade-run-test-002a", "inserted"),
        ("trade-run-test-002b", "reused"),
        ("trade-run-test-002c", "rematerialized"),
    ]
