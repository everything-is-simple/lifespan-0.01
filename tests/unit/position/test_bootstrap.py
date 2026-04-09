"""覆盖 `position` 最小账本 bootstrap。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.position import (
    DEFAULT_POSITION_POLICY_SEEDS,
    POSITION_LEDGER_TABLE_NAMES,
    PositionFormalSignalInput,
    bootstrap_position_ledger,
    materialize_position_from_formal_signals,
    position_ledger_path,
)


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


def test_bootstrap_position_ledger_creates_all_tables_and_policy_seeds(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    created_tables = bootstrap_position_ledger(settings=settings)
    db_path = position_ledger_path(settings)

    assert created_tables == POSITION_LEDGER_TABLE_NAMES
    assert db_path.exists()

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        existing_tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        assert set(POSITION_LEDGER_TABLE_NAMES).issubset(existing_tables)

        policy_ids = [
            row[0]
            for row in conn.execute(
                "SELECT policy_id FROM position_policy_registry ORDER BY policy_id"
            ).fetchall()
        ]
    finally:
        conn.close()

    assert policy_ids == sorted(seed.policy_id for seed in DEFAULT_POSITION_POLICY_SEEDS)


def test_bootstrap_position_ledger_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    bootstrap_position_ledger(settings=settings)
    bootstrap_position_ledger(settings=settings)

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        policy_count = conn.execute(
            "SELECT COUNT(*) FROM position_policy_registry"
        ).fetchone()[0]
    finally:
        conn.close()

    assert policy_count == len(DEFAULT_POSITION_POLICY_SEEDS)


def test_materialize_position_from_formal_signals_writes_candidate_capacity_and_sizing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    bootstrap_position_ledger(settings=settings)
    summary = materialize_position_from_formal_signals(
        [
            PositionFormalSignalInput(
                signal_nk="sig-001",
                instrument="000001.SZ",
                signal_date="2026-04-08",
                asof_date="2026-04-08",
                trigger_family="PAS",
                trigger_type="bof",
                pattern_code="BOF",
                formal_signal_status="admitted",
                trigger_admissible=True,
                malf_context_4="BULL_MAINSTREAM",
                lifecycle_rank_high=1,
                lifecycle_rank_total=4,
                source_trigger_event_nk="evt-001",
                signal_contract_version="pas-formal-signal-v1",
                reference_trade_date="2026-04-09",
                reference_price=10.0,
                capital_base_value=1_000_000.0,
            )
        ],
        policy_id="fixed_notional_full_exit_v1",
        settings=settings,
        run_id="position-bootstrap-test-run",
    )

    assert summary.candidate_count == 1
    assert summary.admitted_count == 1
    assert summary.family_snapshot_count == 1

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        candidate_row = conn.execute(
            """
            SELECT candidate_status, context_code
            FROM position_candidate_audit
            WHERE candidate_nk = 'sig-001|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        capacity_row = conn.execute(
            """
            SELECT final_allowed_position_weight, required_reduction_weight, capacity_source_code
            FROM position_capacity_snapshot
            WHERE candidate_nk = 'sig-001|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        sizing_row = conn.execute(
            """
            SELECT position_action_decision, target_weight, target_notional, target_shares
            FROM position_sizing_snapshot
            WHERE candidate_nk = 'sig-001|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        family_row = conn.execute(
            """
            SELECT cap_trim_applied, final_target_shares
            FROM position_funding_fixed_notional_snapshot
            WHERE candidate_nk = 'sig-001|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
    finally:
        conn.close()

    assert candidate_row == ("admitted", "BULL_MAINSTREAM")
    assert capacity_row == (0.1875, 0.0, "bootstrap_default_capacity")
    assert sizing_row == ("open_up_to_context_cap", 0.1875, 187500.0, 18700)
    assert family_row == (False, 18700)


def test_materialize_position_from_formal_signals_writes_blocked_single_lot_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    bootstrap_position_ledger(settings=settings)
    summary = materialize_position_from_formal_signals(
        [
            PositionFormalSignalInput(
                signal_nk="sig-002",
                instrument="000002.SZ",
                signal_date="2026-04-08",
                asof_date="2026-04-08",
                trigger_family="PAS",
                trigger_type="tst",
                pattern_code="TST",
                formal_signal_status="blocked",
                trigger_admissible=False,
                malf_context_4="BEAR_MAINSTREAM",
                lifecycle_rank_high=0,
                lifecycle_rank_total=4,
                source_trigger_event_nk="evt-002",
                signal_contract_version="pas-formal-signal-v1",
                reference_trade_date="2026-04-09",
                reference_price=12.5,
                capital_base_value=500_000.0,
                blocked_reason_code="alpha_not_admitted",
            )
        ],
        policy_id="single_lot_full_exit_v1",
        settings=settings,
        run_id="position-bootstrap-test-run-blocked",
    )

    assert summary.blocked_count == 1

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        candidate_row = conn.execute(
            """
            SELECT candidate_status, blocked_reason_code
            FROM position_candidate_audit
            WHERE candidate_nk = 'sig-002|single_lot_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        sizing_row = conn.execute(
            """
            SELECT position_action_decision, final_allowed_position_weight, target_shares
            FROM position_sizing_snapshot
            WHERE candidate_nk = 'sig-002|single_lot_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        family_row = conn.execute(
            """
            SELECT min_lot_size, lot_floor_applied, final_target_shares, fallback_reason_code
            FROM position_funding_single_lot_snapshot
            WHERE candidate_nk = 'sig-002|single_lot_full_exit_v1|2026-04-09'
            """
        ).fetchone()
    finally:
        conn.close()

    assert candidate_row == ("blocked", "alpha_not_admitted")
    assert sizing_row == ("reject_open", 0.0, 0)
    assert family_row == (100, False, 0, None)


def test_materialize_position_from_formal_signals_marks_trim_when_current_position_exceeds_cap(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    bootstrap_position_ledger(settings=settings)
    materialize_position_from_formal_signals(
        [
            PositionFormalSignalInput(
                signal_nk="sig-003",
                instrument="000003.SZ",
                signal_date="2026-04-08",
                asof_date="2026-04-08",
                trigger_family="PAS",
                trigger_type="pb",
                pattern_code="PB",
                formal_signal_status="admitted",
                trigger_admissible=True,
                malf_context_4="BULL_COUNTERTREND",
                lifecycle_rank_high=2,
                lifecycle_rank_total=4,
                source_trigger_event_nk="evt-003",
                signal_contract_version="pas-formal-signal-v1",
                reference_trade_date="2026-04-09",
                reference_price=20.0,
                capital_base_value=1_000_000.0,
                current_position_weight=0.20,
                remaining_single_name_capacity_weight=0.20,
                remaining_portfolio_capacity_weight=0.30,
            )
        ],
        policy_id="fixed_notional_full_exit_v1",
        settings=settings,
        run_id="position-bootstrap-test-run-trim",
    )

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        capacity_row = conn.execute(
            """
            SELECT final_allowed_position_weight, required_reduction_weight
            FROM position_capacity_snapshot
            WHERE candidate_nk = 'sig-003|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
        sizing_row = conn.execute(
            """
            SELECT position_action_decision, target_weight
            FROM position_sizing_snapshot
            WHERE candidate_nk = 'sig-003|fixed_notional_full_exit_v1|2026-04-09'
            """
        ).fetchone()
    finally:
        conn.close()

    assert capacity_row == (0.125, 0.07500000000000001)
    assert sizing_row == ("trim_to_context_cap", 0.125)
