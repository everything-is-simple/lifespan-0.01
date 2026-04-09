"""覆盖 `alpha formal signal` 官方 producer 与 `position` 对接。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from mlq.alpha import alpha_ledger_path, run_alpha_formal_signal_build
from mlq.core.paths import default_settings
from mlq.position import position_ledger_path, run_position_formal_signal_materialization


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


def _seed_trigger_source(alpha_path: Path, rows: list[tuple[object, ...]]) -> None:
    alpha_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(alpha_path))
    try:
        conn.execute(
            """
            CREATE TABLE alpha_trigger_event (
                source_trigger_event_nk TEXT NOT NULL,
                instrument TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_date DATE NOT NULL,
                trigger_family TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                pattern_code TEXT NOT NULL
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO alpha_trigger_event VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
    finally:
        conn.close()


def _seed_context_source(malf_path: Path, rows: list[tuple[object, ...]]) -> None:
    malf_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute(
            """
            CREATE TABLE pas_context_snapshot (
                entity_code TEXT NOT NULL,
                signal_date DATE NOT NULL,
                formal_signal_status TEXT NOT NULL,
                filter_trigger_admissible BOOLEAN NOT NULL,
                malf_context_4 TEXT NOT NULL,
                lifecycle_rank_high BIGINT NOT NULL,
                lifecycle_rank_total BIGINT NOT NULL,
                calc_date DATE NOT NULL
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO pas_context_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
    finally:
        conn.close()


def _replace_context_source(malf_path: Path, rows: list[tuple[object, ...]]) -> None:
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute("DELETE FROM pas_context_snapshot")
        for row in rows:
            conn.execute(
                """
                INSERT INTO pas_context_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
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
                close DOUBLE
            )
            """
        )
        for row in rows:
            conn.execute("INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, ?)", row)
    finally:
        conn.close()


def test_run_alpha_formal_signal_build_materializes_run_event_and_run_bridge(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_trigger_source(
        settings.databases.alpha,
        [
            ("evt-001", "000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
            ("evt-002", "000002.SZ", "2026-04-08", "2026-04-08", "PAS", "pb", "PB"),
        ],
    )
    _seed_context_source(
        settings.databases.malf,
        [
            ("000001.SZ", "2026-04-08", "admitted", True, "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
            ("000002.SZ", "2026-04-08", "blocked", False, "BEAR_MAINSTREAM", 0, 4, "2026-04-08"),
        ],
    )

    summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        batch_size=1,
        run_id="alpha-formal-signal-test-001",
    )

    assert summary.candidate_trigger_count == 2
    assert summary.materialized_signal_count == 2
    assert summary.inserted_count == 2
    assert summary.admitted_count == 1
    assert summary.blocked_count == 1

    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_status, bounded_instrument_count
            FROM alpha_formal_signal_run
            WHERE run_id = 'alpha-formal-signal-test-001'
            """
        ).fetchone()
        event_rows = conn.execute(
            """
            SELECT instrument, pattern_code, formal_signal_status, trigger_admissible
            FROM alpha_formal_signal_event
            ORDER BY instrument
            """
        ).fetchall()
        run_event_rows = conn.execute(
            """
            SELECT signal_nk, materialization_action
            FROM alpha_formal_signal_run_event
            WHERE run_id = 'alpha-formal-signal-test-001'
            ORDER BY signal_nk
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", 2)
    assert event_rows == [
        ("000001.SZ", "BOF", "admitted", True),
        ("000002.SZ", "PB", "blocked", False),
    ]
    assert len(run_event_rows) == 2
    assert {row[1] for row in run_event_rows} == {"inserted"}


def test_run_alpha_formal_signal_build_marks_rematerialized_when_context_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_trigger_source(
        settings.databases.alpha,
        [
            ("evt-101", "000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
        ],
    )
    _seed_context_source(
        settings.databases.malf,
        [
            ("000001.SZ", "2026-04-08", "admitted", True, "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
        ],
    )

    first_summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-formal-signal-test-002a",
    )
    assert first_summary.inserted_count == 1

    _replace_context_source(
        settings.databases.malf,
        [
            ("000001.SZ", "2026-04-08", "blocked", False, "BEAR_MAINSTREAM", 0, 4, "2026-04-09"),
        ],
    )

    second_summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-formal-signal-test-002b",
    )

    assert second_summary.rematerialized_count == 1
    assert second_summary.blocked_count == 1

    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        event_row = conn.execute(
            """
            SELECT formal_signal_status, trigger_admissible, last_materialized_run_id
            FROM alpha_formal_signal_event
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
        run_event_row = conn.execute(
            """
            SELECT materialization_action
            FROM alpha_formal_signal_run_event
            WHERE run_id = 'alpha-formal-signal-test-002b'
            """
        ).fetchone()
    finally:
        conn.close()

    assert event_row == ("blocked", False, "alpha-formal-signal-test-002b")
    assert run_event_row == ("rematerialized",)


def test_run_alpha_formal_signal_build_outputs_event_consumable_by_position_runner(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_trigger_source(
        settings.databases.alpha,
        [
            ("evt-201", "000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
        ],
    )
    _seed_context_source(
        settings.databases.malf,
        [
            ("000001.SZ", "2026-04-08", "admitted", True, "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
        ],
    )
    _seed_market_base_prices(
        settings.databases.market_base,
        [
            ("000001.SZ", "2026-04-09", "backward", 10.5),
        ],
    )

    alpha_summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-formal-signal-test-003",
    )
    position_summary = run_position_formal_signal_materialization(
        policy_id="fixed_notional_full_exit_v1",
        capital_base_value=1_000_000.0,
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        run_id="position-runner-test-101",
    )

    assert alpha_summary.materialized_signal_count == 1
    assert position_summary.alpha_signal_count == 1
    assert position_summary.enriched_signal_count == 1

    conn = duckdb.connect(str(position_ledger_path(settings)), read_only=True)
    try:
        candidate_row = conn.execute(
            """
            SELECT instrument, source_signal_run_id
            FROM position_candidate_audit
            WHERE source_signal_run_id = 'alpha-formal-signal-test-003'
            """
        ).fetchone()
    finally:
        conn.close()

    assert candidate_row == ("000001.SZ", "alpha-formal-signal-test-003")
