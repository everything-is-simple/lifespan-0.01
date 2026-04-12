"""覆盖 `alpha trigger / formal signal` 官方 producer 与 `position` 对接。"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mlq.alpha import alpha_ledger_path, run_alpha_formal_signal_build, run_alpha_trigger_build
from mlq.core.paths import default_settings
from mlq.filter import run_filter_snapshot_build
from mlq.position import position_ledger_path, run_position_formal_signal_materialization
from mlq.structure import run_structure_snapshot_build


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
            CREATE TABLE alpha_trigger_candidate (
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
            conn.execute("INSERT INTO alpha_trigger_candidate VALUES (?, ?, ?, ?, ?, ?)", row)
    finally:
        conn.close()


def _seed_malf_sources(
    malf_path: Path,
    *,
    context_rows: list[tuple[object, ...]],
    structure_rows: list[tuple[object, ...]],
    higher_timeframe_rows: list[tuple[object, ...]] | None = None,
) -> None:
    malf_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute(
            """
            CREATE TABLE pas_context_snapshot (
                entity_code TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_date DATE NOT NULL,
                source_context_nk TEXT NOT NULL,
                malf_context_4 TEXT NOT NULL,
                lifecycle_rank_high BIGINT NOT NULL,
                lifecycle_rank_total BIGINT NOT NULL,
                calc_date DATE NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE malf_state_snapshot (
                snapshot_nk TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                code TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                asof_bar_dt DATE NOT NULL,
                major_state TEXT NOT NULL,
                trend_direction TEXT NOT NULL,
                reversal_stage TEXT NOT NULL,
                wave_id BIGINT NOT NULL,
                current_hh_count BIGINT NOT NULL,
                current_ll_count BIGINT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE structure_candidate_snapshot (
                instrument TEXT NOT NULL,
                signal_date DATE NOT NULL,
                asof_date DATE NOT NULL,
                new_high_count BIGINT NOT NULL,
                new_low_count BIGINT NOT NULL,
                refresh_density DOUBLE NOT NULL,
                advancement_density DOUBLE NOT NULL,
                is_failed_extreme BOOLEAN NOT NULL,
                failure_type TEXT
            )
            """
        )
        for row in context_rows:
            conn.execute("INSERT INTO pas_context_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row)
            context_code = str(row[4])
            lifecycle_rank_high = int(row[5])
            if context_code == "BULL_MAINSTREAM":
                major_state, trend_direction, reversal_stage, current_hh_count, current_ll_count = "牛顺", "up", "none", lifecycle_rank_high, 0
            elif context_code == "BEAR_MAINSTREAM":
                major_state, trend_direction, reversal_stage, current_hh_count, current_ll_count = "熊顺", "down", "none", 0, lifecycle_rank_high
            elif context_code == "BULL_COUNTERTREND":
                major_state, trend_direction, reversal_stage, current_hh_count, current_ll_count = "熊逆", "up", "trigger", lifecycle_rank_high, 0
            else:
                major_state, trend_direction, reversal_stage, current_hh_count, current_ll_count = "牛逆", "down", "trigger", 0, lifecycle_rank_high
            conn.execute(
                """
                INSERT INTO malf_state_snapshot VALUES (?, 'stock', ?, 'D', ?, ?, ?, ?, 0, ?, ?)
                """,
                [
                    f"state-{row[0]}-{row[1]}",
                    row[0],
                    row[2],
                    major_state,
                    trend_direction,
                    reversal_stage,
                    current_hh_count,
                    current_ll_count,
                ],
            )
        for row in higher_timeframe_rows or []:
            conn.execute(
                """
                INSERT INTO malf_state_snapshot VALUES (?, 'stock', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        for row in structure_rows:
            conn.execute("INSERT INTO structure_candidate_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)
    finally:
        conn.close()


def _replace_structure_source(malf_path: Path, rows: list[tuple[object, ...]]) -> None:
    conn = duckdb.connect(str(malf_path))
    try:
        conn.execute("DELETE FROM structure_candidate_snapshot")
        for row in rows:
            conn.execute("INSERT INTO structure_candidate_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)
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


def _materialize_official_upstream(settings, *, suffix: str) -> None:
    run_structure_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id=f"structure-upstream-alpha-test-{suffix}",
        source_structure_input_table="structure_candidate_snapshot",
    )
    run_filter_snapshot_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id=f"filter-upstream-alpha-test-{suffix}",
    )


def _materialize_official_trigger(settings, *, suffix: str) -> None:
    run_alpha_trigger_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id=f"alpha-trigger-upstream-test-{suffix}",
    )


def test_run_alpha_trigger_build_materializes_run_event_and_official_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_trigger_source(
        settings.databases.alpha,
        [
            ("000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
            ("000002.SZ", "2026-04-08", "2026-04-08", "PAS", "pb", "PB"),
        ],
    )
    _seed_malf_sources(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-010", "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
            ("000002.SZ", "2026-04-08", "2026-04-08", "ctx-020", "BEAR_MAINSTREAM", 0, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.7, 0.6, False, None),
            ("000002.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme"),
        ],
        higher_timeframe_rows=[
            ("state-w-001", "000001.SZ", "W", "2026-04-03", "牛顺", "up", "none", 3, 1, 0),
            ("state-m-001", "000001.SZ", "M", "2026-03-31", "牛逆", "down", "trigger", 1, 0, 1),
        ],
    )
    _materialize_official_upstream(settings, suffix="trigger-001")

    summary = run_alpha_trigger_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        limit=10,
        batch_size=1,
        run_id="alpha-trigger-test-001",
    )

    assert summary.candidate_trigger_count == 2
    assert summary.materialized_trigger_count == 2
    assert summary.inserted_count == 2

    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        run_row = conn.execute(
            """
            SELECT run_status, bounded_instrument_count, candidate_trigger_count
            FROM alpha_trigger_run
            WHERE run_id = 'alpha-trigger-test-001'
            """
        ).fetchone()
        event_rows = conn.execute(
            """
            SELECT
                instrument,
                trigger_type,
                daily_source_context_nk,
                weekly_major_state,
                monthly_major_state,
                source_filter_snapshot_nk,
                source_structure_snapshot_nk
            FROM alpha_trigger_event
            ORDER BY instrument
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", 2, 2)
    assert event_rows == [
        ("000001.SZ", "bof", "state-000001.SZ-2026-04-08", "牛顺", "牛逆", event_rows[0][5], event_rows[0][6]),
        ("000002.SZ", "pb", "state-000002.SZ-2026-04-08", None, None, event_rows[1][5], event_rows[1][6]),
    ]
    assert all(row[5] and row[6] for row in event_rows)


def test_run_alpha_trigger_build_marks_reused_and_rematerialized_when_upstream_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_trigger_source(
        settings.databases.alpha,
        [
            ("000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
        ],
    )
    _seed_malf_sources(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-110", "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.8, 0.7, False, None),
        ],
    )
    _materialize_official_upstream(settings, suffix="trigger-002a")

    first_summary = run_alpha_trigger_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-trigger-test-002a",
    )
    second_summary = run_alpha_trigger_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-trigger-test-002b",
    )

    assert first_summary.inserted_count == 1
    assert second_summary.reused_count == 1

    _replace_structure_source(
        settings.databases.malf,
        [("000001.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme")],
    )
    _materialize_official_upstream(settings, suffix="trigger-002c")
    third_summary = run_alpha_trigger_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-trigger-test-002c",
    )

    assert third_summary.rematerialized_count == 1

    conn = duckdb.connect(str(alpha_ledger_path(settings)), read_only=True)
    try:
        event_row = conn.execute(
            """
            SELECT last_materialized_run_id, upstream_context_fingerprint
            FROM alpha_trigger_event
            WHERE instrument = '000001.SZ'
            """
        ).fetchone()
    finally:
        conn.close()

    assert event_row[0] == "alpha-trigger-test-002c"
    assert '"structure_progress_state": "failed"' in event_row[1]


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
            ("000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
            ("000002.SZ", "2026-04-08", "2026-04-08", "PAS", "pb", "PB"),
        ],
    )
    _seed_malf_sources(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-001", "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
            ("000002.SZ", "2026-04-08", "2026-04-08", "ctx-002", "BEAR_MAINSTREAM", 0, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.7, 0.6, False, None),
            ("000002.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme"),
        ],
        higher_timeframe_rows=[
            ("state-w-101", "000001.SZ", "W", "2026-04-03", "牛顺", "up", "none", 3, 1, 0),
            ("state-m-101", "000001.SZ", "M", "2026-03-31", "牛逆", "down", "trigger", 1, 0, 1),
        ],
    )
    _materialize_official_upstream(settings, suffix="001")
    _materialize_official_trigger(settings, suffix="001")

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
            SELECT
                instrument,
                pattern_code,
                formal_signal_status,
                trigger_admissible,
                major_state,
                reversal_stage,
                daily_source_context_nk,
                weekly_major_state,
                monthly_major_state
            FROM alpha_formal_signal_event
            ORDER BY instrument
            """
        ).fetchall()
    finally:
        conn.close()

    assert run_row == ("completed", 2)
    assert event_rows == [
        ("000001.SZ", "BOF", "admitted", True, "牛顺", "none", "state-000001.SZ-2026-04-08", "牛顺", "牛逆"),
        ("000002.SZ", "PB", "blocked", False, "熊顺", "none", "state-000002.SZ-2026-04-08", None, None),
    ]


def test_run_alpha_formal_signal_build_marks_rematerialized_when_official_upstream_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)

    _seed_trigger_source(
        settings.databases.alpha,
        [
            ("000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
        ],
    )
    _seed_malf_sources(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-101", "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 1, 0, 0.4, 0.3, False, None),
        ],
    )
    _materialize_official_upstream(settings, suffix="002a")
    _materialize_official_trigger(settings, suffix="002a")

    first_summary = run_alpha_formal_signal_build(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        run_id="alpha-formal-signal-test-002a",
    )
    assert first_summary.inserted_count == 1

    _replace_structure_source(
        settings.databases.malf,
        [("000001.SZ", "2026-04-08", "2026-04-08", 0, 1, 0.0, 0.0, True, "failed_extreme")],
    )
    _materialize_official_upstream(settings, suffix="002b")
    _materialize_official_trigger(settings, suffix="002b")
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
    finally:
        conn.close()

    assert event_row == ("blocked", False, "alpha-formal-signal-test-002b")


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
            ("000001.SZ", "2026-04-08", "2026-04-08", "PAS", "bof", "BOF"),
        ],
    )
    _seed_malf_sources(
        settings.databases.malf,
        context_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", "ctx-201", "BULL_MAINSTREAM", 1, 4, "2026-04-08"),
        ],
        structure_rows=[
            ("000001.SZ", "2026-04-08", "2026-04-08", 2, 0, 0.8, 0.7, False, None),
        ],
    )
    _seed_market_base_prices(
        settings.databases.market_base,
        [("000001.SZ", "2026-04-09", "none", 10.5)],
    )
    _materialize_official_upstream(settings, suffix="003")
    _materialize_official_trigger(settings, suffix="003")

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
