"""覆盖 `Tushare objective source -> profile materialization` 最小正式闭环。"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import duckdb

from mlq.core.paths import default_settings
from mlq.data import (
    OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE,
    OBJECTIVE_PROFILE_MATERIALIZATION_RUN_PROFILE_TABLE,
    RAW_STOCK_DAILY_BAR_TABLE,
    RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE,
    TUSHARE_OBJECTIVE_CHECKPOINT_TABLE,
    TUSHARE_OBJECTIVE_EVENT_TABLE,
    TUSHARE_OBJECTIVE_REQUEST_TABLE,
    bootstrap_raw_market_ledger,
    run_tushare_objective_profile_materialization,
    run_tushare_objective_source_sync,
)
from mlq.data.tushare import (
    TushareNameChangeRow,
    TushareStockBasicRow,
    TushareStockStRow,
    TushareSuspendRow,
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


def _seed_stock_trade_dates(settings) -> None:
    bootstrap_raw_market_ledger(settings)
    connection = duckdb.connect(str(settings.databases.raw_market))
    try:
        source_mtime = datetime(2026, 4, 15, 9, 0, 0)
        connection.execute(
            f"""
            INSERT INTO {RAW_STOCK_DAILY_BAR_TABLE} (
                bar_nk,
                source_file_nk,
                asset_type,
                code,
                name,
                trade_date,
                adjust_method,
                open,
                high,
                low,
                close,
                volume,
                amount,
                source_path,
                source_mtime_utc,
                first_seen_run_id,
                last_ingested_run_id,
                created_at,
                updated_at
            )
            VALUES
                (
                    '000001.SZ|2026-04-08|backward',
                    'seed-file-000001',
                    'stock',
                    '000001.SZ',
                    '平安银行',
                    '2026-04-08',
                    'backward',
                    10.0,
                    10.5,
                    9.8,
                    10.2,
                    1000,
                    10200,
                    'seed',
                    ?,
                    'seed-run',
                    'seed-run',
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                ),
                (
                    '000002.SZ|2026-04-08|backward',
                    'seed-file-000002',
                    'stock',
                    '000002.SZ',
                    '万科A',
                    '2026-04-08',
                    'backward',
                    8.0,
                    8.5,
                    7.9,
                    8.3,
                    1000,
                    8300,
                    'seed',
                    ?,
                    'seed-run',
                    'seed-run',
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                )
            """,
            [source_mtime, source_mtime],
        )
    finally:
        connection.close()


class FakeTushareClient:
    def __init__(
        self,
        *,
        stock_basic_rows: dict[tuple[str, str], tuple[TushareStockBasicRow, ...]],
        suspend_rows: dict[date, tuple[TushareSuspendRow, ...]],
        stock_st_rows: dict[date, tuple[TushareStockStRow, ...]],
        namechange_rows: dict[str, tuple[TushareNameChangeRow, ...]],
    ) -> None:
        self._stock_basic_rows = stock_basic_rows
        self._suspend_rows = suspend_rows
        self._stock_st_rows = stock_st_rows
        self._namechange_rows = namechange_rows
        self.closed = False

    def list_stock_basic(self, exchange: str, list_status: str) -> tuple[TushareStockBasicRow, ...]:
        return self._stock_basic_rows.get((exchange, list_status), ())

    def list_suspend_d(self, trade_date: date) -> tuple[TushareSuspendRow, ...]:
        return self._suspend_rows.get(trade_date, ())

    def list_stock_st(self, trade_date: date) -> tuple[TushareStockStRow, ...]:
        return self._stock_st_rows.get(trade_date, ())

    def list_namechange(self, ts_code: str) -> tuple[TushareNameChangeRow, ...]:
        return self._namechange_rows.get(ts_code, ())

    def close(self) -> None:
        self.closed = True


def _build_fake_client() -> FakeTushareClient:
    return FakeTushareClient(
        stock_basic_rows={
            ("SZSE", "L"): (
                TushareStockBasicRow(
                    ts_code="000001.SZ",
                    name="平安银行",
                    market="主板",
                    exchange="SZSE",
                    list_status="L",
                    list_date=date(1991, 4, 3),
                    delist_date=None,
                ),
                TushareStockBasicRow(
                    ts_code="000002.SZ",
                    name="万科A",
                    market="主板",
                    exchange="SZSE",
                    list_status="L",
                    list_date=date(1991, 1, 29),
                    delist_date=None,
                ),
            ),
        },
        suspend_rows={
            date(2026, 4, 8): (
                TushareSuspendRow(
                    ts_code="000001.SZ",
                    trade_date=date(2026, 4, 8),
                    suspend_timing="全天停牌",
                    suspend_type="S",
                ),
                TushareSuspendRow(
                    ts_code="300001.SZ",
                    trade_date=date(2026, 4, 8),
                    suspend_timing="全天停牌",
                    suspend_type="S",
                ),
            ),
        },
        stock_st_rows={
            date(2026, 4, 8): (
                TushareStockStRow(
                    ts_code="000002.SZ",
                    name="ST万科A",
                    trade_date=date(2026, 4, 8),
                    type="1",
                    type_name="ST",
                ),
                TushareStockStRow(
                    ts_code="300001.SZ",
                    name="ST特锐德",
                    trade_date=date(2026, 4, 8),
                    type="1",
                    type_name="ST",
                ),
            ),
        },
        namechange_rows={
            "000001.SZ": (),
            "000002.SZ": (
                TushareNameChangeRow(
                    ts_code="000002.SZ",
                    name="ST万科A",
                    start_date=date(2026, 4, 1),
                    end_date=date(2026, 4, 30),
                    ann_date=date(2026, 4, 1),
                    change_reason="实施ST",
                ),
                TushareNameChangeRow(
                    ts_code="000002.SZ",
                    name="ST万科A",
                    start_date=date(2026, 4, 1),
                    end_date=date(2026, 4, 30),
                    ann_date=date(2026, 4, 1),
                    change_reason="实施ST",
                ),
            ),
        },
    )


def test_run_tushare_objective_source_sync_records_events_and_checkpoints(tmp_path: Path, monkeypatch) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_stock_trade_dates(settings)

    first_client = _build_fake_client()
    first_summary = run_tushare_objective_source_sync(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        instrument_list=["000001.SZ", "000002.SZ"],
        run_id="tushare-source-001a",
        client_factory=lambda _: first_client,
    )
    second_client = _build_fake_client()
    second_summary = run_tushare_objective_source_sync(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        instrument_list=["000001.SZ", "000002.SZ"],
        run_id="tushare-source-001b",
        client_factory=lambda _: second_client,
    )

    assert first_client.closed is True
    assert second_client.closed is True
    assert first_summary.candidate_cursor_count == 13
    assert first_summary.inserted_event_count == 5
    assert first_summary.failed_request_count == 0
    assert second_summary.reused_event_count == 5
    assert second_summary.failed_request_count == 0

    connection = duckdb.connect(str(settings.databases.raw_market), read_only=True)
    try:
        event_rows = connection.execute(
            f"""
            SELECT code, source_api, objective_dimension
            FROM {TUSHARE_OBJECTIVE_EVENT_TABLE}
            ORDER BY code, source_api, objective_dimension
            """
        ).fetchall()
        request_status_rows = connection.execute(
            f"""
            SELECT run_id, request_status, COUNT(*)
            FROM {TUSHARE_OBJECTIVE_REQUEST_TABLE}
            GROUP BY run_id, request_status
            ORDER BY run_id, request_status
            """
        ).fetchall()
        checkpoint_count = connection.execute(
            f"SELECT COUNT(*) FROM {TUSHARE_OBJECTIVE_CHECKPOINT_TABLE}"
        ).fetchone()[0]
    finally:
        connection.close()

    assert event_rows == [
        ("000001.SZ", "stock_basic", "instrument_metadata"),
        ("000001.SZ", "suspend_d", "suspension_status"),
        ("000002.SZ", "namechange", "risk_warning_status"),
        ("000002.SZ", "stock_basic", "instrument_metadata"),
        ("000002.SZ", "stock_st", "risk_warning_status"),
    ]
    assert request_status_rows == [
        ("tushare-source-001a", "completed", 13),
        ("tushare-source-001b", "skipped_unchanged", 13),
    ]
    assert checkpoint_count == 13


def test_run_tushare_objective_profile_materialization_writes_profiles_and_prefers_stock_st(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    repo_root = _bootstrap_repo_root(tmp_path)
    settings = default_settings(repo_root=repo_root)
    _seed_stock_trade_dates(settings)

    source_client = _build_fake_client()
    run_tushare_objective_source_sync(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        instrument_list=["000001.SZ", "000002.SZ"],
        run_id="tushare-source-002",
        client_factory=lambda _: source_client,
    )

    first_summary = run_tushare_objective_profile_materialization(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        instrument_list=["000001.SZ", "000002.SZ"],
        run_id="tushare-profile-001a",
    )
    second_summary = run_tushare_objective_profile_materialization(
        settings=settings,
        signal_start_date="2026-04-08",
        signal_end_date="2026-04-08",
        instrument_list=["000001.SZ", "000002.SZ"],
        run_id="tushare-profile-001b",
    )

    assert first_summary.inserted_profile_count == 2
    assert second_summary.reused_profile_count == 2

    connection = duckdb.connect(str(settings.databases.raw_market), read_only=True)
    try:
        profile_rows = connection.execute(
            f"""
            SELECT
                code,
                observed_trade_date,
                instrument_name,
                market_type,
                security_type,
                suspension_status,
                risk_warning_status,
                is_suspended_or_unresumed,
                is_risk_warning_excluded,
                source_owner,
                source_run_id,
                source_request_nk,
                source_detail_json
            FROM {RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE}
            ORDER BY code, observed_trade_date
            """
        ).fetchall()
        run_profile_rows = connection.execute(
            f"""
            SELECT run_id, code, materialization_action
            FROM {OBJECTIVE_PROFILE_MATERIALIZATION_RUN_PROFILE_TABLE}
            ORDER BY run_id, code
            """
        ).fetchall()
        checkpoint_rows = connection.execute(
            f"""
            SELECT code, observed_trade_date
            FROM {OBJECTIVE_PROFILE_MATERIALIZATION_CHECKPOINT_TABLE}
            ORDER BY code, observed_trade_date
            """
        ).fetchall()
    finally:
        connection.close()

    assert checkpoint_rows == [
        ("000001.SZ", date(2026, 4, 8)),
        ("000002.SZ", date(2026, 4, 8)),
    ]
    assert run_profile_rows == [
        ("tushare-profile-001a", "000001.SZ", "inserted"),
        ("tushare-profile-001a", "000002.SZ", "inserted"),
        ("tushare-profile-001b", "000001.SZ", "reused"),
        ("tushare-profile-001b", "000002.SZ", "reused"),
    ]

    first_profile = profile_rows[0]
    assert first_profile[:10] == (
        "000001.SZ",
        date(2026, 4, 8),
        "平安银行",
        "sz",
        "stock",
        "suspended",
        None,
        True,
        False,
        "tushare",
    )
    assert first_profile[10] == "tushare-profile-001b"

    second_profile = profile_rows[1]
    assert second_profile[:10] == (
        "000002.SZ",
        date(2026, 4, 8),
        "万科A",
        "sz",
        "stock",
        "trading",
        "st",
        False,
        True,
        "tushare",
    )
    assert second_profile[10] == "tushare-profile-001b"
    assert "stock_st" in second_profile[11]
    source_detail = json.loads(second_profile[12])
    assert source_detail["dimensions"]["risk_warning_status"]["source_api"] == "stock_st"
