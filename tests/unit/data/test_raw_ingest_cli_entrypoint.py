"""覆盖 `run_tdx_asset_raw_ingest.py` 的 CLI 续跑入口。"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "data"
    / "run_tdx_asset_raw_ingest.py"
)


def _load_cli_module():
    spec = importlib.util.spec_from_file_location("data_raw_ingest_cli_entrypoint", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_raw_ingest_cli_accepts_pending_only_from_registry(monkeypatch) -> None:
    module = _load_cli_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "--asset-type",
            "stock",
            "--timeframe",
            "week",
            "--pending-only-from-registry",
        ],
    )

    args = module.parse_args()

    assert args.pending_only_from_registry is True


def test_raw_ingest_cli_uses_registry_pending_scope_for_batched_run(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    module = _load_cli_module()
    summary_path = tmp_path / "summary.json"
    startup_scope = {
        "asset_type": "stock",
        "timeframe": "week",
        "adjust_method": "backward",
        "source_root": "H:/tdx_offline_Data",
        "source_folder": "H:/tdx_offline_Data/stock-day/Backward-Adjusted",
        "source_timeframe": "day",
        "raw_market_path": "H:/Lifespan-data/raw_market.duckdb",
        "candidate_instrument_count": 5,
        "existing_instrument_count": 3,
        "pending_instrument_count": 2,
        "candidate_instruments": ("000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"),
        "existing_instruments": ("000001.SZ", "000002.SZ", "000003.SZ"),
        "pending_instruments": ("000004.SZ", "000005.SZ"),
    }
    called: dict[str, object] = {}

    def _fake_scope(**kwargs):
        called["scope_kwargs"] = kwargs
        return startup_scope

    def _fake_batched(**kwargs):
        called["runner_kwargs"] = kwargs
        return {
            "run_id": "raw-stock-week-batched-test",
            "asset_type": "stock",
            "timeframe": "week",
            "adjust_method": "backward",
            "batch_size": 50,
            "batch_count": 1,
            "candidate_file_count": 2,
            "processed_file_count": 2,
            "ingested_file_count": 2,
            "skipped_unchanged_file_count": 0,
            "failed_file_count": 0,
            "bar_inserted_count": 20,
            "bar_reused_count": 0,
            "bar_rematerialized_count": 0,
            "child_runs": [],
        }

    monkeypatch.setattr(module, "resolve_tdx_asset_pending_registry_scope", _fake_scope)
    monkeypatch.setattr(module, "run_tdx_asset_raw_ingest_batched", _fake_batched)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "--asset-type",
            "stock",
            "--timeframe",
            "week",
            "--pending-only-from-registry",
            "--batch-size",
            "50",
            "--summary-path",
            str(summary_path),
        ],
    )

    module.main()

    stdout_lines = capsys.readouterr().out.strip().splitlines()
    assert json.loads(stdout_lines[0]) == {
        "started_at": json.loads(stdout_lines[0])["started_at"],
        "asset_type": "stock",
        "timeframe": "week",
        "source_timeframe": "day",
        "total_codes": 5,
        "existing_codes": 3,
        "pending_codes": 2,
        "batch_size": 50,
        "pending_only_from_registry": True,
    }
    assert called["scope_kwargs"] == {
        "asset_type": "stock",
        "timeframe": "week",
        "source_root": Path("H:/tdx_offline_Data"),
        "adjust_method": "backward",
        "instruments": [],
    }
    assert called["runner_kwargs"] == {
        "asset_type": "stock",
        "timeframe": "week",
        "source_root": Path("H:/tdx_offline_Data"),
        "adjust_method": "backward",
        "run_mode": "incremental",
        "force_hash": False,
        "instruments": ["000004.SZ", "000005.SZ"],
        "batch_size": 50,
        "run_id": None,
        "summary_path": summary_path,
    }
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["pending_only_from_registry"] is True
    assert summary_payload["timeframe"] == "week"
    assert summary_payload["candidate_file_count"] == 2


def test_raw_ingest_cli_skips_runner_when_pending_scope_is_empty(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    module = _load_cli_module()
    summary_path = tmp_path / "summary.json"
    startup_scope = {
        "asset_type": "stock",
        "timeframe": "week",
        "adjust_method": "backward",
        "source_root": "H:/tdx_offline_Data",
        "source_folder": "H:/tdx_offline_Data/stock-day/Backward-Adjusted",
        "source_timeframe": "day",
        "raw_market_path": "H:/Lifespan-data/raw_market.duckdb",
        "candidate_instrument_count": 5,
        "existing_instrument_count": 5,
        "pending_instrument_count": 0,
        "candidate_instruments": ("000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"),
        "existing_instruments": ("000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"),
        "pending_instruments": (),
    }

    def _fake_scope(**kwargs):
        return startup_scope

    def _unexpected_runner(**kwargs):
        raise AssertionError("batched runner should not be called when pending scope is empty")

    monkeypatch.setattr(module, "resolve_tdx_asset_pending_registry_scope", _fake_scope)
    monkeypatch.setattr(module, "run_tdx_asset_raw_ingest_batched", _unexpected_runner)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "--asset-type",
            "stock",
            "--timeframe",
            "week",
            "--pending-only-from-registry",
            "--batch-size",
            "50",
            "--summary-path",
            str(summary_path),
        ],
    )

    module.main()

    stdout_lines = capsys.readouterr().out.strip().splitlines()
    startup_payload = json.loads(stdout_lines[0])
    assert startup_payload["pending_codes"] == 0
    stdout_summary_payload = json.loads("\n".join(stdout_lines[1:]))
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert stdout_summary_payload == summary_payload
    assert summary_payload["pending_only_from_registry"] is True
    assert summary_payload["candidate_file_count"] == 0
    assert summary_payload["batch_count"] == 0
    assert summary_payload["child_runs"] == []
