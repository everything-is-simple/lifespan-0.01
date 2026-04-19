"""主线本地正式库增量同步的 source-state / checkpoint / report 辅助。"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from mlq.core.paths import WorkspaceRoots


MAINLINE_LEDGER_LATEST_BAR_DATE_CANDIDATES: dict[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "raw_market": (
        ("stock_daily_bar", ("trade_date",)),
        ("index_daily_bar", ("trade_date",)),
        ("block_daily_bar", ("trade_date",)),
    ),
    "market_base": (
        ("stock_daily_adjusted", ("trade_date",)),
        ("index_daily_adjusted", ("trade_date",)),
        ("block_daily_adjusted", ("trade_date",)),
    ),
    "malf": (
        ("malf_state_snapshot", ("asof_bar_dt",)),
        ("malf_wave_life_snapshot", ("asof_bar_dt",)),
        ("malf_wave_ledger", ("end_bar_dt", "start_bar_dt")),
    ),
    "structure": (
        ("structure_snapshot", ("signal_date", "asof_date")),
        ("structure_run", ("signal_end_date", "signal_start_date")),
    ),
    "filter": (
        ("filter_snapshot", ("signal_date", "asof_date")),
        ("filter_run", ("signal_end_date", "signal_start_date")),
    ),
    "alpha": (
        ("alpha_formal_signal_event", ("signal_date", "asof_date")),
        ("alpha_family_event", ("signal_date", "asof_date")),
        ("alpha_trigger_event", ("signal_date", "asof_date")),
    ),
    "position": (
        ("position_candidate_audit", ("reference_trade_date",)),
        ("position_sizing_snapshot", ("reference_trade_date",)),
    ),
    "portfolio_plan": (("portfolio_plan_snapshot", ("reference_trade_date",)),),
    "trade_runtime": (
        ("trade_execution_plan", ("planned_entry_trade_date", "signal_date")),
        ("trade_position_leg", ("entry_trade_date",)),
        ("trade_carry_snapshot", ("snapshot_date",)),
    ),
    "system": (
        ("system_mainline_snapshot", ("snapshot_date",)),
        ("system_run", ("snapshot_date",)),
    ),
}


def resolve_source_state(
    *,
    ledger_name: str,
    source_path: Path,
    explicit_latest_bar_dt: date | None,
) -> dict[str, object]:
    """统一解析 source 是否存在、最新 bar 日期与稳定 fingerprint。"""

    source_exists = source_path.exists()
    latest_bar_dt = explicit_latest_bar_dt
    if source_exists and latest_bar_dt is None:
        latest_bar_dt = detect_latest_bar_date(
            ledger_name=ledger_name,
            database_path=source_path,
        )
    return {
        "source_exists": source_exists,
        "source_latest_bar_dt": latest_bar_dt,
        "source_fingerprint": build_source_fingerprint(
            source_path=source_path,
            source_latest_bar_dt=latest_bar_dt,
        ),
    }


def detect_latest_bar_date(*, ledger_name: str, database_path: Path) -> date | None:
    """从正式 ledger 中探测给定账本的最新业务日期。"""

    if not database_path.exists():
        return None
    candidates = MAINLINE_LEDGER_LATEST_BAR_DATE_CANDIDATES.get(ledger_name, ())
    if not candidates:
        return None
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        latest_values: list[date] = []
        for table_name, column_names in candidates:
            available_columns = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'main'
                      AND table_name = ?
                    """,
                    [table_name],
                ).fetchall()
            }
            if not available_columns:
                continue
            for column_name in column_names:
                if column_name not in available_columns:
                    continue
                value = connection.execute(
                    f'SELECT MAX(CAST("{column_name}" AS DATE)) FROM "{table_name}"'
                ).fetchone()
                candidate = to_date(None if value is None else value[0])
                if candidate is not None:
                    latest_values.append(candidate)
        return max(latest_values) if latest_values else None
    finally:
        connection.close()


def build_source_fingerprint(
    *,
    source_path: Path,
    source_latest_bar_dt: date | None,
) -> str:
    """构建 source 账本的稳定 fingerprint。"""

    if not source_path.exists():
        return json.dumps(
            {
                "exists": False,
                "path": str(source_path),
                "source_latest_bar_dt": dump_date(source_latest_bar_dt),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    stat = source_path.stat()
    return json.dumps(
        {
            "exists": True,
            "path": str(source_path),
            "size_bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
            "source_latest_bar_dt": dump_date(source_latest_bar_dt),
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def normalize_date_map(raw_map: dict[str, str | date] | None) -> dict[str, date]:
    """把 CLI/调用方提供的日期映射规整成稳定 `date`。"""

    normalized: dict[str, date] = {}
    for ledger_name, raw_value in (raw_map or {}).items():
        normalized[ledger_name] = to_date(raw_value)  # type: ignore[assignment]
    return {key: value for key, value in normalized.items() if value is not None}


def to_date(value: object) -> date | None:
    """把 DuckDB/CLI 读出的日期值规整成 `date`。"""

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def dump_date(value: date | None) -> str | None:
    """稳定序列化 `date`。"""

    return None if value is None else value.isoformat()


def load_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    checkpoint_table: str,
    ledger_name: str,
) -> dict[str, object] | None:
    """读取主链 ledger checkpoint 条目。"""

    row = connection.execute(
        f"""
        SELECT last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint
        FROM {checkpoint_table}
        WHERE ledger_name = ?
        """,
        [ledger_name],
    ).fetchone()
    if row is None:
        return None
    return {
        "last_completed_bar_dt": to_date(row[0]),
        "tail_start_bar_dt": to_date(row[1]),
        "tail_confirm_until_dt": to_date(row[2]),
        "source_fingerprint": "" if row[3] is None else str(row[3]),
    }


def derive_dirty_reason(
    *,
    checkpoint: dict[str, object] | None,
    same_path: bool,
    source_exists: bool,
    target_exists: bool,
    source_latest_bar_dt: date | None,
    replay_start_bar_dt: date | None,
    replay_confirm_until_dt: date | None,
    source_fingerprint: str,
) -> str | None:
    """根据 source/checkpoint 状态推导是否需要重放同步。"""

    if not source_exists:
        return "bootstrap_missing_target" if same_path and not target_exists else None
    if not target_exists:
        return "target_missing"
    if checkpoint is None:
        return "bootstrap_missing_checkpoint"
    if str(checkpoint["source_fingerprint"]) != source_fingerprint:
        return "source_fingerprint_changed"
    last_completed_bar_dt = to_date(checkpoint["last_completed_bar_dt"])
    if last_completed_bar_dt is None:
        return "checkpoint_incomplete"
    if source_latest_bar_dt is not None and source_latest_bar_dt > last_completed_bar_dt:
        return "source_advanced"
    tail_start_bar_dt = to_date(checkpoint["tail_start_bar_dt"])
    if replay_start_bar_dt is not None and (tail_start_bar_dt is None or replay_start_bar_dt < tail_start_bar_dt):
        return "source_replayed"
    tail_confirm_until_dt = to_date(checkpoint["tail_confirm_until_dt"])
    if replay_confirm_until_dt is not None and (
        tail_confirm_until_dt is None or replay_confirm_until_dt > tail_confirm_until_dt
    ):
        return "tail_confirm_advanced"
    return None


def upsert_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    checkpoint_table: str,
    ledger_name: str,
    module_name: str,
    source_path: Path,
    target_path: Path,
    last_completed_bar_dt: date | None,
    tail_start_bar_dt: date | None,
    tail_confirm_until_dt: date | None,
    source_fingerprint: str,
    last_run_id: str,
) -> None:
    """回写主链 ledger checkpoint。"""

    connection.execute(
        f"""
        INSERT OR REPLACE INTO {checkpoint_table} (
            ledger_name, module_name, source_path, target_path, last_completed_bar_dt,
            tail_start_bar_dt, tail_confirm_until_dt, source_fingerprint, last_run_id, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            ledger_name,
            module_name,
            str(source_path),
            str(target_path),
            last_completed_bar_dt,
            tail_start_bar_dt,
            tail_confirm_until_dt,
            source_fingerprint,
            last_run_id,
        ],
    )


def resolve_tail_dates(
    *,
    source_latest_bar_dt: date | None,
    replay_start_bar_dt: date | None,
    replay_confirm_until_dt: date | None,
) -> tuple[date | None, date | None, date | None]:
    """统一解析 last_completed / tail range。"""

    last_completed_bar_dt = replay_confirm_until_dt or source_latest_bar_dt
    tail_start_bar_dt = replay_start_bar_dt or last_completed_bar_dt
    tail_confirm_until_dt = replay_confirm_until_dt or last_completed_bar_dt
    return last_completed_bar_dt, tail_start_bar_dt, tail_confirm_until_dt


def write_reports(
    *,
    settings: WorkspaceRoots,
    run_id: str,
    sync_results: list[Any],
    freshness_rows: list[Any],
    summary_path: Path | None,
) -> tuple[Path, Path]:
    """写出 incremental sync JSON/Markdown 报告。"""

    report_root = settings.module_report_root("data") / "mainline_local_ledger_incremental_sync"
    report_root.mkdir(parents=True, exist_ok=True)
    report_json_path = (summary_path or (report_root / f"{run_id}.json")).resolve()
    report_json_path.parent.mkdir(parents=True, exist_ok=True)
    report_markdown_path = report_json_path.with_suffix(".md")
    generated_at_utc = datetime.utcnow().isoformat()
    payload = {
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "sync_results": [row.as_dict() for row in sync_results],
        "freshness_rows": [row.as_dict() for row in freshness_rows],
    }
    report_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# mainline local ledger incremental sync",
        "",
        f"- run_id: `{run_id}`",
        f"- generated_at_utc: `{generated_at_utc}`",
        "",
        "## sync_results",
    ]
    for row in sync_results:
        lines.append(
            "- "
            + f"`{row.ledger_name}` dirty_reason={row.dirty_reason} | sync_action={row.sync_action} | "
            + f"last_completed_bar_dt={dump_date(row.last_completed_bar_dt)} | "
            + f"tail_start_bar_dt={dump_date(row.tail_start_bar_dt)} | "
            + f"tail_confirm_until_dt={dump_date(row.tail_confirm_until_dt)}"
        )
    lines.extend(["", "## freshness"])
    for row in freshness_rows:
        lines.append(
            "- "
            + f"`{row.ledger_name}` freshness_status={row.freshness_status} | "
            + f"source_latest_bar_dt={dump_date(row.source_latest_bar_dt)} | "
            + f"last_completed_bar_dt={dump_date(row.last_completed_bar_dt)} | "
            + f"freshness_lag_days={row.freshness_lag_days}"
        )
    report_markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_json_path, report_markdown_path
