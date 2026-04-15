"""`filter` 客观状态覆盖率只读审计。"""

from __future__ import annotations

import json
from bisect import bisect_right
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE
from mlq.filter.bootstrap import FILTER_SNAPSHOT_TABLE, filter_ledger_path
from mlq.filter.filter_shared import _coerce_date, _normalize_date_value


@dataclass(frozen=True)
class FilterObjectiveCoverageBucket:
    bucket_key: str
    missing_count: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class FilterObjectiveCoverageAuditSummary:
    generated_at_utc: str
    filter_path: str
    raw_market_path: str
    filter_table: str
    objective_table: str
    filter_snapshot_count: int
    objective_profile_table_present: bool
    objective_profile_row_count: int
    objective_profile_instrument_count: int
    filter_signal_start_date: str | None
    filter_signal_end_date: str | None
    covered_objective_count: int
    missing_objective_count: int
    missing_ratio: float
    suggested_backfill_start_date: str | None
    suggested_backfill_end_date: str | None
    suggested_backfill_reason: str | None
    top_missing_by_signal_date: tuple[FilterObjectiveCoverageBucket, ...]
    top_missing_by_instrument: tuple[FilterObjectiveCoverageBucket, ...]
    top_missing_by_market_type: tuple[FilterObjectiveCoverageBucket, ...]

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["top_missing_by_signal_date"] = [row.as_dict() for row in self.top_missing_by_signal_date]
        payload["top_missing_by_instrument"] = [row.as_dict() for row in self.top_missing_by_instrument]
        payload["top_missing_by_market_type"] = [row.as_dict() for row in self.top_missing_by_market_type]
        return payload


@dataclass(frozen=True)
class _FilterAuditRow:
    instrument: str
    signal_date: date


@dataclass(frozen=True)
class _ObjectiveProfileRow:
    instrument: str
    observed_trade_date: date
    market_type: str | None


def run_filter_objective_coverage_audit(
    *,
    settings: WorkspaceRoots | None = None,
    filter_path: Path | None = None,
    raw_market_path: Path | None = None,
    filter_table: str = FILTER_SNAPSHOT_TABLE,
    objective_table: str = RAW_TDXQUANT_INSTRUMENT_PROFILE_TABLE,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    group_limit: int = 50,
    summary_path: Path | None = None,
    report_path: Path | None = None,
) -> FilterObjectiveCoverageAuditSummary:
    """审计 `filter_snapshot` 对 objective profile 的历史覆盖率。"""

    workspace = settings or default_settings()
    resolved_filter_path = Path(filter_path or filter_ledger_path(workspace))
    resolved_raw_market_path = Path(raw_market_path or workspace.databases.raw_market)
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_group_limit = max(int(group_limit), 1)

    if not resolved_filter_path.exists():
        raise FileNotFoundError(f"Missing filter database: {resolved_filter_path}")

    filter_rows = _load_filter_audit_rows(
        filter_path=resolved_filter_path,
        table_name=filter_table,
        signal_start_date=normalized_start_date,
        signal_end_date=normalized_end_date,
    )
    objective_table_present = resolved_raw_market_path.exists() and _table_exists(
        resolved_raw_market_path,
        objective_table,
    )
    objective_rows = _load_objective_profile_rows(
        raw_market_path=resolved_raw_market_path,
        table_name=objective_table,
        instruments=tuple(sorted({row.instrument for row in filter_rows})),
        signal_end_date=normalized_end_date,
    )
    objective_dates_by_instrument = {
        instrument: [row.observed_trade_date for row in rows]
        for instrument, rows in objective_rows.items()
    }

    missing_by_signal_date: Counter[str] = Counter()
    missing_by_instrument: Counter[str] = Counter()
    missing_by_market_type: Counter[str] = Counter()
    missing_dates: list[date] = []
    covered_objective_count = 0

    for filter_row in filter_rows:
        matched_profile = _resolve_objective_profile_for_signal(
            objective_rows,
            objective_dates_by_instrument,
            instrument=filter_row.instrument,
            signal_date=filter_row.signal_date,
        )
        if matched_profile is not None:
            covered_objective_count += 1
            continue
        missing_dates.append(filter_row.signal_date)
        missing_by_signal_date[filter_row.signal_date.isoformat()] += 1
        missing_by_instrument[filter_row.instrument] += 1
        missing_by_market_type[_infer_market_type(filter_row.instrument)] += 1

    filter_signal_start = min((row.signal_date for row in filter_rows), default=None)
    filter_signal_end = max((row.signal_date for row in filter_rows), default=None)
    missing_objective_count = len(missing_dates)
    suggested_backfill_start_date = min(missing_dates, default=None)
    suggested_backfill_end_date = max(missing_dates, default=None)
    suggested_backfill_reason = _build_backfill_reason(
        objective_table_present=objective_table_present,
        objective_profile_row_count=sum(len(rows) for rows in objective_rows.values()),
        missing_objective_count=missing_objective_count,
        suggested_backfill_start_date=suggested_backfill_start_date,
        suggested_backfill_end_date=suggested_backfill_end_date,
    )

    summary = FilterObjectiveCoverageAuditSummary(
        generated_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        filter_path=str(resolved_filter_path),
        raw_market_path=str(resolved_raw_market_path),
        filter_table=filter_table,
        objective_table=objective_table,
        filter_snapshot_count=len(filter_rows),
        objective_profile_table_present=objective_table_present,
        objective_profile_row_count=sum(len(rows) for rows in objective_rows.values()),
        objective_profile_instrument_count=len(objective_rows),
        filter_signal_start_date=None if filter_signal_start is None else filter_signal_start.isoformat(),
        filter_signal_end_date=None if filter_signal_end is None else filter_signal_end.isoformat(),
        covered_objective_count=covered_objective_count,
        missing_objective_count=missing_objective_count,
        missing_ratio=0.0 if not filter_rows else missing_objective_count / len(filter_rows),
        suggested_backfill_start_date=(
            None if suggested_backfill_start_date is None else suggested_backfill_start_date.isoformat()
        ),
        suggested_backfill_end_date=(
            None if suggested_backfill_end_date is None else suggested_backfill_end_date.isoformat()
        ),
        suggested_backfill_reason=suggested_backfill_reason,
        top_missing_by_signal_date=_top_buckets(missing_by_signal_date, limit=normalized_group_limit),
        top_missing_by_instrument=_top_buckets(missing_by_instrument, limit=normalized_group_limit),
        top_missing_by_market_type=_top_buckets(missing_by_market_type, limit=normalized_group_limit),
    )
    _write_json(summary.as_dict(), summary_path)
    _write_markdown(_render_markdown(summary), report_path)
    return summary


def _load_filter_audit_rows(
    *,
    filter_path: Path,
    table_name: str,
    signal_start_date: date | None,
    signal_end_date: date | None,
) -> list[_FilterAuditRow]:
    connection = duckdb.connect(str(filter_path), read_only=True)
    try:
        parameters: list[object] = []
        where_clauses: list[str] = []
        if signal_start_date is not None:
            where_clauses.append("signal_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("signal_date <= ?")
            parameters.append(signal_end_date)
        where_sql = "" if not where_clauses else f"WHERE {' AND '.join(where_clauses)}"
        rows = connection.execute(
            f"""
            SELECT instrument, signal_date
            FROM {table_name}
            {where_sql}
            ORDER BY signal_date, instrument
            """,
            parameters,
        ).fetchall()
    finally:
        connection.close()
    return [
        _FilterAuditRow(
            instrument=str(row[0]),
            signal_date=_normalize_date_value(row[1], field_name="signal_date"),
        )
        for row in rows
    ]


def _load_objective_profile_rows(
    *,
    raw_market_path: Path,
    table_name: str,
    instruments: tuple[str, ...],
    signal_end_date: date | None,
) -> dict[str, list[_ObjectiveProfileRow]]:
    if not raw_market_path.exists() or not instruments:
        return {}
    if not _table_exists(raw_market_path, table_name):
        return {}
    connection = duckdb.connect(str(raw_market_path), read_only=True)
    try:
        placeholders = ", ".join("?" for _ in instruments)
        parameters: list[object] = [*instruments]
        where_clauses = [f"code IN ({placeholders})"]
        if signal_end_date is not None:
            where_clauses.append("observed_trade_date <= ?")
            parameters.append(signal_end_date)
        rows = connection.execute(
            f"""
            SELECT code, observed_trade_date, market_type
            FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY code, observed_trade_date
            """,
            parameters,
        ).fetchall()
    finally:
        connection.close()
    grouped_rows: dict[str, list[_ObjectiveProfileRow]] = {}
    for row in rows:
        grouped_rows.setdefault(str(row[0]), []).append(
            _ObjectiveProfileRow(
                instrument=str(row[0]),
                observed_trade_date=_normalize_date_value(row[1], field_name="observed_trade_date"),
                market_type=None if row[2] is None else str(row[2]).strip().lower() or None,
            )
        )
    return grouped_rows


def _resolve_objective_profile_for_signal(
    objective_rows: dict[str, list[_ObjectiveProfileRow]],
    objective_dates_by_instrument: dict[str, list[date]],
    *,
    instrument: str,
    signal_date: date,
) -> _ObjectiveProfileRow | None:
    instrument_rows = objective_rows.get(instrument)
    instrument_dates = objective_dates_by_instrument.get(instrument)
    if not instrument_rows or not instrument_dates:
        return None
    index = bisect_right(instrument_dates, signal_date) - 1
    if index < 0:
        return None
    return instrument_rows[index]


def _table_exists(database_path: Path, table_name: str) -> bool:
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()
        return row is not None
    finally:
        connection.close()


def _infer_market_type(instrument: str) -> str:
    normalized = str(instrument).strip().upper()
    if "." in normalized:
        suffix = normalized.rsplit(".", 1)[1].lower()
        if suffix:
            return suffix
    return "unknown"


def _top_buckets(counter: Counter[str], *, limit: int) -> tuple[FilterObjectiveCoverageBucket, ...]:
    return tuple(
        FilterObjectiveCoverageBucket(bucket_key=key, missing_count=count)
        for key, count in counter.most_common(limit)
    )


def _build_backfill_reason(
    *,
    objective_table_present: bool,
    objective_profile_row_count: int,
    missing_objective_count: int,
    suggested_backfill_start_date: date | None,
    suggested_backfill_end_date: date | None,
) -> str | None:
    if missing_objective_count <= 0:
        return None
    if not objective_table_present:
        return "official raw_tdxquant_instrument_profile table is missing in raw_market; bootstrap and backfill the whole missing window first."
    if objective_profile_row_count <= 0:
        return "objective profile table exists but contains no relevant rows; backfill the whole missing window first."
    if suggested_backfill_start_date == suggested_backfill_end_date:
        return "backfill the single missing signal_date first, then rerun coverage audit."
    return "backfill the contiguous missing signal_date window first, then rerun coverage audit."


def _render_markdown(summary: FilterObjectiveCoverageAuditSummary) -> str:
    lines = [
        "# Filter Objective Coverage Audit",
        "",
        f"- Generated At (UTC): `{summary.generated_at_utc}`",
        f"- Filter Snapshot Count: `{summary.filter_snapshot_count}`",
        f"- Covered Objective Count: `{summary.covered_objective_count}`",
        f"- Missing Objective Count: `{summary.missing_objective_count}`",
        f"- Missing Ratio: `{summary.missing_ratio:.4f}`",
        f"- Objective Table Present: `{summary.objective_profile_table_present}`",
        f"- Objective Profile Row Count: `{summary.objective_profile_row_count}`",
        f"- Filter Signal Range: `{summary.filter_signal_start_date}` -> `{summary.filter_signal_end_date}`",
        f"- Suggested Backfill Window: `{summary.suggested_backfill_start_date}` -> `{summary.suggested_backfill_end_date}`",
        f"- Suggested Reason: `{summary.suggested_backfill_reason}`",
        "",
        "## Top Missing By Signal Date",
        "",
    ]
    lines.extend(_render_bucket_lines(summary.top_missing_by_signal_date))
    lines.extend(
        [
            "",
            "## Top Missing By Instrument",
            "",
        ]
    )
    lines.extend(_render_bucket_lines(summary.top_missing_by_instrument))
    lines.extend(
        [
            "",
            "## Top Missing By Market Type",
            "",
        ]
    )
    lines.extend(_render_bucket_lines(summary.top_missing_by_market_type))
    return "\n".join(lines) + "\n"


def _render_bucket_lines(buckets: tuple[FilterObjectiveCoverageBucket, ...]) -> list[str]:
    if not buckets:
        return ["- none"]
    return [f"- `{row.bucket_key}`: `{row.missing_count}`" for row in buckets]


def _write_json(payload: dict[str, object], output_path: Path | None) -> None:
    if output_path is None:
        return
    target_path = Path(output_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown(content: str, output_path: Path | None) -> None:
    if output_path is None:
        return
    target_path = Path(output_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(content, encoding="utf-8")
