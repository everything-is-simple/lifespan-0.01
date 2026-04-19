"""`malf 0/1 wave` 的只读分类审计。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import duckdb

from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.malf.bootstrap import MALF_STATE_SNAPSHOT_TABLE, MALF_WAVE_LEDGER_TABLE, malf_ledger_path
from mlq.malf.canonical_shared import _normalize_timeframes

DEFAULT_ZERO_ONE_GUARD_STALE_DAYS: Final[int] = 250
DEFAULT_ZERO_ONE_SAMPLE_LIMIT: Final[int] = 20
_DETAIL_COLUMNS: Final[tuple[str, ...]] = (
    "category",
    "category_reason",
    "timeframe",
    "code",
    "wave_id",
    "direction",
    "major_state",
    "reversal_stage",
    "start_bar_dt",
    "end_bar_dt",
    "bar_count",
    "same_day_start_end_flag",
    "has_state_at_start_flag",
    "guard_source",
    "guard_bar_dt",
    "guard_price",
    "guard_age_days",
    "stale_guard_flag",
    "next_wave_id",
    "next_direction",
    "next_start_bar_dt",
    "next_wave_active_flag",
    "immediate_opposite_reflip_flag",
)
_DETAIL_COLUMNS_WITH_SORT: Final[tuple[str, ...]] = _DETAIL_COLUMNS + ("guard_age_days_sort",)


@dataclass(frozen=True)
class MalfZeroOneWaveAuditSample:
    category: str
    timeframe: str
    code: str
    wave_id: int
    direction: str
    start_bar_dt: str
    end_bar_dt: str | None
    bar_count: int
    guard_source: str | None
    guard_bar_dt: str | None
    guard_age_days: int | None
    next_wave_id: int | None
    next_direction: str | None
    next_start_bar_dt: str | None
    next_wave_active_flag: bool | None
    immediate_opposite_reflip_flag: bool

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MalfZeroOneWaveAuditTimeframeSummary:
    timeframe: str
    malf_path: str
    total_short_wave_count: int
    same_bar_double_switch_count: int
    stale_guard_trigger_count: int
    next_bar_reflip_count: int
    non_immediate_boundary_count: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MalfZeroOneWaveAuditSummary:
    generated_at_utc: str
    stale_guard_age_days: int
    sample_limit: int
    timeframe_list: tuple[str, ...]
    total_short_wave_count: int
    same_bar_double_switch_count: int
    stale_guard_trigger_count: int
    next_bar_reflip_count: int
    non_immediate_boundary_count: int
    detail_row_count: int
    detail_columns: tuple[str, ...]
    timeframe_summaries: tuple[MalfZeroOneWaveAuditTimeframeSummary, ...]
    top_same_bar_double_switch_samples: tuple[MalfZeroOneWaveAuditSample, ...]
    top_stale_guard_trigger_samples: tuple[MalfZeroOneWaveAuditSample, ...]
    top_next_bar_reflip_samples: tuple[MalfZeroOneWaveAuditSample, ...]

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["timeframe_summaries"] = [row.as_dict() for row in self.timeframe_summaries]
        payload["top_same_bar_double_switch_samples"] = [
            row.as_dict() for row in self.top_same_bar_double_switch_samples
        ]
        payload["top_stale_guard_trigger_samples"] = [
            row.as_dict() for row in self.top_stale_guard_trigger_samples
        ]
        payload["top_next_bar_reflip_samples"] = [
            row.as_dict() for row in self.top_next_bar_reflip_samples
        ]
        return payload


def run_malf_zero_one_wave_audit(
    *,
    settings: WorkspaceRoots | None = None,
    timeframes: list[str] | tuple[str, ...] | None = None,
    stale_guard_age_days: int = DEFAULT_ZERO_ONE_GUARD_STALE_DAYS,
    sample_limit: int = DEFAULT_ZERO_ONE_SAMPLE_LIMIT,
    summary_path: Path | None = None,
    report_path: Path | None = None,
    detail_path: Path | None = None,
) -> MalfZeroOneWaveAuditSummary:
    """按三类规则只读审计 `malf_wave_ledger` 中的完成短 wave。"""

    workspace = settings or default_settings()
    normalized_timeframes = _normalize_timeframes(timeframes)
    normalized_stale_days = max(int(stale_guard_age_days), 1)
    normalized_sample_limit = max(int(sample_limit), 1)

    timeframe_summaries: list[MalfZeroOneWaveAuditTimeframeSummary] = []
    same_bar_candidates: list[dict[str, object]] = []
    stale_guard_candidates: list[dict[str, object]] = []
    next_bar_candidates: list[dict[str, object]] = []
    total_short_wave_count = 0
    same_bar_double_switch_count = 0
    stale_guard_trigger_count = 0
    next_bar_reflip_count = 0
    non_immediate_boundary_count = 0

    export_parts: list[Path] = []
    export_root = None if detail_path is None else Path(detail_path)

    for timeframe in normalized_timeframes:
        resolved_path = Path(malf_ledger_path(workspace, timeframe=timeframe))
        if not resolved_path.exists():
            raise FileNotFoundError(f"Missing malf database for timeframe={timeframe}: {resolved_path}")

        with duckdb.connect(str(resolved_path), read_only=True) as connection:
            summary_row = _load_timeframe_summary(
                connection=connection,
                timeframe=timeframe,
                database_path=resolved_path,
                stale_guard_age_days=normalized_stale_days,
            )
            timeframe_summaries.append(summary_row)
            total_short_wave_count += summary_row.total_short_wave_count
            same_bar_double_switch_count += summary_row.same_bar_double_switch_count
            stale_guard_trigger_count += summary_row.stale_guard_trigger_count
            next_bar_reflip_count += summary_row.next_bar_reflip_count
            non_immediate_boundary_count += summary_row.non_immediate_boundary_count

            same_bar_candidates.extend(
                _load_sample_rows(
                    connection=connection,
                    stale_guard_age_days=normalized_stale_days,
                    category="same_bar_double_switch",
                    limit=normalized_sample_limit,
                    order_by="timeframe ASC, code ASC, start_bar_dt ASC, wave_id ASC",
                )
            )
            stale_guard_candidates.extend(
                _load_sample_rows(
                    connection=connection,
                    stale_guard_age_days=normalized_stale_days,
                    category="stale_guard_trigger",
                    limit=normalized_sample_limit,
                    order_by="guard_age_days DESC, timeframe ASC, code ASC, wave_id ASC",
                )
            )
            next_bar_candidates.extend(
                _load_sample_rows(
                    connection=connection,
                    stale_guard_age_days=normalized_stale_days,
                    category="next_bar_reflip",
                    limit=normalized_sample_limit,
                    order_by="guard_age_days_sort ASC, timeframe ASC, code ASC, wave_id ASC",
                )
            )

            if export_root is not None:
                export_parts.append(
                    _export_detail_part(
                        connection=connection,
                        stale_guard_age_days=normalized_stale_days,
                        output_root=export_root,
                        timeframe=timeframe,
                    )
                )

    if export_root is not None:
        _merge_detail_parts(output_path=export_root, part_paths=export_parts)

    summary = MalfZeroOneWaveAuditSummary(
        generated_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        stale_guard_age_days=normalized_stale_days,
        sample_limit=normalized_sample_limit,
        timeframe_list=normalized_timeframes,
        total_short_wave_count=total_short_wave_count,
        same_bar_double_switch_count=same_bar_double_switch_count,
        stale_guard_trigger_count=stale_guard_trigger_count,
        next_bar_reflip_count=next_bar_reflip_count,
        non_immediate_boundary_count=non_immediate_boundary_count,
        detail_row_count=total_short_wave_count,
        detail_columns=_DETAIL_COLUMNS,
        timeframe_summaries=tuple(timeframe_summaries),
        top_same_bar_double_switch_samples=_build_samples_from_rows(
            same_bar_candidates,
            limit=normalized_sample_limit,
            sort_key=lambda row: (
                str(row["timeframe"]),
                str(row["code"]),
                _date_sort_key(row["start_bar_dt"]),
                int(row["wave_id"]),
            ),
            reverse=False,
        ),
        top_stale_guard_trigger_samples=_build_samples_from_rows(
            stale_guard_candidates,
            limit=normalized_sample_limit,
            sort_key=lambda row: (
                _sort_int(row["guard_age_days"]),
                str(row["timeframe"]),
                str(row["code"]),
                int(row["wave_id"]),
            ),
            reverse=True,
        ),
        top_next_bar_reflip_samples=_build_samples_from_rows(
            next_bar_candidates,
            limit=normalized_sample_limit,
            sort_key=lambda row: (
                _sort_int(row["guard_age_days_sort"]),
                str(row["timeframe"]),
                str(row["code"]),
                int(row["wave_id"]),
            ),
            reverse=False,
        ),
    )
    _write_json(summary.as_dict(), summary_path)
    _write_markdown(_render_markdown(summary), report_path)
    return summary


def _load_timeframe_summary(
    *,
    connection: duckdb.DuckDBPyConnection,
    timeframe: str,
    database_path: Path,
    stale_guard_age_days: int,
) -> MalfZeroOneWaveAuditTimeframeSummary:
    sql = f"""
        SELECT
            COUNT(*) AS total_short_wave_count,
            SUM(CASE WHEN category = 'same_bar_double_switch' THEN 1 ELSE 0 END) AS same_bar_double_switch_count,
            SUM(CASE WHEN category = 'stale_guard_trigger' THEN 1 ELSE 0 END) AS stale_guard_trigger_count,
            SUM(CASE WHEN category = 'next_bar_reflip' THEN 1 ELSE 0 END) AS next_bar_reflip_count,
            SUM(CASE WHEN NOT immediate_opposite_reflip_flag THEN 1 ELSE 0 END) AS non_immediate_boundary_count
        FROM ({_tagged_rows_sql(stale_guard_age_days=stale_guard_age_days, include_sort=False)}) tagged
    """
    row = connection.execute(sql).fetchone()
    if row is None:
        row = (0, 0, 0, 0, 0)
    return MalfZeroOneWaveAuditTimeframeSummary(
        timeframe=timeframe,
        malf_path=str(database_path),
        total_short_wave_count=int(row[0] or 0),
        same_bar_double_switch_count=int(row[1] or 0),
        stale_guard_trigger_count=int(row[2] or 0),
        next_bar_reflip_count=int(row[3] or 0),
        non_immediate_boundary_count=int(row[4] or 0),
    )


def _load_sample_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    stale_guard_age_days: int,
    category: str,
    limit: int,
    order_by: str,
) -> list[dict[str, object]]:
    sql = f"""
        SELECT {", ".join(_DETAIL_COLUMNS_WITH_SORT)}
        FROM ({_tagged_rows_sql(stale_guard_age_days=stale_guard_age_days, include_sort=True)}) tagged
        WHERE category = ?
        ORDER BY {order_by}
        LIMIT ?
    """
    cursor = connection.execute(sql, [category, limit])
    column_names = [str(column[0]) for column in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(column_names, row, strict=True)) for row in rows]


def _export_detail_part(
    *,
    connection: duckdb.DuckDBPyConnection,
    stale_guard_age_days: int,
    output_root: Path,
    timeframe: str,
) -> Path:
    output_root.parent.mkdir(parents=True, exist_ok=True)
    part_path = output_root.with_suffix(f".{timeframe}.part.csv")
    copy_sql = f"""
        COPY (
            SELECT {", ".join(_DETAIL_COLUMNS)}
            FROM ({_tagged_rows_sql(stale_guard_age_days=stale_guard_age_days, include_sort=False)}) tagged
            ORDER BY code, wave_id
        )
        TO {_sql_literal(str(part_path))}
        WITH (HEADER, DELIMITER ',')
    """
    connection.execute(copy_sql)
    return part_path


def _merge_detail_parts(*, output_path: Path, part_paths: list[Path]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as merged_handle:
        wrote_header = False
        for part_path in part_paths:
            if not part_path.exists():
                continue
            with part_path.open("r", encoding="utf-8", newline="") as part_handle:
                for line_number, line in enumerate(part_handle):
                    if line_number == 0:
                        if wrote_header:
                            continue
                        wrote_header = True
                    merged_handle.write(line)
            part_path.unlink(missing_ok=True)


def _tagged_rows_sql(*, stale_guard_age_days: int, include_sort: bool) -> str:
    normalized_stale_days = max(int(stale_guard_age_days), 1)
    sort_column = ",\n                COALESCE(guard_age_days, -1) AS guard_age_days_sort" if include_sort else ""
    return f"""
        WITH short_waves AS (
            SELECT
                code,
                timeframe,
                wave_id,
                direction,
                major_state,
                reversal_stage,
                start_bar_dt,
                end_bar_dt,
                bar_count
            FROM {MALF_WAVE_LEDGER_TABLE}
            WHERE active_flag = FALSE
              AND bar_count <= 1
        ),
        all_waves AS (
            SELECT code, timeframe, wave_id, direction, start_bar_dt, active_flag
            FROM {MALF_WAVE_LEDGER_TABLE}
        ),
        joined AS (
            SELECT
                w.code,
                w.timeframe,
                w.wave_id,
                w.direction,
                w.major_state,
                w.reversal_stage,
                w.start_bar_dt,
                w.end_bar_dt,
                w.bar_count,
                s.snapshot_nk IS NOT NULL AS has_state_at_start_flag,
                CASE
                    WHEN w.direction = 'up' THEN 'last_valid_lh'
                    WHEN w.direction = 'down' THEN 'last_valid_hl'
                    ELSE NULL
                END AS guard_source,
                CASE
                    WHEN w.direction = 'up' THEN s.last_valid_lh_bar_dt
                    WHEN w.direction = 'down' THEN s.last_valid_hl_bar_dt
                    ELSE NULL
                END AS guard_bar_dt,
                CASE
                    WHEN w.direction = 'up' THEN s.last_valid_lh_price
                    WHEN w.direction = 'down' THEN s.last_valid_hl_price
                    ELSE NULL
                END AS guard_price,
                CASE
                    WHEN w.direction = 'up' AND s.last_valid_lh_bar_dt IS NOT NULL
                        THEN DATE_DIFF('day', s.last_valid_lh_bar_dt, w.start_bar_dt)
                    WHEN w.direction = 'down' AND s.last_valid_hl_bar_dt IS NOT NULL
                        THEN DATE_DIFF('day', s.last_valid_hl_bar_dt, w.start_bar_dt)
                    ELSE NULL
                END AS guard_age_days,
                nw.wave_id AS next_wave_id,
                nw.direction AS next_direction,
                nw.start_bar_dt AS next_start_bar_dt,
                nw.active_flag AS next_wave_active_flag
            FROM short_waves w
            LEFT JOIN {MALF_STATE_SNAPSHOT_TABLE} s
              ON s.code = w.code
             AND s.timeframe = w.timeframe
             AND s.wave_id = w.wave_id
             AND s.asof_bar_dt = w.start_bar_dt
            LEFT JOIN all_waves nw
              ON nw.code = w.code
             AND nw.timeframe = w.timeframe
             AND nw.wave_id = w.wave_id + 1
        )
        SELECT
            CASE
                WHEN bar_count = 0 THEN 'same_bar_double_switch'
                WHEN guard_age_days IS NOT NULL AND guard_age_days >= {normalized_stale_days} THEN 'stale_guard_trigger'
                ELSE 'next_bar_reflip'
            END AS category,
            CASE
                WHEN bar_count = 0 THEN 'wave was created and finalized on the same bar before any state snapshot was written'
                WHEN guard_age_days IS NOT NULL AND guard_age_days >= {normalized_stale_days} THEN 'one-bar wave used an aged guard pivot beyond the stale threshold'
                ELSE 'one-bar wave was finalized on the next opposite wave without crossing the stale threshold'
            END AS category_reason,
            timeframe,
            code,
            wave_id,
            direction,
            major_state,
            reversal_stage,
            start_bar_dt,
            end_bar_dt,
            bar_count,
            start_bar_dt = end_bar_dt AS same_day_start_end_flag,
            has_state_at_start_flag,
            guard_source,
            guard_bar_dt,
            guard_price,
            guard_age_days,
            guard_age_days IS NOT NULL AND guard_age_days >= {normalized_stale_days} AS stale_guard_flag,
            next_wave_id,
            next_direction,
            next_start_bar_dt,
            next_wave_active_flag,
            (
                next_wave_id IS NOT NULL
                AND next_start_bar_dt = end_bar_dt
                AND next_direction <> direction
            ) AS immediate_opposite_reflip_flag
            {sort_column}
        FROM joined
    """


def _build_samples_from_rows(
    rows: list[dict[str, object]],
    *,
    limit: int,
    sort_key,
    reverse: bool,
) -> tuple[MalfZeroOneWaveAuditSample, ...]:
    if not rows:
        return ()
    ordered_rows = sorted(rows, key=sort_key, reverse=reverse)[:limit]
    return tuple(_sample_from_row(row) for row in ordered_rows)


def _sample_from_row(row: dict[str, object]) -> MalfZeroOneWaveAuditSample:
    return MalfZeroOneWaveAuditSample(
        category=str(row["category"]),
        timeframe=str(row["timeframe"]),
        code=str(row["code"]),
        wave_id=int(row["wave_id"]),
        direction=str(row["direction"]),
        start_bar_dt=_date_to_iso(row["start_bar_dt"]),
        end_bar_dt=_date_to_iso(row["end_bar_dt"]),
        bar_count=int(row["bar_count"]),
        guard_source=_str_or_none(row["guard_source"]),
        guard_bar_dt=_date_to_iso(row["guard_bar_dt"]),
        guard_age_days=_int_or_none(row["guard_age_days"]),
        next_wave_id=_int_or_none(row["next_wave_id"]),
        next_direction=_str_or_none(row["next_direction"]),
        next_start_bar_dt=_date_to_iso(row["next_start_bar_dt"]),
        next_wave_active_flag=_bool_or_none(row["next_wave_active_flag"]),
        immediate_opposite_reflip_flag=bool(row["immediate_opposite_reflip_flag"]),
    )


def _render_markdown(summary: MalfZeroOneWaveAuditSummary) -> str:
    lines = [
        "# MALF 0/1 Wave Audit",
        "",
        f"- Generated At (UTC): `{summary.generated_at_utc}`",
        f"- Stale Guard Threshold (days): `{summary.stale_guard_age_days}`",
        f"- Timeframes: `{', '.join(summary.timeframe_list)}`",
        f"- Total Short Waves: `{summary.total_short_wave_count}`",
        f"- Same-Bar Double Switch: `{summary.same_bar_double_switch_count}`",
        f"- Stale Guard Trigger: `{summary.stale_guard_trigger_count}`",
        f"- Next-Bar Reflip: `{summary.next_bar_reflip_count}`",
        f"- Non-Immediate Boundary Count: `{summary.non_immediate_boundary_count}`",
        "",
        "## Timeframe Summary",
        "",
        "| Timeframe | Path | Short | Same-Bar | Stale Guard | Next-Bar | Non-Immediate |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary.timeframe_summaries:
        lines.append(
            f"| `{row.timeframe}` | `{row.malf_path}` | `{row.total_short_wave_count}` | "
            f"`{row.same_bar_double_switch_count}` | `{row.stale_guard_trigger_count}` | "
            f"`{row.next_bar_reflip_count}` | `{row.non_immediate_boundary_count}` |"
        )
    lines.extend(["", "## Top Same-Bar Double Switch Samples", ""])
    lines.extend(_render_sample_lines(summary.top_same_bar_double_switch_samples))
    lines.extend(["", "## Top Stale Guard Trigger Samples", ""])
    lines.extend(_render_sample_lines(summary.top_stale_guard_trigger_samples))
    lines.extend(["", "## Top Next-Bar Reflip Samples", ""])
    lines.extend(_render_sample_lines(summary.top_next_bar_reflip_samples))
    return "\n".join(lines) + "\n"


def _render_sample_lines(samples: tuple[MalfZeroOneWaveAuditSample, ...]) -> list[str]:
    if not samples:
        return ["- none"]
    return [
        (
            f"- `{row.timeframe}` `{row.code}` wave `{row.wave_id}` `{row.direction}` "
            f"`{row.start_bar_dt}` -> `{row.end_bar_dt}` `bar_count={row.bar_count}` "
            f"`guard_age_days={row.guard_age_days}` `next_wave={row.next_wave_id}` "
            f"`next_direction={row.next_direction}` `next_active={row.next_wave_active_flag}`"
        )
        for row in samples
    ]


def _date_to_iso(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _str_or_none(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _bool_or_none(value: object) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _date_sort_key(value: object) -> str:
    return _date_to_iso(value) or ""


def _sort_int(value: object) -> int:
    if value is None:
        return -1
    return int(value)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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
