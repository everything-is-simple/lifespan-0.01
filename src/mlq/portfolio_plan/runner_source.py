"""`portfolio_plan` runner 的 position bridge 读取辅助。"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb

from .materialization import _PositionBridgeRow
from .runner_shared import (
    DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE,
    DEFAULT_POSITION_CAPACITY_TABLE,
    DEFAULT_POSITION_SIZING_TABLE,
)


def load_position_bridge_rows(
    *,
    position_path: Path,
    signal_start_date: date | None,
    signal_end_date: date | None,
    reference_trade_dates: tuple[date, ...],
    instruments: tuple[str, ...],
    candidate_nks: tuple[str, ...],
    limit: int | None,
) -> list[_PositionBridgeRow]:
    """只读 position 正式账本，构造 `portfolio_plan` bridge 行。"""

    connection = duckdb.connect(str(position_path), read_only=True)
    try:
        ensure_bridge_tables_exist(connection)
        parameters: list[object] = []
        where_clauses: list[str] = []
        if signal_start_date is not None:
            where_clauses.append("s.reference_trade_date >= ?")
            parameters.append(signal_start_date)
        if signal_end_date is not None:
            where_clauses.append("s.reference_trade_date <= ?")
            parameters.append(signal_end_date)
        if reference_trade_dates:
            placeholders = ", ".join("?" for _ in reference_trade_dates)
            where_clauses.append(f"s.reference_trade_date IN ({placeholders})")
            parameters.extend(reference_trade_dates)
        if instruments:
            placeholders = ", ".join("?" for _ in instruments)
            where_clauses.append(f"a.instrument IN ({placeholders})")
            parameters.extend(instruments)
        if candidate_nks:
            placeholders = ", ".join("?" for _ in candidate_nks)
            where_clauses.append(f"a.candidate_nk IN ({placeholders})")
            parameters.extend(candidate_nks)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        limit_sql = "LIMIT ?" if limit is not None else ""
        if limit is not None:
            parameters.append(limit)
        rows = connection.execute(
            f"""
            SELECT
                a.candidate_nk,
                a.instrument,
                a.policy_id,
                s.reference_trade_date,
                a.candidate_status,
                a.blocked_reason_code,
                s.position_action_decision,
                COALESCE(s.schedule_stage, 't+1'),
                COALESCE(s.schedule_lag_days, 1),
                c.final_allowed_position_weight,
                COALESCE(s.required_reduction_weight, c.required_reduction_weight, 0),
                COALESCE(c.remaining_single_name_capacity_weight, 0),
                COALESCE(c.remaining_portfolio_capacity_weight, 0),
                COALESCE(c.binding_cap_code, 'no_binding_cap'),
                COALESCE(c.capacity_source_code, 'unknown')
            FROM {DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE} AS a
            INNER JOIN {DEFAULT_POSITION_CAPACITY_TABLE} AS c
                ON c.candidate_nk = a.candidate_nk
            INNER JOIN {DEFAULT_POSITION_SIZING_TABLE} AS s
                ON s.candidate_nk = a.candidate_nk
            {where_sql}
            ORDER BY s.reference_trade_date, a.instrument, a.candidate_nk
            {limit_sql}
            """,
            parameters,
        ).fetchall()
        return [
            _PositionBridgeRow(
                candidate_nk=str(row[0]),
                instrument=str(row[1]),
                policy_id=str(row[2]),
                reference_trade_date=normalize_date_value(
                    row[3],
                    field_name="reference_trade_date",
                ),
                candidate_status=normalize_optional_str(row[4]).lower(),
                blocked_reason_code=normalize_optional_str(row[5]) or None,
                position_action_decision=normalize_optional_str(row[6]),
                schedule_stage=normalize_optional_str(row[7]) or "t+1",
                schedule_lag_days=max(int(row[8] or 1), 0),
                final_allowed_position_weight=float(row[9] or 0.0),
                required_reduction_weight=float(row[10] or 0.0),
                remaining_single_name_capacity_weight=float(row[11] or 0.0),
                remaining_portfolio_capacity_weight=float(row[12] or 0.0),
                binding_cap_code=normalize_optional_str(row[13]) or "no_binding_cap",
                capacity_source_code=normalize_optional_str(row[14]) or "unknown",
            )
            for row in rows
        ]
    finally:
        connection.close()


def load_bridge_rows_for_claimed_dates(
    *,
    position_path: Path,
    claimed_queue_rows: list[dict[str, object]],
    signal_start_date: date | None,
    signal_end_date: date | None,
) -> list[_PositionBridgeRow]:
    """按 claimed queue 覆盖的 trade_date 重载整日 bridge 行。"""

    if not claimed_queue_rows:
        return []
    claimed_dates = tuple(
        sorted(
            {
                normalize_date_value(
                    row["reference_trade_date"],
                    field_name="queue.reference_trade_date",
                )
                for row in claimed_queue_rows
            }
        )
    )
    return load_position_bridge_rows(
        position_path=position_path,
        signal_start_date=signal_start_date,
        signal_end_date=signal_end_date,
        reference_trade_dates=claimed_dates,
        instruments=(),
        candidate_nks=(),
        limit=None,
    )


def ensure_bridge_tables_exist(connection: duckdb.DuckDBPyConnection) -> None:
    """确保 position 桥接所需表族已存在。"""

    rows = connection.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        """
    ).fetchall()
    existing_tables = {str(row[0]) for row in rows}
    required_tables = {
        DEFAULT_POSITION_CANDIDATE_AUDIT_TABLE,
        DEFAULT_POSITION_CAPACITY_TABLE,
        DEFAULT_POSITION_SIZING_TABLE,
    }
    missing_tables = sorted(required_tables - existing_tables)
    if missing_tables:
        raise ValueError(
            "Missing required position bridge tables: " + ", ".join(missing_tables)
        )


def max_reference_trade_date(bridge_rows: list[_PositionBridgeRow]) -> date | None:
    """返回 bridge 行覆盖到的最新 reference_trade_date。"""

    if not bridge_rows:
        return None
    return max(row.reference_trade_date for row in bridge_rows)


def normalize_date_value(value: object, *, field_name: str) -> date:
    """把读取到的日期字段规范成 `date`。"""

    if value is None:
        raise ValueError(f"Missing required date field: {field_name}")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def normalize_optional_str(value: object) -> str:
    """把可空字符串字段规整成稳定文本。"""

    if value is None:
        return ""
    return str(value).strip()
