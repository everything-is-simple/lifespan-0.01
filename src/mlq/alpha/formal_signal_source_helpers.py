"""`alpha formal signal source` 的共享 helper。"""

from __future__ import annotations

import json

import duckdb


def _load_table_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchall()
    if not rows:
        raise ValueError(f"Missing table: {table_name}")
    return {str(row[0]) for row in rows}


def _resolve_existing_column(
    available_columns: set[str],
    candidates: tuple[str, ...],
    *,
    field_name: str,
    table_name: str,
) -> str:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    raise ValueError(f"Missing required column `{field_name}` in table `{table_name}`.")


def _resolve_optional_column(available_columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in available_columns:
            return candidate
    return None


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


def _parse_payload_json(payload_json: object) -> dict[str, object]:
    if payload_json is None:
        return {}
    try:
        parsed = json.loads(str(payload_json))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
