"""执行 `alpha family ledger` 官方 bounded materialization。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Final

import duckdb

from mlq.alpha.bootstrap import alpha_ledger_path, bootstrap_alpha_family_ledger
from mlq.alpha.family_materialization import (
    _insert_run_row,
    _mark_run_completed,
    _materialize_family_rows,
    _update_run_summary,
)
from mlq.alpha.family_shared import (
    ALPHA_FAMILY_SCOPE_ALL as SHARED_ALPHA_FAMILY_SCOPE_ALL,
    DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE as SHARED_DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE,
    DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION as SHARED_DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION,
    DEFAULT_ALPHA_FAMILY_MALF_TABLE as SHARED_DEFAULT_ALPHA_FAMILY_MALF_TABLE,
    DEFAULT_ALPHA_FAMILY_STRUCTURE_TABLE as SHARED_DEFAULT_ALPHA_FAMILY_STRUCTURE_TABLE,
    DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE as SHARED_DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE,
    AlphaFamilyBuildSummary,
    _build_alpha_family_run_id,
    _coerce_date,
    _normalize_family_scope,
    _write_summary,
)
from mlq.alpha.family_source import (
    _load_candidate_rows,
    _load_official_context_rows,
    _load_trigger_rows,
)
from mlq.core.paths import WorkspaceRoots, default_settings


ALPHA_FAMILY_SCOPE_ALL: Final[tuple[str, ...]] = SHARED_ALPHA_FAMILY_SCOPE_ALL
DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE: Final[str] = SHARED_DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE
DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE: Final[str] = SHARED_DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE
DEFAULT_ALPHA_FAMILY_STRUCTURE_TABLE: Final[str] = SHARED_DEFAULT_ALPHA_FAMILY_STRUCTURE_TABLE
DEFAULT_ALPHA_FAMILY_MALF_TABLE: Final[str] = SHARED_DEFAULT_ALPHA_FAMILY_MALF_TABLE
DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION: Final[str] = SHARED_DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION


def run_alpha_family_build(
    *,
    settings: WorkspaceRoots | None = None,
    alpha_path: Path | None = None,
    structure_path: Path | None = None,
    malf_path: Path | None = None,
    signal_start_date: str | date | None = None,
    signal_end_date: str | date | None = None,
    family_scope: list[str] | tuple[str, ...] | None = None,
    instruments: list[str] | tuple[str, ...] | None = None,
    limit: int = 100,
    batch_size: int = 100,
    run_id: str | None = None,
    source_trigger_table: str = DEFAULT_ALPHA_FAMILY_TRIGGER_TABLE,
    source_candidate_table: str = DEFAULT_ALPHA_FAMILY_CANDIDATE_TABLE,
    source_structure_table: str = DEFAULT_ALPHA_FAMILY_STRUCTURE_TABLE,
    source_malf_table: str = DEFAULT_ALPHA_FAMILY_MALF_TABLE,
    family_contract_version: str = DEFAULT_ALPHA_FAMILY_CONTRACT_VERSION,
    producer_name: str = "alpha_family_builder",
    producer_version: str = "v1",
    summary_path: Path | None = None,
) -> AlphaFamilyBuildSummary:
    """从官方 `alpha_trigger_event` 与 bounded family candidate 输入物化 family ledger。"""

    workspace = settings or default_settings()
    workspace.ensure_directories()

    resolved_alpha_path = Path(alpha_path or alpha_ledger_path(workspace))
    resolved_structure_path = Path(structure_path or workspace.databases.structure)
    resolved_malf_path = Path(malf_path or workspace.databases.malf)
    normalized_start_date = _coerce_date(signal_start_date)
    normalized_end_date = _coerce_date(signal_end_date)
    normalized_family_scope = _normalize_family_scope(family_scope)
    normalized_instruments = tuple(sorted({item for item in instruments or () if item}))
    normalized_limit = max(int(limit), 1)
    normalized_batch_size = max(int(batch_size), 1)
    materialization_run_id = run_id or _build_alpha_family_run_id()

    alpha_connection = duckdb.connect(str(resolved_alpha_path))
    try:
        bootstrap_alpha_family_ledger(workspace, connection=alpha_connection)
        trigger_rows = _load_trigger_rows(
            connection=alpha_connection,
            table_name=source_trigger_table,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            family_scope=normalized_family_scope,
            instruments=normalized_instruments,
            limit=normalized_limit,
        )
        candidate_map = _load_candidate_rows(
            connection=alpha_connection,
            table_name=source_candidate_table,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            family_scope=normalized_family_scope,
            instruments=tuple(sorted({row.instrument for row in trigger_rows})),
        )
        family_context_map = _load_official_context_rows(
            structure_path=resolved_structure_path,
            malf_path=resolved_malf_path,
            structure_table_name=source_structure_table,
            malf_table_name=source_malf_table,
            trigger_rows=trigger_rows,
        )

        bounded_instrument_count = len({row.instrument for row in trigger_rows})
        _insert_run_row(
            alpha_connection,
            run_id=materialization_run_id,
            producer_name=producer_name,
            producer_version=producer_version,
            family_scope=normalized_family_scope,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            bounded_instrument_count=bounded_instrument_count,
            source_trigger_table=source_trigger_table,
            source_candidate_table=source_candidate_table,
            family_contract_version=family_contract_version,
        )
        summary = _materialize_family_rows(
            connection=alpha_connection,
            run_id=materialization_run_id,
            trigger_rows=trigger_rows,
            candidate_map=candidate_map,
            family_context_map=family_context_map,
            family_contract_version=family_contract_version,
            family_scope=normalized_family_scope,
            producer_name=producer_name,
            producer_version=producer_version,
            signal_start_date=normalized_start_date,
            signal_end_date=normalized_end_date,
            alpha_path=resolved_alpha_path,
            structure_path=resolved_structure_path,
            malf_path=resolved_malf_path,
            source_trigger_table=source_trigger_table,
            source_candidate_table=source_candidate_table,
            source_structure_table=source_structure_table,
            source_malf_table=source_malf_table,
            batch_size=normalized_batch_size,
        )
        _mark_run_completed(alpha_connection, run_id=materialization_run_id, summary=summary)
        _write_summary(summary, summary_path)
        return summary
    except Exception:
        _update_run_summary(
            alpha_connection,
            run_id=materialization_run_id,
            run_status="failed",
            summary_payload={"run_status": "failed"},
        )
        raise
    finally:
        alpha_connection.close()
