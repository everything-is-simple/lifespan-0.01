"""主线本地正式库标准化 bootstrap 与迁移审计。"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb

from mlq.alpha.bootstrap import (
    alpha_ledger_path,
    bootstrap_alpha_family_ledger,
    bootstrap_alpha_formal_signal_ledger,
    bootstrap_alpha_trigger_ledger,
)
from mlq.core.paths import WorkspaceRoots, default_settings
from mlq.data.bootstrap import (
    bootstrap_market_base_ledger,
    bootstrap_raw_market_ledger,
    market_base_ledger_path,
    raw_market_ledger_path,
)
from mlq.filter.bootstrap import bootstrap_filter_snapshot_ledger, filter_ledger_path
from mlq.malf.bootstrap import bootstrap_malf_ledger, malf_ledger_path
from mlq.portfolio_plan.bootstrap import (
    bootstrap_portfolio_plan_ledger,
    portfolio_plan_ledger_path,
)
from mlq.position.bootstrap import bootstrap_position_ledger, position_ledger_path
from mlq.structure.bootstrap import bootstrap_structure_snapshot_ledger, structure_ledger_path
from mlq.system.bootstrap import bootstrap_system_ledger, system_ledger_path
from mlq.trade.bootstrap import bootstrap_trade_runtime_ledger, trade_runtime_ledger_path


def _bootstrap_alpha_ledger(settings: WorkspaceRoots) -> Path:
    bootstrap_alpha_trigger_ledger(settings)
    bootstrap_alpha_family_ledger(settings)
    return bootstrap_alpha_formal_signal_ledger(settings)


@dataclass(frozen=True)
class MainlineLedgerSpec:
    ledger_name: str
    module_name: str
    target_path_fn: Callable[[WorkspaceRoots], Path]
    bootstrap_fn: Callable[[WorkspaceRoots], Path]

    def target_path(self, settings: WorkspaceRoots) -> Path:
        return self.target_path_fn(settings)

    def bootstrap(self, settings: WorkspaceRoots) -> Path:
        return self.bootstrap_fn(settings)


@dataclass(frozen=True)
class MainlineLedgerInventoryRow:
    ledger_name: str
    module_name: str
    target_path: Path
    exists_before: bool
    size_bytes_before: int
    selected_for_bootstrap: bool
    source_path: Path | None

    def as_dict(self) -> dict[str, object]:
        return {
            "ledger_name": self.ledger_name,
            "module_name": self.module_name,
            "target_path": str(self.target_path),
            "exists_before": self.exists_before,
            "size_bytes_before": self.size_bytes_before,
            "selected_for_bootstrap": self.selected_for_bootstrap,
            "source_path": None if self.source_path is None else str(self.source_path),
        }


@dataclass(frozen=True)
class MainlineLedgerStandardizationResult:
    ledger_name: str
    module_name: str
    source_path: Path | None
    target_path: Path
    migration_action: str
    exists_after: bool
    size_bytes_after: int
    table_count_after: int
    total_row_count_after: int
    table_row_counts: dict[str, int]

    def as_dict(self) -> dict[str, object]:
        return {
            "ledger_name": self.ledger_name,
            "module_name": self.module_name,
            "source_path": None if self.source_path is None else str(self.source_path),
            "target_path": str(self.target_path),
            "migration_action": self.migration_action,
            "exists_after": self.exists_after,
            "size_bytes_after": self.size_bytes_after,
            "table_count_after": self.table_count_after,
            "total_row_count_after": self.total_row_count_after,
            "table_row_counts": dict(self.table_row_counts),
        }


@dataclass(frozen=True)
class MainlineLocalLedgerStandardizationSummary:
    run_id: str
    selected_ledger_count: int
    copied_ledger_count: int
    bootstrapped_ledger_count: int
    reused_ledger_count: int
    missing_source_count: int
    inventory_rows: tuple[MainlineLedgerInventoryRow, ...]
    result_rows: tuple[MainlineLedgerStandardizationResult, ...]
    report_json_path: Path
    report_markdown_path: Path

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "selected_ledger_count": self.selected_ledger_count,
            "copied_ledger_count": self.copied_ledger_count,
            "bootstrapped_ledger_count": self.bootstrapped_ledger_count,
            "reused_ledger_count": self.reused_ledger_count,
            "missing_source_count": self.missing_source_count,
            "inventory_rows": [row.as_dict() for row in self.inventory_rows],
            "result_rows": [row.as_dict() for row in self.result_rows],
            "report_json_path": str(self.report_json_path),
            "report_markdown_path": str(self.report_markdown_path),
        }


MAINLINE_LEDGER_SPECS: tuple[MainlineLedgerSpec, ...] = (
    MainlineLedgerSpec(
        ledger_name="raw_market",
        module_name="data",
        target_path_fn=raw_market_ledger_path,
        bootstrap_fn=bootstrap_raw_market_ledger,
    ),
    MainlineLedgerSpec(
        ledger_name="market_base",
        module_name="data",
        target_path_fn=market_base_ledger_path,
        bootstrap_fn=bootstrap_market_base_ledger,
    ),
    MainlineLedgerSpec(
        ledger_name="malf",
        module_name="malf",
        target_path_fn=malf_ledger_path,
        bootstrap_fn=bootstrap_malf_ledger,
    ),
    MainlineLedgerSpec(
        ledger_name="structure",
        module_name="structure",
        target_path_fn=structure_ledger_path,
        bootstrap_fn=bootstrap_structure_snapshot_ledger,
    ),
    MainlineLedgerSpec(
        ledger_name="filter",
        module_name="filter",
        target_path_fn=filter_ledger_path,
        bootstrap_fn=bootstrap_filter_snapshot_ledger,
    ),
    MainlineLedgerSpec(
        ledger_name="alpha",
        module_name="alpha",
        target_path_fn=alpha_ledger_path,
        bootstrap_fn=_bootstrap_alpha_ledger,
    ),
    MainlineLedgerSpec(
        ledger_name="position",
        module_name="position",
        target_path_fn=position_ledger_path,
        bootstrap_fn=bootstrap_position_ledger,
    ),
    MainlineLedgerSpec(
        ledger_name="portfolio_plan",
        module_name="portfolio_plan",
        target_path_fn=portfolio_plan_ledger_path,
        bootstrap_fn=bootstrap_portfolio_plan_ledger,
    ),
    MainlineLedgerSpec(
        ledger_name="trade_runtime",
        module_name="trade",
        target_path_fn=trade_runtime_ledger_path,
        bootstrap_fn=bootstrap_trade_runtime_ledger,
    ),
    MainlineLedgerSpec(
        ledger_name="system",
        module_name="system",
        target_path_fn=system_ledger_path,
        bootstrap_fn=bootstrap_system_ledger,
    ),
)

MAINLINE_LEDGER_SPEC_BY_NAME: dict[str, MainlineLedgerSpec] = {
    spec.ledger_name: spec for spec in MAINLINE_LEDGER_SPECS
}


def run_mainline_local_ledger_standardization_bootstrap(
    *,
    settings: WorkspaceRoots | None = None,
    ledgers: list[str] | tuple[str, ...] | None = None,
    source_ledger_paths: dict[str, str | Path] | None = None,
    run_id: str | None = None,
    force_copy: bool = False,
    summary_path: Path | None = None,
) -> MainlineLocalLedgerStandardizationSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    selected_ledgers = _normalize_selected_ledgers(ledgers)
    source_map = _normalize_source_map(source_ledger_paths)
    inventory_rows = _build_inventory_rows(
        settings=workspace,
        selected_ledgers=selected_ledgers,
        source_map=source_map,
    )
    effective_run_id = run_id or f"mainline-local-ledger-standardization-{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
    result_rows: list[MainlineLedgerStandardizationResult] = []
    copied_count = 0
    bootstrapped_count = 0
    reused_count = 0
    missing_source_count = 0

    for ledger_name in selected_ledgers:
        spec = MAINLINE_LEDGER_SPEC_BY_NAME[ledger_name]
        target_path = spec.target_path(workspace)
        source_path = source_map.get(ledger_name)
        migration_action = _materialize_standard_ledger(
            spec=spec,
            settings=workspace,
            source_path=source_path,
            force_copy=force_copy,
        )
        if migration_action == "copied_from_source":
            copied_count += 1
        elif migration_action == "bootstrapped_empty_target":
            bootstrapped_count += 1
        elif migration_action == "missing_source":
            missing_source_count += 1
        else:
            reused_count += 1
        table_row_counts = _load_table_row_counts(target_path) if target_path.exists() else {}
        result_rows.append(
            MainlineLedgerStandardizationResult(
                ledger_name=ledger_name,
                module_name=spec.module_name,
                source_path=source_path,
                target_path=target_path,
                migration_action=migration_action,
                exists_after=target_path.exists(),
                size_bytes_after=_path_size_bytes(target_path),
                table_count_after=len(table_row_counts),
                total_row_count_after=sum(table_row_counts.values()),
                table_row_counts=table_row_counts,
            )
        )

    report_json_path, report_markdown_path = _write_standardization_reports(
        settings=workspace,
        run_id=effective_run_id,
        inventory_rows=inventory_rows,
        result_rows=result_rows,
        summary_path=summary_path,
    )
    return MainlineLocalLedgerStandardizationSummary(
        run_id=effective_run_id,
        selected_ledger_count=len(selected_ledgers),
        copied_ledger_count=copied_count,
        bootstrapped_ledger_count=bootstrapped_count,
        reused_ledger_count=reused_count,
        missing_source_count=missing_source_count,
        inventory_rows=tuple(inventory_rows),
        result_rows=tuple(result_rows),
        report_json_path=report_json_path,
        report_markdown_path=report_markdown_path,
    )


def _normalize_selected_ledgers(ledgers: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if ledgers is None:
        return tuple(spec.ledger_name for spec in MAINLINE_LEDGER_SPECS)
    normalized = []
    seen: set[str] = set()
    for ledger_name in ledgers:
        if ledger_name not in MAINLINE_LEDGER_SPEC_BY_NAME:
            raise ValueError(f"Unknown mainline ledger: {ledger_name}")
        if ledger_name in seen:
            continue
        seen.add(ledger_name)
        normalized.append(ledger_name)
    return tuple(normalized)


def _normalize_source_map(
    source_ledger_paths: dict[str, str | Path] | None,
) -> dict[str, Path]:
    normalized: dict[str, Path] = {}
    for ledger_name, raw_path in (source_ledger_paths or {}).items():
        if ledger_name not in MAINLINE_LEDGER_SPEC_BY_NAME:
            raise ValueError(f"Unknown source ledger mapping: {ledger_name}")
        normalized[ledger_name] = Path(raw_path).resolve()
    return normalized


def _build_inventory_rows(
    *,
    settings: WorkspaceRoots,
    selected_ledgers: tuple[str, ...],
    source_map: dict[str, Path],
) -> list[MainlineLedgerInventoryRow]:
    selected_set = set(selected_ledgers)
    rows: list[MainlineLedgerInventoryRow] = []
    for spec in MAINLINE_LEDGER_SPECS:
        target_path = spec.target_path(settings)
        rows.append(
            MainlineLedgerInventoryRow(
                ledger_name=spec.ledger_name,
                module_name=spec.module_name,
                target_path=target_path,
                exists_before=target_path.exists(),
                size_bytes_before=_path_size_bytes(target_path),
                selected_for_bootstrap=spec.ledger_name in selected_set,
                source_path=source_map.get(spec.ledger_name),
            )
        )
    return rows


def _materialize_standard_ledger(
    *,
    spec: MainlineLedgerSpec,
    settings: WorkspaceRoots,
    source_path: Path | None,
    force_copy: bool,
) -> str:
    target_path = spec.target_path(settings)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path is not None:
        if not source_path.exists():
            return "missing_source"
        if source_path.resolve() != target_path.resolve():
            if force_copy or not target_path.exists():
                shutil.copy2(source_path, target_path)
                spec.bootstrap(settings)
                return "copied_from_source"
            spec.bootstrap(settings)
            return "reused_existing_target"
        spec.bootstrap(settings)
        return "reused_existing_target"
    if target_path.exists():
        spec.bootstrap(settings)
        return "reused_existing_target"
    spec.bootstrap(settings)
    return "bootstrapped_empty_target"


def _load_table_row_counts(database_path: Path) -> dict[str, int]:
    if not database_path.exists():
        return {}
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        table_names = [
            str(row[0])
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                ORDER BY table_name
                """
            ).fetchall()
        ]
        row_counts: dict[str, int] = {}
        for table_name in table_names:
            quoted_table = '"' + table_name.replace('"', '""') + '"'
            row_count = connection.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
            row_counts[table_name] = 0 if row_count is None else int(row_count[0] or 0)
        return row_counts
    finally:
        connection.close()


def _path_size_bytes(path: Path) -> int:
    return 0 if not path.exists() else int(path.stat().st_size)


def _write_standardization_reports(
    *,
    settings: WorkspaceRoots,
    run_id: str,
    inventory_rows: list[MainlineLedgerInventoryRow],
    result_rows: list[MainlineLedgerStandardizationResult],
    summary_path: Path | None,
) -> tuple[Path, Path]:
    report_root = settings.module_report_root("data") / "mainline_local_ledger_standardization"
    report_root.mkdir(parents=True, exist_ok=True)
    report_json_path = (summary_path or (report_root / f"{run_id}.json")).resolve()
    report_markdown_path = report_json_path.with_suffix(".md")
    payload = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "inventory_rows": [row.as_dict() for row in inventory_rows],
        "result_rows": [row.as_dict() for row in result_rows],
    }
    report_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    report_markdown_path.write_text(
        _render_markdown_report(run_id=run_id, inventory_rows=inventory_rows, result_rows=result_rows),
        encoding="utf-8",
    )
    return report_json_path, report_markdown_path


def _render_markdown_report(
    *,
    run_id: str,
    inventory_rows: list[MainlineLedgerInventoryRow],
    result_rows: list[MainlineLedgerStandardizationResult],
) -> str:
    lines = [
        "# mainline local ledger standardization bootstrap",
        "",
        f"- run_id: `{run_id}`",
        f"- generated_at_utc: `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "## inventory",
    ]
    for row in inventory_rows:
        lines.append(
            "- "
            + f"`{row.ledger_name}` -> `{row.target_path}` | exists_before={row.exists_before} | "
            + f"size_bytes_before={row.size_bytes_before} | selected={row.selected_for_bootstrap}"
        )
        if row.source_path is not None:
            lines.append(f"  source_path: `{row.source_path}`")
    lines.extend(["", "## results"])
    for row in result_rows:
        lines.append(
            "- "
            + f"`{row.ledger_name}` action={row.migration_action} | exists_after={row.exists_after} | "
            + f"size_bytes_after={row.size_bytes_after} | table_count_after={row.table_count_after} | "
            + f"total_row_count_after={row.total_row_count_after}"
        )
        lines.append(f"  target_path: `{row.target_path}`")
        if row.source_path is not None:
            lines.append(f"  source_path: `{row.source_path}`")
    lines.append("")
    return "\n".join(lines)
