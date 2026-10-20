import os
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable
from rich.console import Console
from rich.table import Table

console = Console()


@dataclass(frozen=True)
class TableMetrics:
    table_uri: str
    snapshot_version: int
    num_files: int
    total_bytes: int
    selected_engine: str
    reason: str


def resolve_storage_target(layer: str, table_name: str) -> tuple[str, dict[str, str]]:
    """
    Resolves storage URI and credentials across Local, Azurite, and Azure ADLS Gen2.
    """
    backend = os.getenv("BIFROST_STORAGE", "local").lower()

    if backend == "azurite":
        uri = f"az://{layer}/{table_name}"
        opts = {
            "account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1"),
            "account_key": ("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="),
            "use_emulator": "true",
        }
        return uri, opts

    if backend == "azure":
        account = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
        uri = f"abfss://{layer}@{account}.dfs.core.windows.net/{table_name}"
        opts = {"account_name": account, "use_azure_cli": "true"}
        return uri, opts

    # Default: Local filesystem lakehouse
    base_dir = Path(os.getenv("BIFROST_DATA_DIR", ".lakehouse_data")).resolve()
    target_path = base_dir / layer / table_name
    target_path.parent.mkdir(parents=True, exist_ok=True)
    return str(target_path), {}


class BifrostRouter:
    """
    Cost-based adaptive compute router that inspects Delta Lake transaction log metadata
    (_delta_log) to route between single-node vectorized execution (DuckDB) and
    distributed execution (PySpark).
    """

    def __init__(self, duckdb_max_gb: float = 25.0, duckdb_max_files: int = 500) -> None:
        self.duckdb_max_bytes = int(duckdb_max_gb * (1024**3))
        self.duckdb_max_files = duckdb_max_files

    def inspect_and_route(
        self,
        table_uri: str,
        storage_options: dict[str, str] | None = None,
        force_engine: str | None = None,
    ) -> TableMetrics:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        version = dt.version()

        actions_tbl = dt.get_add_actions(flatten=True)
        add_actions = pa.table(actions_tbl).to_pydict()

        sizes = add_actions.get("size_bytes") or add_actions.get("size") or []
        num_files = len(sizes)
        total_bytes = sum(int(s) for s in sizes if s is not None)

        env_force = force_engine or os.getenv("BIFROST_FORCE_ENGINE")

        if env_force in ("duckdb", "pyspark"):
            selected_engine = env_force
            reason = f"Explicitly forced via CLI (--engine={env_force})"
        elif total_bytes <= self.duckdb_max_bytes and num_files <= self.duckdb_max_files:
            selected_engine = "duckdb"
            reason = (
                f"Fast path: Payload ({self._fmt_bytes(total_bytes)}, {num_files} files) "
                f"fits in single-node vector memory (< {self.duckdb_max_bytes // (1024**3)} GB)"
            )
        else:
            selected_engine = "pyspark"
            reason = (
                f"Scale path: Payload ({self._fmt_bytes(total_bytes)}, {num_files} files) "
                f"exceeds single-node threshold; routing to distributed Spark"
            )

        metrics = TableMetrics(
            table_uri=table_uri,
            snapshot_version=version,
            num_files=num_files,
            total_bytes=total_bytes,
            selected_engine=selected_engine,
            reason=reason,
        )
        self._render_decision_table(metrics)
        return metrics

    @staticmethod
    def _fmt_bytes(num_bytes: int) -> str:
        val = float(num_bytes)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if val < 1024.0:
                return f"{val:.1f} {unit}"
            val /= 1024.0
        return f"{val:.1f} PB"

    @staticmethod
    def _render_decision_table(m: TableMetrics) -> None:
        table = Table(title="Bifrost Adaptive Compute Routing Decision", show_lines=True)
        table.add_column("Delta Table Target", style="cyan")
        table.add_column("Snapshot Ver", justify="right", style="magenta")
        table.add_column("Parquet Files", justify="right", style="green")
        table.add_column("Payload Size", justify="right", style="yellow")
        table.add_column("Target Engine", style="bold red" if m.selected_engine == "pyspark" else "bold green")
        table.add_column("Routing Heuristic", style="white")

        short_uri = f".../{'/'.join(Path(m.table_uri).parts[-2:])}" if len(m.table_uri) > 30 else m.table_uri
        table.add_row(
            short_uri,
            str(m.snapshot_version),
            str(m.num_files),
            BifrostRouter._fmt_bytes(m.total_bytes),
            m.selected_engine.upper(),
            m.reason,
        )
        console.print(table)
