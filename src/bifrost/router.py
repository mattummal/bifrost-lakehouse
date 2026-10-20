import os
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable
from rich.console import Console
from rich.table import Table

console = Console()

AZURITE_STORAGE_OPTIONS: dict[str, str] = {
    "azure_storage_account_name": "devstoreaccount1",
    "azure_storage_account_key": (
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    ),
    "azure_storage_use_emulator": "true",
    "azure_storage_allow_http": "true",
}


def resolve_storage_target(layer: str, table: str) -> tuple[str, dict[str, str] | None]:
    """Resolves URI and storage options across local_fs, azurite, and production Azure ADLS."""
    backend = os.getenv("BIFROST_STORAGE", "local")
    if backend == "azurite":
        return f"az://{layer}/{table}", AZURITE_STORAGE_OPTIONS
    elif backend == "azure":
        return f"az://{layer}/{table}", {
            "azure_storage_account_name": os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        }
    else:
        repo_root = Path(__file__).resolve().parents[2]
        target_dir = repo_root / ".lakehouse_data" / layer / table
        target_dir.mkdir(parents=True, exist_ok=True)
        return str(target_dir), None


@dataclass
class TableMetrics:
    uri: str
    version: int
    file_count: int
    total_bytes: int
    selected_engine: str
    reason: str


class BifrostRouter:
    """Inspects Delta Lake transaction logs to route workloads between DuckDB and Spark."""

    def __init__(self, scale_threshold_gb: float = 25.0):
        self.scale_threshold_bytes = int(scale_threshold_gb * (1024**3))

    def inspect_and_route(
        self,
        table_uri: str,
        storage_options: dict[str, str] | None = None,
        force_engine: str | None = None,
    ) -> TableMetrics:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        actions_tbl = dt.get_add_actions(flatten=True)

        # Safely convert arro3/PyArrow action table to a Python dictionary
        add_actions = pa.table(actions_tbl).to_pydict()

        sizes = add_actions.get("size_bytes", [])
        file_count = len(sizes)
        total_bytes = sum(sizes)

        if force_engine:
            selected = force_engine
            reason = f"Explicitly forced via CLI (--engine={force_engine})"
        elif total_bytes < self.scale_threshold_bytes:
            selected = "duckdb"
            reason = (
                f"Dataset ({total_bytes / (1024**2):.2f} MB across {file_count} files) "
                f"< {self.scale_threshold_bytes / (1024**3):.0f} GB threshold -> In-Process Vectorized"
            )
        else:
            selected = "pyspark"
            reason = "Dataset exceeds single-node memory threshold -> Distributed Shuffle"

        metrics = TableMetrics(
            uri=table_uri,
            version=dt.version(),
            file_count=file_count,
            total_bytes=total_bytes,
            selected_engine=selected,
            reason=reason,
        )
        self._print_routing_decision(metrics)
        return metrics

    @staticmethod
    def _print_routing_decision(m: TableMetrics) -> None:
        table = Table(title="Bifrost Adaptive Compute Routing Decision", show_header=True)
        table.add_column("Delta Table Target", style="cyan")
        table.add_column("Snapshot Ver", justify="right")
        table.add_column("Parquet Files", justify="right")
        table.add_column("Payload Size", justify="right")
        table.add_column("Target Engine", style="bold green")
        table.add_column("Routing Heuristic")

        short_uri = m.uri if m.uri.startswith("az://") else f".../{Path(m.uri).parent.name}/{Path(m.uri).name}"
        table.add_row(
            short_uri,
            str(m.version),
            str(m.file_count),
            f"{m.total_bytes / 1024:.1f} KB",
            m.selected_engine.upper(),
            m.reason,
        )
        console.print(table)
