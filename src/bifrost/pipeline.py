import os
import time
from collections.abc import Mapping
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from rich.console import Console
from soda.scan import Scan

from bifrost.engines.spark_exec import SparkEngine
from bifrost.query import get_gold_query
from bifrost.router import BifrostRouter, resolve_storage_target

console = Console()


# Soda dataset name -> (medallion layer, Delta table name)
SODA_CONTRACT_TABLES: dict[str, tuple[str, str]] = {
    "bronze_orderbook_bbo": ("bronze", "orderbook_bbo"),
    "silver_orderbook_validated": ("silver", "orderbook_validated"),
    "gold_microstructure_1m": ("gold", "microstructure_1m"),
}

DEFAULT_CONTRACT_PATH = Path(os.getenv("BIFROST_CONTRACT_PATH", "bifrost_contract.yml"))


def run_bifrost_soda_validation(
    table_uris: Mapping[str, str] | None = None,
    storage_options: dict[str, str] | None = None,
    contract_path: str | Path = DEFAULT_CONTRACT_PATH,
) -> bool:
    """
    Runs the Soda contract against the Bronze, Silver and Gold Delta tables.

    ``table_uris`` maps Soda dataset names to Delta table URIs; when omitted they are resolved
    from the active storage backend (``BIFROST_STORAGE`` / ``BIFROST_DATA_DIR``).
    """
    if table_uris is None:
        table_uris = {}
        for dataset, (layer, table_name) in SODA_CONTRACT_TABLES.items():
            table_uris[dataset], resolved_opts = resolve_storage_target(layer, table_name)
            storage_options = storage_options or resolved_opts or None

    missing = set(SODA_CONTRACT_TABLES) - set(table_uris)
    if missing:
        raise ValueError(f"Missing table URIs for contract datasets: {sorted(missing)}")

    if not Path(contract_path).is_file():
        raise FileNotFoundError(f"Soda contract not found: {contract_path}")

    # Zero-copy views over the Delta tables in an in-memory DuckDB; nothing is materialised to disk.
    con = duckdb.connect()
    try:
        for dataset, uri in table_uris.items():
            con.register(dataset, DeltaTable(uri, storage_options=storage_options).to_pyarrow_dataset())

        scan = Scan()
        scan.set_data_source_name("bifrost_duckdb")
        scan.add_duckdb_connection(con, data_source_name="bifrost_duckdb")
        scan.add_sodacl_yaml_file(str(contract_path))
        scan.execute()
        print(scan.get_logs_text())
    finally:
        con.close()

    if scan.has_error_logs():
        return False

    return not scan.has_check_fails()


def run_medallion_pipeline(force_engine: str | None = None) -> None:
    router = BifrostRouter()
    t0 = time.perf_counter()

    bronze_uri, storage_opts = resolve_storage_target("bronze", "orderbook_bbo")
    silver_valid_uri, _ = resolve_storage_target("silver", "orderbook_validated")
    silver_quarantine_uri, _ = resolve_storage_target("silver", "orderbook_quarantine")
    gold_bars_uri, _ = resolve_storage_target("gold", "microstructure_1m")

    # 1. Inspect Bronze Delta Log & Determine Route
    metrics = router.inspect_and_route(bronze_uri, storage_options=storage_opts, force_engine=force_engine)

    # 2. Run Silver ingestion contract validation via DuckDB/Arrow
    bronze_dt = DeltaTable(bronze_uri, storage_options=storage_opts)
    bronze_arrow = pa.table(bronze_dt.to_pyarrow_table())

    con = duckdb.connect()
    con.register("bronze_ticks", bronze_arrow)

    valid_arrow = con.execute("""
        SELECT *, (best_bid_price + best_ask_price) / 2.0 AS mid_price
        FROM bronze_ticks
        WHERE best_bid_price > 0 AND best_ask_price > best_bid_price
          AND best_bid_qty > 0 AND best_ask_qty > 0
    """).to_arrow_table()

    quarantine_arrow = con.execute("""
        SELECT *, 'INVALID_ORDERBOOK' AS quarantine_reason
        FROM bronze_ticks
        WHERE NOT (best_bid_price > 0 AND best_ask_price > best_bid_price
                   AND best_bid_qty > 0 AND best_ask_qty > 0)
    """).to_arrow_table()

    # Silver is rebuilt from the full Bronze snapshot, so quarantine is overwritten in lockstep
    # (even when empty) to keep reruns idempotent.
    write_deltalake(silver_valid_uri, valid_arrow, mode="overwrite", schema_mode="overwrite", storage_options=storage_opts)
    write_deltalake(
        silver_quarantine_uri,
        quarantine_arrow,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=storage_opts,
    )

    console.print(
        f"[green]✓ Silver Contract Enforced:[/green] {valid_arrow.num_rows} valid rows | "
        f"{quarantine_arrow.num_rows} quarantined."
    )

    # 3. Route Gold Microstructure Aggregations based on router metrics
    con.register("silver_ticks", valid_arrow)

    if metrics.selected_engine == "duckdb":
        console.print("[bold yellow]🚀 Executing Fast Path via DuckDB Vectorized Engine...[/bold yellow]")
        duckdb_sql = get_gold_query(target_dialect="duckdb")
        gold_arrow = con.execute(duckdb_sql).to_arrow_table()
        write_deltalake(
            gold_bars_uri,
            gold_arrow,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=storage_opts,
        )

        elapsed_ms = (time.perf_counter() - t0) * 1000
        console.print(
            f"[bold green]✓ DuckDB Gold Path Complete:[/bold green] {gold_arrow.num_rows} bars in {elapsed_ms:.1f} ms\n"
        )
        con.sql("SELECT * FROM gold_arrow LIMIT 10").show()

    else:
        console.print("[bold magenta]🌐 Executing Scale Path via PySpark Distributed Engine...[/bold magenta]")
        spark_engine = SparkEngine()
        spark_engine.run_transformations(storage_options=storage_opts)

    # 4. Run Soda Core Multi-Tier Data Contracts across Bronze, Silver, and Gold
    console.print("\n[bold cyan]🔍 Executing Soda Core Data Contract Validation across all tiers...[/bold cyan]")
    contracts_passed = run_bifrost_soda_validation(
        table_uris={
            "bronze_orderbook_bbo": bronze_uri,
            "silver_orderbook_validated": silver_valid_uri,
            "gold_microstructure_1m": gold_bars_uri,
        },
        storage_options=storage_opts or None,
    )

    if not contracts_passed:
        raise RuntimeError("❌ Soda Core data contract validation failed! Check logs above.")

    console.print("[bold green]✓ All Data Contracts Passed Successfully![/bold green]")
