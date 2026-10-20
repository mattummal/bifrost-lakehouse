import time

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from rich.console import Console

from bifrost.engines.spark_exec import SparkEngine
from bifrost.query import get_gold_query
from bifrost.router import BifrostRouter, resolve_storage_target

console = Console()


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
    """).fetch_arrow_table()

    quarantine_arrow = con.execute("""
        SELECT *, 'INVALID_ORDERBOOK' AS quarantine_reason
        FROM bronze_ticks
        WHERE NOT (best_bid_price > 0 AND best_ask_price > best_bid_price
                   AND best_bid_qty > 0 AND best_ask_qty > 0)
    """).fetch_arrow_table()

    write_deltalake(silver_valid_uri, valid_arrow, mode="overwrite", storage_options=storage_opts)
    if quarantine_arrow.num_rows > 0:
        write_deltalake(silver_quarantine_uri, quarantine_arrow, mode="append", storage_options=storage_opts)

    console.print(
        f"[green]✓ Silver Contract Enforced:[/green] {valid_arrow.num_rows} valid rows | "
        f"{quarantine_arrow.num_rows} quarantined."
    )

    # 3. Route Gold Microstructure Aggregations based on router metrics
    con.register("silver_ticks", valid_arrow)

    if metrics.selected_engine == "duckdb":
        console.print("[bold yellow]🚀 Executing Fast Path via DuckDB Vectorized Engine...[/bold yellow]")
        duckdb_sql = get_gold_query(target_dialect="duckdb")
        gold_arrow = con.execute(duckdb_sql).fetch_arrow_table()
        write_deltalake(gold_bars_uri, gold_arrow, mode="overwrite", storage_options=storage_opts)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        console.print(
            f"[bold green]✓ DuckDB Gold Path Complete:[/bold green] {gold_arrow.num_rows} bars in {elapsed_ms:.1f} ms\n"
        )
        con.sql("SELECT * FROM gold_arrow LIMIT 10").show()

    else:
        console.print("[bold magenta]🌐 Executing Scale Path via PySpark Distributed Engine...[/bold magenta]")
        spark_engine = SparkEngine()
        spark_engine.run_transformations(storage_options=storage_opts)
