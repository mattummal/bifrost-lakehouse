import time

import duckdb
from deltalake import DeltaTable, write_deltalake
from rich.console import Console

from bifrost.engines.spark_exec import SparkEngine
from bifrost.router import BifrostRouter, resolve_storage_target

console = Console()

DUCKDB_GOLD_MICROSTRUCTURE_SQL = """
SELECT
    symbol,
    time_bucket(INTERVAL '1 minute', to_timestamp(exchange_ts_us / 1000000.0)) AS window_start,
    COUNT(*) AS tick_count,
    ROUND(AVG((best_bid_price + best_ask_price) / 2.0), 4) AS avg_mid_price,
    ROUND(AVG(
        (best_bid_qty * best_ask_price + best_ask_qty * best_bid_price)
        / NULLIF(best_bid_qty + best_ask_qty, 0)
    ), 4) AS avg_vw_micro_price,
    ROUND(AVG(
        ((best_ask_price - best_bid_price) / ((best_bid_price + best_ask_price) / 2.0)) * 10000.0
    ), 4) AS avg_spread_bps,
    ROUND(AVG(
        (best_bid_qty - best_ask_qty) / NULLIF(best_bid_qty + best_ask_qty, 0)
    ), 4) AS avg_orderbook_imbalance
FROM silver_ticks
GROUP BY 1, 2
ORDER BY window_start DESC, symbol
"""


def run_medallion_pipeline(force_engine: str | None = None) -> None:
    router = BifrostRouter()
    t0 = time.perf_counter()

    bronze_uri, storage_opts = resolve_storage_target("bronze", "orderbook_bbo")
    silver_valid_uri, _ = resolve_storage_target("silver", "orderbook_validated")
    silver_quarantine_uri, _ = resolve_storage_target("silver", "orderbook_quarantine")
    gold_bars_uri, _ = resolve_storage_target("gold", "microstructure_1m")

    # 1. Inspect Bronze Delta Log & Determine Route
    metrics = router.inspect_and_route(bronze_uri, storage_options=storage_opts, force_engine=force_engine)

    # 2. Always run Silver ingestion contract validation via DuckDB/Arrow (Fast single-node validation)
    bronze_dt = DeltaTable(bronze_uri, storage_options=storage_opts)
    bronze_arrow = bronze_dt.to_pyarrow_table()

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
        gold_arrow = con.execute(DUCKDB_GOLD_MICROSTRUCTURE_SQL).fetch_arrow_table()
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
