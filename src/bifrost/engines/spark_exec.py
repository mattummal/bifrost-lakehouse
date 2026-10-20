import time

from rich.console import Console

from bifrost.router import resolve_storage_target

console = Console()

SPARK_GOLD_MICROSTRUCTURE_SQL = """
SELECT
    symbol,
    date_trunc('minute', cast(from_unixtime(exchange_ts_us / 1000000.0) as timestamp)) AS window_start,
    COUNT(*) AS tick_count,
    ROUND(AVG((best_bid_price + best_ask_price) / 2.0), 4) AS avg_mid_price,
    ROUND(AVG(
        (best_bid_qty * best_ask_price + best_ask_qty * best_bid_price)
        / NULLIF(best_bid_qty + best_ask_qty, 0.0)
    ), 4) AS avg_vw_micro_price,
    ROUND(AVG(
        ((best_ask_price - best_bid_price) / ((best_bid_price + best_ask_price) / 2.0)) * 10000.0
    ), 4) AS avg_spread_bps,
    ROUND(AVG(
        (best_bid_qty - best_ask_qty) / NULLIF(best_bid_qty + best_ask_qty, 0.0)
    ), 4) AS avg_orderbook_imbalance
FROM silver_ticks
GROUP BY 1, 2
ORDER BY window_start DESC, symbol
"""


class SparkEngine:
    """Distributed Scale-Path Engine utilizing PySpark and Delta-Spark."""

    def __init__(self) -> None:
        pass

    def run_transformations(self, storage_options: dict[str, str] | None = None) -> None:
        try:
            from delta import configure_spark_with_delta_pip
            from pyspark.sql import SparkSession
        except ImportError as e:
            console.print(
                "[bold red]Error:[/bold red] PySpark or delta-spark not installed. "
                "Run `uv sync --extra spark` to install them."
            )
            raise e

        t0 = time.perf_counter()
        console.print("[bold blue]⚡ Initializing PySpark Distributed Cluster Session...[/bold blue]")

        builder = (
            SparkSession.builder.appName("Bifrost-ScaleEngine-PySpark")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        silver_valid_uri, _ = resolve_storage_target("silver", "orderbook_validated")
        gold_bars_uri, _ = resolve_storage_target("gold", "microstructure_1m")

        # Read Delta table into Spark DataFrame
        df_silver = spark.read.format("delta").load(silver_valid_uri)
        df_silver.createOrReplaceTempView("silver_ticks")

        console.print("[cyan]Executing Microstructure Aggregations via Spark SQL...[/cyan]")
        df_gold = spark.sql(SPARK_GOLD_MICROSTRUCTURE_SQL)

        # Write Delta Lake Gold table
        df_gold.write.format("delta").mode("overwrite").save(gold_bars_uri)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        console.print(
            f"[bold green]✓ PySpark Scale-Path Complete:[/bold green] "
            f"Gold bars committed to [bold]{gold_bars_uri}[/bold] in {elapsed_ms:.1f} ms\n"
        )

        df_gold.show(10, truncate=False)
        spark.stop()
