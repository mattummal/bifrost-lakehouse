import os
import time
from pathlib import Path

from rich.console import Console
from rich.syntax import Syntax

from bifrost.query import get_gold_query
from bifrost.router import resolve_storage_target

console = Console()


class SparkEngine:
    """Distributed Scale-Path Engine utilising PySpark and Delta-Spark."""

    def __init__(self) -> None:
        pass

    def run_transformations(self, storage_options: dict[str, str] | None = None) -> None:
        # Auto-heal JAVA_HOME on macOS Apple Silicon if unset or pointing to a dead Coursier path
        current_java = os.environ.get("JAVA_HOME", "")
        if not current_java or "Coursier" in current_java or not Path(current_java).exists():
            for candidate in [
                "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
                "/opt/homebrew/opt/openjdk@21",
                "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
                "/opt/homebrew/opt/openjdk@17",
            ]:
                if Path(candidate).exists():
                    os.environ["JAVA_HOME"] = candidate
                    break
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

        # Transpile DuckDB SQL -> Spark SQL via SQLGlot AST
        spark_sql = get_gold_query(target_dialect="spark")
        console.print("[dim]🔄 SQLGlot Transpiled Query (DuckDB -> Spark SQL):[/dim]")
        console.print(Syntax(spark_sql, "sql", theme="monokai", word_wrap=True))

        # Java 17/21 module flags required by Apache Spark & Arrow
        jvm_opts = (
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
            "--add-opens=java.base/java.io=ALL-UNNAMED "
            "--add-opens=java.base/java.net=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
        )

        builder = (
            SparkSession.builder.appName("Bifrost-ScaleEngine-PySpark")
            .master("local[*]")
            .config("spark.driver.extraJavaOptions", jvm_opts)
            .config("spark.executor.extraJavaOptions", jvm_opts)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.session.timeZone", "UTC")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        silver_valid_uri, _ = resolve_storage_target("silver", "orderbook_validated")
        gold_bars_uri, _ = resolve_storage_target("gold", "microstructure_1m")

        # Read Delta table into Spark DataFrame
        df_silver = spark.read.format("delta").load(silver_valid_uri)
        df_silver.createOrReplaceTempView("silver_ticks")

        console.print("[cyan]Executing Microstructure Aggregations via Spark SQL...[/cyan]")
        df_gold = spark.sql(spark_sql)

        # Write Delta Lake Gold table
        df_gold.write.format("delta").mode("overwrite").save(gold_bars_uri)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        console.print(
            f"[bold green]✓ PySpark Scale-Path Complete:[/bold green] "
            f"Gold bars committed to [bold]{gold_bars_uri}[/bold] in {elapsed_ms:.1f} ms\n"
        )

        df_gold.show(10, truncate=False)
        spark.stop()
