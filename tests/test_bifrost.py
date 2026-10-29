from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from deltalake import DeltaTable

from bifrost.pipeline import run_medallion_pipeline
from bifrost.query import get_gold_query
from bifrost.router import BifrostRouter
from tests.conftest import make_bronze_table, tick


def test_sqlglot_transpilation_duckdb_to_spark() -> None:
    """Verifies that the canonical DuckDB query transpiles cleanly into valid Spark SQL."""
    duck_sql = get_gold_query(target_dialect="duckdb")
    spark_sql = get_gold_query(target_dialect="spark")

    assert "FROM silver_ticks" in duck_sql
    assert "FROM silver_ticks" in spark_sql
    # make_timestamp(µs) maps to Spark's TIMESTAMP_MICROS (no seconds truncation / string round-trip)
    assert "TIMESTAMP_MICROS(EXCHANGE_TS_US)" in spark_sql.upper()
    assert "FROM_UNIXTIME" not in spark_sql.upper()
    assert "AVG_VW_MICRO_PRICE" in spark_sql.upper()


def test_router_selects_duckdb_for_small_payload(sample_bronze_delta_table: Path) -> None:
    """Verifies cost-based routing selects DuckDB when Delta table is below threshold."""
    router = BifrostRouter(duckdb_max_gb=25.0, duckdb_max_files=500)
    metrics = router.inspect_and_route(str(sample_bronze_delta_table))

    assert metrics.snapshot_version == 0
    assert metrics.num_files == 1
    assert metrics.total_bytes > 0
    assert metrics.selected_engine == "duckdb"


def test_router_switches_to_pyspark_when_threshold_exceeded(
    sample_bronze_delta_table: Path,
) -> None:
    """Verifies router automatically switches to PySpark when file count or byte threshold is breached."""
    # Set max_files=0 so even 1 file triggers the distributed scale path
    router = BifrostRouter(duckdb_max_gb=25.0, duckdb_max_files=0)
    metrics = router.inspect_and_route(str(sample_bronze_delta_table))

    assert metrics.selected_engine == "pyspark"
    assert "Scale path" in metrics.reason


def test_router_respects_cli_force_override(sample_bronze_delta_table: Path) -> None:
    """Verifies explicit --engine override takes precedence over volume heuristics."""
    router = BifrostRouter()
    metrics = router.inspect_and_route(str(sample_bronze_delta_table), force_engine="pyspark")

    assert metrics.selected_engine == "pyspark"
    assert "Explicitly forced" in metrics.reason


@pytest.mark.parametrize("session_tz", ["UTC", "Asia/Kolkata", "America/New_York"])
def test_gold_windows_are_utc_regardless_of_session_timezone(session_tz: str) -> None:
    """1m windows must bucket on UTC wall-clock, not the host/DuckDB session timezone."""
    con = duckdb.connect()
    con.execute(f"SET TimeZone = '{session_tz}'")
    con.register("silver_ticks", make_bronze_table([tick("BTCUSDT", 1, 100.0, 1.0, 101.0, 1.0, 1729190000123456)]))

    (window_start,) = con.execute(f"SELECT window_start FROM ({get_gold_query('duckdb')})").fetchone() or (None,)

    # 1729190000.123456 s == 2024-10-17 18:33:20.123456 UTC
    assert window_start == datetime(2024, 10, 17, 18, 33)


def test_end_to_end_medallion_duckdb_contracts_and_microstructure(
    sample_bronze_delta_table: Path,
) -> None:
    """
    End-to-end integration test verifying:
    1. Silver contract splits valid rows (3) from quarantined anomalies (2).
    2. Gold microstructure calculations produce exact Mid-Price, OBI, and VWMP metrics.
    """
    lakehouse_root = sample_bronze_delta_table.parent.parent

    # Execute pipeline on DuckDB fast path
    run_medallion_pipeline(force_engine="duckdb")

    # 1. Verify Silver Validated Table (3 valid rows)
    silver_valid_dt = DeltaTable(str(lakehouse_root / "silver" / "orderbook_validated"))
    valid_df = pa.table(silver_valid_dt.to_pyarrow_table()).to_pydict()
    assert len(valid_df["symbol"]) == 3

    # 2. Verify Silver Quarantine Table (2 anomalous rows isolated)
    silver_quarantine_dt = DeltaTable(str(lakehouse_root / "silver" / "orderbook_quarantine"))
    quarantine_df = pa.table(silver_quarantine_dt.to_pyarrow_table()).to_pydict()
    assert len(quarantine_df["symbol"]) == 2
    assert all(r == "INVALID_ORDERBOOK" for r in quarantine_df["quarantine_reason"])

    # 3. Verify Gold Microstructure Bars (1m aggregations for BTCUSDT and ETHUSDT)
    gold_dt = DeltaTable(str(lakehouse_root / "gold" / "microstructure_1m"))
    gold_arrow = pa.table(gold_dt.to_pyarrow_table())

    con = duckdb.connect()
    con.register("gold", gold_arrow)
    eth_row = con.execute("SELECT * FROM gold WHERE symbol = 'ETHUSDT'").fetchone()

    assert eth_row is not None
    # ETHUSDT had Bid=3000.0 (Qty 10), Ask=3003.0 (Qty 10) -> Mid=3001.5, OBI=0.0
    symbol, _, tick_count, avg_mid, avg_vwmp, avg_spread_bps, avg_obi = eth_row
    assert symbol == "ETHUSDT"
    assert tick_count == 1
    assert avg_mid == 3001.5
    assert avg_vwmp == 3001.5
    assert avg_obi == 0.0
