from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

# Schema mirrors the Rust edge-ingester (crates/edge-ingester/src/arrow_buffer.rs).
BRONZE_SCHEMA = pa.schema(
    [
        ("exchange_ts_us", pa.int64()),
        ("ingest_ts_us", pa.int64()),
        ("symbol", pa.string()),
        ("sequence_id", pa.int64()),
        ("best_bid_price", pa.float64()),
        ("best_bid_qty", pa.float64()),
        ("best_ask_price", pa.float64()),
        ("best_ask_qty", pa.float64()),
    ]
)


def make_bronze_table(rows: list[dict[str, object]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)


def tick(
    symbol: str,
    sequence_id: int,
    bid: float,
    bid_qty: float,
    ask: float,
    ask_qty: float,
    exchange_ts_us: int,
    ingest_lag_us: int = 1000,
) -> dict[str, object]:
    return {
        "exchange_ts_us": exchange_ts_us,
        "ingest_ts_us": exchange_ts_us + ingest_lag_us,
        "symbol": symbol,
        "sequence_id": sequence_id,
        "best_bid_price": bid,
        "best_bid_qty": bid_qty,
        "best_ask_price": ask,
        "best_ask_qty": ask_qty,
    }


@pytest.fixture
def lakehouse_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Points every storage resolution (pipeline, Spark engine, Soda) at an isolated local lakehouse."""
    monkeypatch.delenv("BIFROST_STORAGE", raising=False)
    monkeypatch.delenv("BIFROST_FORCE_ENGINE", raising=False)
    monkeypatch.setenv("BIFROST_DATA_DIR", str(tmp_path))
    return tmp_path


def write_bronze(lakehouse_root: Path, rows: list[dict[str, object]]) -> Path:
    bronze_dir = lakehouse_root / "bronze" / "orderbook_bbo"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(bronze_dir), make_bronze_table(rows), mode="overwrite")
    return bronze_dir


@pytest.fixture
def sample_bronze_delta_table(lakehouse_root: Path) -> Path:
    """
    Bronze table with 3 valid ticks and 2 anomalies (1 crossed book where bid >= ask, 1 zero qty)
    to exercise the quarantine contract.
    """
    return write_bronze(
        lakehouse_root,
        [
            tick("BTCUSDT", 101, 60000.0, 2.0, 60010.0, 2.0, 1729190000000000),
            tick("BTCUSDT", 102, 60010.0, 3.0, 60020.0, 1.0, 1729190010000000),
            tick("ETHUSDT", 201, 3000.0, 10.0, 3003.0, 10.0, 1729190015000000),
            tick("BTCUSDT", 103, 60050.0, 1.0, 60000.0, 1.0, 1729190020000000),  # crossed
            tick("ETHUSDT", 202, 3000.0, 0.0, 3001.0, 5.0, 1729190025000000),  # zero bid qty
        ],
    )
