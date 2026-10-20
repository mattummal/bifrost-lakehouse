// Implement the Zero-Copy Arrow Buffer (src/arrow_buffer.rs)
// Instead of writing individual JSON ticks, we append orderbook snapshots
// directly into columnar Arrow builders and flush a RecordBatch.

use std::sync::Arc;
use deltalake::arrow::array::{
    Float64Builder, Int64Builder, StringBuilder, UInt64Builder,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use anyhow::Result;

pub struct OrderbookTick {
    pub exchange_ts_us: i64,
    pub ingest_ts_us: i64,
    pub symbol: String,
    pub sequence_id: u64,
    pub best_bid_price: f64,
    pub best_bid_qty: f64,
    pub best_ask_price: f64,
    pub best_ask_qty: f64,
}

pub struct ArrowOrderbookBuffer {
    schema: Arc<Schema>,
    exchange_ts: Int64Builder,
    ingest_ts: Int64Builder,
    symbol: StringBuilder,
    sequence_id: UInt64Builder,
    best_bid_price: Float64Builder,
    best_bid_qty: Float64Builder,
    best_ask_price: Float64Builder,
    best_ask_qty: Float64Builder,
    row_count: usize,
}

impl ArrowOrderbookBuffer {
    pub fn new(capacity: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("exchange_ts_us", DataType::Int64, false),
            Field::new("ingest_ts_us", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("sequence_id", DataType::UInt64, false),
            Field::new("best_bid_price", DataType::Float64, false),
            Field::new("best_bid_qty", DataType::Float64, false),
            Field::new("best_ask_price", DataType::Float64, false),
            Field::new("best_ask_qty", DataType::Float64, false),
        ]));

        Self {
            schema,
            exchange_ts: Int64Builder::with_capacity(capacity),
            ingest_ts: Int64Builder::with_capacity(capacity),
            symbol: StringBuilder::with_capacity(capacity, capacity * 8),
            sequence_id: UInt64Builder::with_capacity(capacity),
            best_bid_price: Float64Builder::with_capacity(capacity),
            best_bid_qty: Float64Builder::with_capacity(capacity),
            best_ask_price: Float64Builder::with_capacity(capacity),
            best_ask_qty: Float64Builder::with_capacity(capacity),
            row_count: 0,
        }
    }

    pub fn push(&mut self, tick: &OrderbookTick) {
        self.exchange_ts.append_value(tick.exchange_ts_us);
        self.ingest_ts.append_value(tick.ingest_ts_us);
        self.symbol.append_value(&tick.symbol);
        self.sequence_id.append_value(tick.sequence_id);
        self.best_bid_price.append_value(tick.best_bid_price);
        self.best_bid_qty.append_value(tick.best_bid_qty);
        self.best_ask_price.append_value(tick.best_ask_price);
        self.best_ask_qty.append_value(tick.best_ask_qty);
        self.row_count += 1;
    }

    pub fn len(&self) -> usize {
        self.row_count
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn flush(&mut self) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.exchange_ts.finish()),
                Arc::new(self.ingest_ts.finish()),
                Arc::new(self.symbol.finish()),
                Arc::new(self.sequence_id.finish()),
                Arc::new(self.best_bid_price.finish()),
                Arc::new(self.best_bid_qty.finish()),
                Arc::new(self.best_ask_price.finish()),
                Arc::new(self.best_ask_qty.finish()),
            ],
        )?;
        self.row_count = 0;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::AsArray;
    use deltalake::arrow::datatypes::{Float64Type, Int64Type, UInt64Type};

    #[test]
    fn test_arrow_buffer_push_and_flush() {
        let mut buffer = ArrowOrderbookBuffer::new(10);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());

        // Push two valid ticks via OrderbookTick struct
        buffer.push(&OrderbookTick {
            exchange_ts_us: 1700000000000000,
            ingest_ts_us: 1700000000000100,
            symbol: "BTCUSDT".to_string(),
            sequence_id: 1001,
            best_bid_price: 50000.0,
            best_bid_qty: 1.5,
            best_ask_price: 50001.0,
            best_ask_qty: 2.0,
        });

        buffer.push(&OrderbookTick {
            exchange_ts_us: 1700000000000500,
            ingest_ts_us: 1700000000000600,
            symbol: "ETHUSDT".to_string(),
            sequence_id: 2002,
            best_bid_price: 3000.0,
            best_bid_qty: 10.0,
            best_ask_price: 3000.5,
            best_ask_qty: 5.0,
        });

        assert_eq!(buffer.len(), 2);
        assert!(!buffer.is_empty());

        // Flush into RecordBatch
        let batch = buffer.flush().expect("Failed to flush RecordBatch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 8);

        // Verify buffer is reset after flush
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());

        // Verify columnar values inside the RecordBatch match field indices
        let exch_ts = batch.column(0).as_primitive::<Int64Type>();
        assert_eq!(exch_ts.value(0), 1700000000000000);
        assert_eq!(exch_ts.value(1), 1700000000000500);

        let symbols = batch.column(2).as_string::<i32>();
        assert_eq!(symbols.value(0), "BTCUSDT");
        assert_eq!(symbols.value(1), "ETHUSDT");

        let seq_ids = batch.column(3).as_primitive::<UInt64Type>();
        assert_eq!(seq_ids.value(0), 1001);
        assert_eq!(seq_ids.value(1), 2002);

        let bids = batch.column(4).as_primitive::<Float64Type>();
        assert_eq!(bids.value(0), 50000.0);
        assert_eq!(bids.value(1), 3000.0);
    }
}