// Implement the Live WebSocket + Delta Lake Writer (src/main.rs)
// This connects to the live Binance USD-M futures @bookTicker stream for
// BTCUSDT, ETHUSDT, and SOLUSDT, micro-batches events in memory, and
// commits compacted Parquet files + _delta_log transactions to the configured storage.
//----------------------------------------------
// automatically creates and writes to
// .lakehouse_data/bronze/orderbook_bbo by default,
// or switches to az:// when BIFROST_STORAGE=azurite or azure:
//----------------------------------------------
mod arrow_buffer;

use std::collections::HashMap;
use std::time::Duration;
use anyhow::{bail, Context, Result};
use arrow_buffer::{ArrowOrderbookBuffer, OrderbookTick};
use chrono::Utc;
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::time::interval;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

// USD-M futures bookTicker carries exchange timestamps (E/T); the spot stream does not.
const BINANCE_WS_URL: &str =
    "wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker/solusdt@bookTicker";
const BATCH_SIZE_THRESHOLD: usize = 500;
const FLUSH_INTERVAL_SECS: u64 = 10;

#[derive(Debug, Deserialize)]
struct CombinedStreamWrapper {
    data: BookTickerEvent,
}

#[derive(Debug, Deserialize)]
struct BookTickerEvent {
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s")]
    symbol: String,
    /// Matching-engine transaction time (ms since epoch).
    #[serde(rename = "T")]
    transaction_time_ms: i64,
    #[serde(rename = "b")]
    best_bid_price: String,
    #[serde(rename = "B")]
    best_bid_qty: String,
    #[serde(rename = "a")]
    best_ask_price: String,
    #[serde(rename = "A")]
    best_ask_qty: String,
}

fn parse_price_or_qty(field: &str, raw: &str) -> Result<f64> {
    let value: f64 = raw
        .parse()
        .with_context(|| format!("field `{field}` is not a number: {raw:?}"))?;
    if !value.is_finite() {
        bail!("field `{field}` is not finite: {raw:?}");
    }
    Ok(value)
}

/// Parses a combined-stream bookTicker message into a tick. Malformed numeric fields are
/// rejected rather than coerced, so they surface as parse errors instead of fake quarantine rows.
fn parse_tick(text: &str, ingest_ts_us: i64) -> Result<OrderbookTick> {
    let ev = serde_json::from_str::<CombinedStreamWrapper>(text)
        .context("invalid bookTicker payload")?
        .data;
    let exchange_ts_us = ev
        .transaction_time_ms
        .checked_mul(1_000)
        .context("transaction time overflows microseconds")?;

    Ok(OrderbookTick {
        exchange_ts_us,
        ingest_ts_us,
        best_bid_price: parse_price_or_qty("b", &ev.best_bid_price)?,
        best_bid_qty: parse_price_or_qty("B", &ev.best_bid_qty)?,
        best_ask_price: parse_price_or_qty("a", &ev.best_ask_price)?,
        best_ask_qty: parse_price_or_qty("A", &ev.best_ask_qty)?,
        symbol: ev.symbol,
        sequence_id: ev.update_id,
    })
}

fn azurite_storage_options() -> HashMap<String, String> {
    let mut opts = HashMap::new();
    opts.insert("azure_storage_account_name".to_string(), "devstoreaccount1".to_string());
    opts.insert(
        "azure_storage_account_key".to_string(),
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_string(),
    );
    opts.insert("azure_storage_use_emulator".to_string(), "true".to_string());
    opts.insert("azure_storage_allow_http".to_string(), "true".to_string());
    opts
}

fn resolve_table_target() -> (String, HashMap<String, String>) {
    match std::env::var("BIFROST_STORAGE").as_deref() {
        Ok("azurite") => {
            deltalake::azure::register_handlers(None);
            ("az://bronze/orderbook_bbo".to_string(), azurite_storage_options())
        }
        Ok("azure") => {
            deltalake::azure::register_handlers(None);
            let mut opts = HashMap::new();
            if let Ok(acct) = std::env::var("AZURE_STORAGE_ACCOUNT_NAME") {
                opts.insert("azure_storage_account_name".to_string(), acct);
            }
            ("az://bronze/orderbook_bbo".to_string(), opts)
        }
        _ => {
            // Default: Zero-dependency local filesystem Delta table
            let mut path = std::env::current_dir().expect("Failed to get current dir");
            if path.ends_with("crates/edge-ingester") {
                path = path.join("../..");
            }
            let table_path = path.join(".lakehouse_data/bronze/orderbook_bbo");
            std::fs::create_dir_all(&table_path).expect("Failed to create local bronze directory");
            let canonical = table_path.canonicalize().unwrap_or(table_path);
            (canonical.to_string_lossy().to_string(), HashMap::new())
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let (table_uri, storage_opts) = resolve_table_target();
    info!("Storage target initialized at: {}", table_uri);

    info!("Connecting to Binance live orderbook stream...");
    let (ws_stream, _) = connect_async(BINANCE_WS_URL).await?;
    let (_, mut read) = ws_stream.split();

    let mut buffer = ArrowOrderbookBuffer::new(BATCH_SIZE_THRESHOLD);
    let mut flush_timer = interval(Duration::from_secs(FLUSH_INTERVAL_SECS));

    info!("Ingesting live ticks into Arrow memory buffer...");

    let mut parse_errors: u64 = 0;

    loop {
        tokio::select! {
            Some(msg) = read.next() => {
                if let Ok(Message::Text(text)) = msg {
                    match parse_tick(&text, Utc::now().timestamp_micros()) {
                        Ok(tick) => {
                            buffer.push(&tick);
                            if buffer.len() >= BATCH_SIZE_THRESHOLD {
                                flush_to_delta(&mut buffer, &table_uri, &storage_opts).await?;
                            }
                        }
                        Err(err) => {
                            parse_errors += 1;
                            warn!(parse_errors, "Dropping unparseable tick: {err:#}");
                        }
                    }
                }
            }
            _ = flush_timer.tick() => {
                if !buffer.is_empty() {
                    flush_to_delta(&mut buffer, &table_uri, &storage_opts).await?;
                }
            }
        }
    }
}

async fn flush_to_delta(
    buffer: &mut ArrowOrderbookBuffer,
    table_uri: &str,
    storage_opts: &HashMap<String, String>,
) -> Result<()> {
    let row_count = buffer.len();
    let batch = buffer.flush()?;

    let ops = if storage_opts.is_empty() {
        DeltaOps::try_from_uri(table_uri).await?
    } else {
        DeltaOps::try_from_uri_with_storage_options(table_uri, storage_opts.clone()).await?
    };

    let table = ops
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    info!(
        "Committed Delta version {} with {} compacted rows to {}",
        table.version(),
        row_count,
        table_uri
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":11959768037182,"s":"BTCUSDT","ps":"BTCUSDT","b":"80677.10","B":"6.597","a":"80677.20","A":"1.951","T":1793296313365,"E":1793296313366,"st":1}}"#;

    #[test]
    fn parses_futures_book_ticker_with_exchange_time() {
        let tick = parse_tick(SAMPLE, 42).expect("valid payload");
        assert_eq!(tick.symbol, "BTCUSDT");
        assert_eq!(tick.sequence_id, 11959768037182);
        assert_eq!(tick.exchange_ts_us, 1793296313365000);
        assert_eq!(tick.ingest_ts_us, 42);
        assert_eq!(tick.best_bid_price, 80677.10);
        assert_eq!(tick.best_bid_qty, 6.597);
        assert_eq!(tick.best_ask_price, 80677.20);
        assert_eq!(tick.best_ask_qty, 1.951);
    }

    #[test]
    fn rejects_malformed_numbers_instead_of_zeroing() {
        let bad = SAMPLE.replace(r#""b":"80677.10""#, r#""b":"oops""#);
        let err = parse_tick(&bad, 0).unwrap_err();
        assert!(format!("{err:#}").contains("`b`"));
    }

    #[test]
    fn rejects_non_finite_numbers() {
        let bad = SAMPLE.replace(r#""A":"1.951""#, r#""A":"NaN""#);
        assert!(parse_tick(&bad, 0).is_err());
    }

    #[test]
    fn rejects_payload_without_exchange_time() {
        let spot = SAMPLE.replace(r#""T":1793296313365,"#, "");
        assert!(parse_tick(&spot, 0).is_err());
    }
}
