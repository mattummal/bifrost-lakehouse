// Implement the Live WebSocket + Delta Lake Writer (src/main.rs)
// This connects to the live Binance @bookTicker / @depth stream for
// BTCUSDT, ETHUSDT, and SOLUSDT, micro-batches events in memory, and
// commits compacted Parquet files + _delta_log transactions directly to local Azurite.
//----------------------------------------------
// automatically creates and writes to
// .lakehouse_data/bronze/orderbook_bbo by default,
// or switches to az:// when BIFROST_STORAGE=azurite or azure:
//----------------------------------------------
mod arrow_buffer;

use std::collections::HashMap;
use std::time::Duration;
use anyhow::Result;
use arrow_buffer::{ArrowOrderbookBuffer, OrderbookTick};
use chrono::Utc;
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::time::interval;
use tokio_tungstenite::connect_async;
use tracing::info;

const BINANCE_WS_URL: &str =
    "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker/solusdt@bookTicker";
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
    #[serde(rename = "b")]
    best_bid_price: String,
    #[serde(rename = "B")]
    best_bid_qty: String,
    #[serde(rename = "a")]
    best_ask_price: String,
    #[serde(rename = "A")]
    best_ask_qty: String,
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

    loop {
        tokio::select! {
            Some(msg) = read.next() => {
                if let Ok(tungstenite_msg) = msg {
                    if let Ok(text) = tungstenite_msg.into_text() {
                        if let Ok(wrapper) = serde_json::from_str::<CombinedStreamWrapper>(&text) {
                            let ev = wrapper.data;
                            let now_us = Utc::now().timestamp_micros();

                            let tick = OrderbookTick {
                                exchange_ts_us: now_us,
                                ingest_ts_us: now_us,
                                symbol: ev.symbol,
                                sequence_id: ev.update_id,
                                best_bid_price: ev.best_bid_price.parse().unwrap_or(0.0),
                                best_bid_qty: ev.best_bid_qty.parse().unwrap_or(0.0),
                                best_ask_price: ev.best_ask_price.parse().unwrap_or(0.0),
                                best_ask_qty: ev.best_ask_qty.parse().unwrap_or(0.0),
                            };
                            buffer.push(&tick);

                            if buffer.len() >= BATCH_SIZE_THRESHOLD {
                                flush_to_delta(&mut buffer, &table_uri, &storage_opts).await?;
                            }
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