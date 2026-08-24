//! Wires the pipeline together:
//!
//!   ingest-sec  ---\
//!                    -->  signal-engine  -->  executor
//!   ingest-market --/
//!
//! Both ingestion stages push onto one shared inbound channel; the
//! signal-engine consumes InsiderFiling + Tick events and emits Signal
//! events onto a second channel that the executor consumes.

use common::new_channel;
use executor::PaperExecutor;
use ingest_market::{MarketFeed, MockFeed};
use ingest_sec::{poll_loop, PollerConfig};
use signal_engine::EngineConfig;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    // inbound: ingest-sec + ingest-market -> signal-engine
    let (inbound_tx, inbound_rx) = new_channel();
    // outbound: signal-engine -> executor
    let (outbound_tx, outbound_rx) = new_channel();

    // --- ingest-sec: real SEC EDGAR Form 4 poller ---
    // NOTE: set a real contact email in the User-Agent before running this
    // against the live feed -- SEC will block generic/default UAs.
    let sec_cfg = PollerConfig {
        user_agent: "insider-trade-tracker/0.1 (replace-with-your-email@example.com)".to_string(),
        poll_interval: Duration::from_secs(3),
        max_concurrent_fetches: 8,
    };
    let sec_tx = inbound_tx.clone();
    let sec_handle = tokio::spawn(async move {
        if let Err(e) = poll_loop(sec_cfg, sec_tx).await {
            tracing::error!(error = %e, "ingest-sec exited");
        }
    });

    // --- ingest-market: placeholder mock feed ---
    // Swap MockFeed for a real venue adapter implementing `MarketFeed`
    // once you have a market data subscription.
    let watchlist = vec!["AAPL".to_string(), "MSFT".to_string(), "TSLA".to_string()];
    let market_tx = inbound_tx.clone();
    let market_handle = tokio::spawn(async move {
        let feed = MockFeed::new(watchlist);
        if let Err(e) = feed.stream(market_tx).await {
            tracing::error!(error = %e, "ingest-market exited");
        }
    });
    drop(inbound_tx); // only the spawned tasks' clones should keep the channel alive

    // --- signal-engine ---
    let engine_handle = tokio::spawn(async move {
        if let Err(e) = signal_engine::run(EngineConfig::default(), inbound_rx, outbound_tx).await {
            tracing::error!(error = %e, "signal-engine exited");
        }
    });

    // --- executor: paper trading by default ---
    let executor_handle = tokio::spawn(async move {
        let sink = PaperExecutor::new(100.0);
        if let Err(e) = executor::run(sink, outbound_rx).await {
            tracing::error!(error = %e, "executor exited");
        }
    });

    tracing::info!("pipeline running: ingest-sec + ingest-market -> signal-engine -> executor");

    let _ = tokio::join!(sec_handle, market_handle, engine_handle, executor_handle);
    Ok(())
}
