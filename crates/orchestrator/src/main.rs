//! Wires the pipeline together:
//!
//!   ingest-sec  ---\
//!                    -->  signal-engine  -->  executor
//!   ingest-market --/
//!
//! Both ingestion stages push onto one shared inbound channel; the
//! signal-engine consumes InsiderFiling + Tick events and emits Signal
//! events onto a second channel that the executor consumes.
//!
//! Persistence: set DATABASE_URL (e.g. `sqlite:insider_tracker.db`) to get
//! durable filing dedup, an audit log of filings/signals, and a position
//! book that survives restarts. Without it, everything falls back to
//! in-memory (dedup resets, positions reset) -- fine for quick local runs,
//! not fine for anything you actually want to trust across restarts.

use common::{new_channel, AuditSink, InMemorySeenStore, PositionStore, SeenStore};
use executor::PaperExecutor;
use ingest_market::{MarketFeed, MockFeed, PolygonConfig, PolygonFeed};
use ingest_sec::{poll_loop, PollerConfig};
use signal_engine::EngineConfig;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    // --- persistence: optional, controlled by DATABASE_URL ---
    let store: Option<Arc<persistence::Store>> = match std::env::var("DATABASE_URL") {
        Ok(url) if !url.is_empty() => {
            tracing::info!(%url, "connecting to durable store");
            Some(Arc::new(persistence::Store::connect(&url).await?))
        }
        _ => {
            tracing::warn!(
                "DATABASE_URL not set -- dedup and positions are IN-MEMORY ONLY and will reset on restart"
            );
            None
        }
    };

    let seen_store: Arc<dyn SeenStore> = match &store {
        Some(s) => s.clone(),
        None => Arc::new(InMemorySeenStore::default()),
    };
    let audit_sink: Option<Arc<dyn AuditSink>> = store.as_ref().map(|s| s.clone() as Arc<dyn AuditSink>);
    let position_store: Option<Arc<dyn PositionStore>> =
        store.as_ref().map(|s| s.clone() as Arc<dyn PositionStore>);

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
        if let Err(e) = poll_loop(sec_cfg, sec_tx, seen_store).await {
            tracing::error!(error = %e, "ingest-sec exited");
        }
    });

    // --- ingest-market: real Polygon.io feed if POLYGON_API_KEY is set,
    // otherwise fall back to the synthetic MockFeed for local testing.
    let watchlist = vec!["AAPL".to_string(), "MSFT".to_string(), "TSLA".to_string()];
    let market_tx = inbound_tx.clone();
    let market_handle = tokio::spawn(async move {
        let feed: Arc<dyn MarketFeed> = match std::env::var("POLYGON_API_KEY") {
            Ok(key) if !key.is_empty() => {
                tracing::info!("using PolygonFeed for market data");
                Arc::new(PolygonFeed::new(PolygonConfig::new(key, watchlist)))
            }
            _ => {
                tracing::warn!(
                    "POLYGON_API_KEY not set -- using MockFeed (synthetic, not real prices)"
                );
                Arc::new(MockFeed::new(watchlist))
            }
        };
        if let Err(e) = feed.stream(market_tx).await {
            tracing::error!(error = %e, "ingest-market exited");
        }
    });
    drop(inbound_tx); // only the spawned tasks' clones should keep the channel alive

    // --- signal-engine ---
    let engine_handle = tokio::spawn(async move {
        if let Err(e) =
            signal_engine::run(EngineConfig::default(), inbound_rx, outbound_tx, audit_sink).await
        {
            tracing::error!(error = %e, "signal-engine exited");
        }
    });

    // --- executor: paper trading by default, durable if a store is configured ---
    let executor_handle = tokio::spawn(async move {
        let sink = match position_store {
            Some(store) => match PaperExecutor::with_store(100.0, store).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "failed to load positions from store, falling back to in-memory");
                    PaperExecutor::new(100.0)
                }
            },
            None => PaperExecutor::new(100.0),
        };
        if let Err(e) = executor::run(sink, outbound_rx).await {
            tracing::error!(error = %e, "executor exited");
        }
    });

    tracing::info!("pipeline running: ingest-sec + ingest-market -> signal-engine -> executor");

    let _ = tokio::join!(sec_handle, market_handle, engine_handle, executor_handle);
    Ok(())
}