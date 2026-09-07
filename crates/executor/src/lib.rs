//! Execution sink abstraction. `PaperExecutor` is a safe no-op-on-the-market
//! default -- it logs and tracks a virtual position book so you can validate
//! signal quality before wiring up a real broker/venue adapter behind the
//! same `ExecutionSink` trait.

use async_trait::async_trait;
use common::{Direction, EventReceiver, PositionStore, Signal, SystemEvent};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

#[async_trait]
pub trait ExecutionSink: Send + Sync {
    async fn execute(&mut self, signal: &Signal) -> anyhow::Result<()>;
}

/// Tracks a virtual position per symbol. Real position sizing (e.g. sizing
/// by signal strength, risk limits, max-position caps) belongs here once
/// you're ready to move past "does the signal pipeline even work".
///
/// When constructed with a `PositionStore` (`with_store`), the store is the
/// source of truth for each position -- `execute` persists every delta and
/// uses the store's returned total, so positions survive a restart instead
/// of silently resetting to zero. Without a store, positions live only in
/// the in-memory `positions` map for the life of the process.
pub struct PaperExecutor {
    pub positions: HashMap<String, f64>,
    /// Fixed share size per signal for now -- replace with real sizing logic.
    pub default_size: f64,
    store: Option<Arc<dyn PositionStore>>,
}

impl PaperExecutor {
    pub fn new(default_size: f64) -> Self {
        Self {
            positions: HashMap::new(),
            default_size,
            store: None,
        }
    }

    /// Load existing positions from `store` and persist all future changes
    /// to it. Call this instead of `new` once you have a durable store.
    pub async fn with_store(default_size: f64, store: Arc<dyn PositionStore>) -> anyhow::Result<Self> {
        let positions = store.load_all().await?;
        info!(count = positions.len(), "loaded positions from durable store");
        Ok(Self {
            positions,
            default_size,
            store: Some(store),
        })
    }
}

#[async_trait]
impl ExecutionSink for PaperExecutor {
    async fn execute(&mut self, signal: &Signal) -> anyhow::Result<()> {
        let delta = match signal.direction {
            Direction::Buy => self.default_size,
            Direction::Sell => -self.default_size,
        };

        let new_position = if let Some(store) = &self.store {
            // The store is authoritative: this survives concurrent updates
            // and process restarts, unlike a purely in-memory counter.
            let total = store.apply_delta(&signal.symbol, delta).await?;
            self.positions.insert(signal.symbol.clone(), total);
            total
        } else {
            let pos = self.positions.entry(signal.symbol.clone()).or_insert(0.0);
            *pos += delta;
            *pos
        };

        info!(
            symbol = %signal.symbol,
            strength = ?signal.strength,
            new_position,
            "paper-executed signal"
        );
        Ok(())
    }
}

/// Consumes `Signal` events from `rx` and forwards them to `sink`.
/// Non-Signal events on the channel are ignored (the orchestrator should
/// generally give this stage a receiver that's already filtered, but this
/// keeps the loop robust if it isn't).
pub async fn run(mut sink: impl ExecutionSink, mut rx: EventReceiver) -> anyhow::Result<()> {
    while let Some(event) = rx.recv().await {
        if let SystemEvent::Signal(signal) = event {
            if let Err(e) = sink.execute(&signal).await {
                warn!(error = %e, "execution failed");
            }
        }
    }
    Ok(())
}