//! Execution sink abstraction. `PaperExecutor` is a safe no-op-on-the-market
//! default -- it logs and tracks a virtual position book so you can validate
//! signal quality before wiring up a real broker/venue adapter behind the
//! same `ExecutionSink` trait.

use async_trait::async_trait;
use common::{Direction, EventReceiver, Signal, SystemEvent};
use std::collections::HashMap;
use tracing::{info, warn};

#[async_trait]
pub trait ExecutionSink: Send + Sync {
    async fn execute(&mut self, signal: &Signal) -> anyhow::Result<()>;
}

/// Tracks a virtual position per symbol. Real position sizing (e.g. sizing
/// by signal strength, risk limits, max-position caps) belongs here once
/// you're ready to move past "does the signal pipeline even work".
#[derive(Debug, Default)]
pub struct PaperExecutor {
    pub positions: HashMap<String, f64>,
    /// Fixed share size per signal for now -- replace with real sizing logic.
    pub default_size: f64,
}

impl PaperExecutor {
    pub fn new(default_size: f64) -> Self {
        Self {
            positions: HashMap::new(),
            default_size,
        }
    }
}

#[async_trait]
impl ExecutionSink for PaperExecutor {
    async fn execute(&mut self, signal: &Signal) -> anyhow::Result<()> {
        let delta = match signal.direction {
            Direction::Buy => self.default_size,
            Direction::Sell => -self.default_size,
        };
        let pos = self.positions.entry(signal.symbol.clone()).or_insert(0.0);
        *pos += delta;
        info!(
            symbol = %signal.symbol,
            strength = ?signal.strength,
            new_position = *pos,
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
