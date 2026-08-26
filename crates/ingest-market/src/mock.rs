use crate::MarketFeed;
use async_trait::async_trait;
use chrono::Utc;
use common::{EventSender, MarketTick, SystemEvent, TickSide};
use std::time::Duration;
use tracing::warn;

/// Emits a synthetic random-walk tick for each watched symbol on a fixed
/// interval. Replace with a real feed for anything beyond pipeline testing --
/// this does not reflect real market prices.
pub struct MockFeed {
    pub symbols: Vec<String>,
    pub interval: Duration,
}

impl MockFeed {
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            symbols,
            interval: Duration::from_secs(1),
        }
    }
}

#[async_trait]
impl MarketFeed for MockFeed {
    async fn stream(&self, tx: EventSender) -> anyhow::Result<()> {
        let mut prices: Vec<f64> = self.symbols.iter().map(|_| 100.0).collect();
        loop {
            for (i, symbol) in self.symbols.iter().enumerate() {
                let jitter = ((Utc::now().timestamp_nanos_opt().unwrap_or(0) % 21) - 10) as f64 / 100.0;
                prices[i] = (prices[i] + jitter).max(1.0);

                let tick = MarketTick {
                    symbol: symbol.clone(),
                    price: prices[i],
                    size: 100.0,
                    side: TickSide::Trade,
                    timestamp: Utc::now(),
                };
                if tx.send(SystemEvent::Tick(tick)).await.is_err() {
                    warn!("event channel closed, stopping mock feed");
                    return Ok(());
                }
            }
            tokio::time::sleep(self.interval).await;
        }
    }
}
