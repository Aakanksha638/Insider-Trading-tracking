use chrono::{DateTime, Utc};
use common::{Direction, MarketTick};
use std::collections::VecDeque;

const TICK_HISTORY: usize = 512;

#[derive(Debug, Clone)]
pub struct InsiderEvent {
    pub filer_cik: String,
    pub direction: Direction,
    pub at: DateTime<Utc>,
}

/// Rolling per-symbol context used to score incoming insider filings.
#[derive(Debug, Default)]
pub struct SymbolState {
    pub ticks: VecDeque<MarketTick>,
    /// Recent insider transactions for this symbol, used for cluster detection
    /// (multiple distinct insiders trading the same direction close together
    /// is historically a stronger signal than a single filing).
    pub recent_insider_events: VecDeque<InsiderEvent>,
}

impl SymbolState {
    pub fn push_tick(&mut self, tick: MarketTick) {
        self.ticks.push_back(tick);
        if self.ticks.len() > TICK_HISTORY {
            self.ticks.pop_front();
        }
    }

    pub fn last_price(&self) -> Option<f64> {
        self.ticks.back().map(|t| t.price)
    }

    pub fn push_insider_event(&mut self, ev: InsiderEvent, cluster_window: chrono::Duration) {
        self.recent_insider_events.push_back(ev.clone());
        let cutoff = ev.at - cluster_window;
        while self
            .recent_insider_events
            .front()
            .map(|e| e.at < cutoff)
            .unwrap_or(false)
        {
            self.recent_insider_events.pop_front();
        }
    }

    /// Count of *distinct filers* trading in `direction` within the current
    /// cluster window (already trimmed by `push_insider_event`).
    pub fn cluster_count(&self, direction: Direction) -> usize {
        let mut ciks: Vec<&str> = self
            .recent_insider_events
            .iter()
            .filter(|e| e.direction == direction)
            .map(|e| e.filer_cik.as_str())
            .collect();
        ciks.sort_unstable();
        ciks.dedup();
        ciks.len()
    }
}
