use crate::types::{InsiderTx, MarketTick, Signal};
use serde::{Deserialize, Serialize};

/// Everything that flows through the internal bus, unified so every stage
/// can be wired with a single channel type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemEvent {
    InsiderFiling(InsiderTx),
    Tick(MarketTick),
    Signal(Signal),
}

/// Ingestion stages produce `SystemEvent`s onto an mpsc channel; the
/// signal-engine consumes them and the executor consumes `Signal`s.
/// Bounded channels are used everywhere on purpose — an unbounded channel
/// hides backpressure, and in a trading system silently falling behind is
/// worse than an explicit, measurable drop/slow-consumer signal.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 4096;

pub type EventSender = tokio::sync::mpsc::Sender<SystemEvent>;
pub type EventReceiver = tokio::sync::mpsc::Receiver<SystemEvent>;

pub fn new_channel() -> (EventSender, EventReceiver) {
    tokio::sync::mpsc::channel(DEFAULT_CHANNEL_CAPACITY)
}
