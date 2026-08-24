//! Shared domain types + the internal event bus contract.
//!
//! Latency reality check (important for design decisions downstream):
//! Form 4 filings are NOT a sub-millisecond feed. Insiders must file within
//! 2 business days of the transaction, and EDGAR's real-time index updates
//! on the order of seconds. So the "HFT" edge in this system isn't
//! market-making-grade tick-to-trade latency — it's being the fastest
//! *reactor* to a public filing the moment it lands on EDGAR, and fusing
//! it with live market microstructure (order book / tape) to time entry.
//! Keep that distinction in the signal-engine design: insider events are
//! the low-frequency trigger, market ticks are the high-frequency context.

pub mod events;
pub mod types;

pub use events::*;
pub use types::*;
