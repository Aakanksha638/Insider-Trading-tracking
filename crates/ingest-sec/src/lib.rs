//! Polls SEC EDGAR's "current events" Atom feed for new Form 4 filings,
//! fetches each filing's XML, and emits normalized `InsiderTx` events.
//!
//! SEC fair-access rules require a descriptive User-Agent
//! ("AppName/Version (contact@email)") on every request, and ask that
//! automated tools stay within ~10 requests/second. We poll the index at a
//! configurable interval and fetch filing bodies with a small concurrency
//! cap — this is a "react fast to a public disclosure" system, not a
//! feed we're allowed to hammer.

mod feed;
mod form4;

pub use feed::{poll_loop, PollerConfig};
