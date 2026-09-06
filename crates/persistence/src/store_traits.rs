//! Storage contracts. Kept in `common` (not the `persistence` crate) so
//! that `ingest-sec`, `signal-engine`, and `executor` can depend on a
//! trait rather than a concrete database -- the `persistence` crate is
//! the only thing that knows SQLite/Postgres exist. This keeps a
//! zero-config in-memory fallback trivial (see `InMemorySeenStore`) and
//! keeps the domain crates testable without a real DB.

use crate::{InsiderTx, Signal};
use async_trait::async_trait;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex;

/// Durable dedup: "have we already processed this key?". Used by
/// `ingest-sec` to avoid reprocessing filings after a restart -- SEC's
/// current-events feed replays roughly the last day of filings on every
/// poll, so without a durable check a restart silently re-emits signals
/// for everything still in that window.
#[async_trait]
pub trait SeenStore: Send + Sync {
    /// Returns `true` if `key` was newly recorded (i.e. this is the first
    /// time we've seen it), `false` if it was already present.
    async fn mark_seen_if_new(&self, key: &str) -> anyhow::Result<bool>;
}

/// Write-only audit trail for filings considered and signals emitted.
/// Optional -- pass `None` anywhere this is threaded through if you don't
/// want an audit log yet.
#[async_trait]
pub trait AuditSink: Send + Sync {
    async fn record_filing(&self, tx: &InsiderTx) -> anyhow::Result<()>;
    async fn record_signal(&self, signal: &Signal) -> anyhow::Result<()>;
}

/// Durable per-symbol position book. The store is the source of truth for
/// the *value* returned by `apply_delta` so concurrent executors (or a
/// restarted process) can't silently diverge from what's on disk.
#[async_trait]
pub trait PositionStore: Send + Sync {
    async fn load_all(&self) -> anyhow::Result<HashMap<String, f64>>;
    /// Atomically apply `delta` to `symbol`'s position and return the new total.
    async fn apply_delta(&self, symbol: &str, delta: f64) -> anyhow::Result<f64>;
}

// --- Zero-config in-memory default for SeenStore --------------------------

/// The pre-persistence behavior, kept as the default so the pipeline still
/// runs with no database configured. Does NOT survive restarts -- use a
/// real `SeenStore` (e.g. `persistence::Store`) once that matters.
#[derive(Default)]
pub struct InMemorySeenStore {
    seen: Mutex<HashSet<String>>,
}

#[async_trait]
impl SeenStore for InMemorySeenStore {
    async fn mark_seen_if_new(&self, key: &str) -> anyhow::Result<bool> {
        let mut seen = self.seen.lock().expect("seen-store mutex poisoned");
        Ok(seen.insert(key.to_string()))
    }
}