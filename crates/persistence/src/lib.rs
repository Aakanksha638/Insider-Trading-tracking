//! SQLite-backed implementation of the storage traits declared in
//! `common::store_traits`. Swapping to Postgres later means writing a new
//! `Store` here (or a feature-gated backend) -- nothing in `ingest-sec`,
//! `signal-engine`, or `executor` needs to change, since they only see
//! the trait objects.

use async_trait::async_trait;
use common::{AuditSink, InsiderTx, PositionStore, SeenStore, Signal};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::str::FromStr;
use tracing::info;

pub struct Store {
    pool: SqlitePool,
}

const SCHEMA_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS seen_filings (
        accession TEXT PRIMARY KEY,
        first_seen_at TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS filings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        accession TEXT NOT NULL,
        symbol TEXT NOT NULL,
        filer_name TEXT NOT NULL,
        filer_cik TEXT NOT NULL,
        is_officer INTEGER NOT NULL,
        is_director INTEGER NOT NULL,
        is_ten_pct_owner INTEGER NOT NULL,
        code TEXT NOT NULL,
        shares REAL NOT NULL,
        price_per_share REAL,
        shares_owned_after REAL NOT NULL,
        transaction_date TEXT NOT NULL,
        filed_at TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        strength TEXT NOT NULL,
        reason TEXT NOT NULL,
        generated_at TEXT NOT NULL,
        source_accession TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS positions (
        symbol TEXT PRIMARY KEY,
        quantity REAL NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS idx_filings_symbol ON filings(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)",
];

impl Store {
    /// `database_url` examples: `sqlite:insider_tracker.db`, `sqlite::memory:`.
    /// The file is created automatically if it doesn't exist.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let opts = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
        let pool = SqlitePoolOptions::new().max_connections(5).connect_with(opts).await?;
        let store = Self { pool };
        store.run_migrations().await?;
        Ok(store)
    }

    async fn run_migrations(&self) -> anyhow::Result<()> {
        for stmt in SCHEMA_STATEMENTS {
            sqlx::query(stmt).execute(&self.pool).await?;
        }
        info!("persistence schema ready");
        Ok(())
    }
}

#[async_trait]
impl SeenStore for Store {
    async fn mark_seen_if_new(&self, key: &str) -> anyhow::Result<bool> {
        let result = sqlx::query(
            "INSERT OR IGNORE INTO seen_filings (accession, first_seen_at) VALUES (?, ?)",
        )
        .bind(key)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }
}

#[async_trait]
impl AuditSink for Store {
    async fn record_filing(&self, tx: &InsiderTx) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO filings (
                accession, symbol, filer_name, filer_cik, is_officer, is_director,
                is_ten_pct_owner, code, shares, price_per_share, shares_owned_after,
                transaction_date, filed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&tx.accession_number)
        .bind(&tx.issuer_symbol)
        .bind(&tx.filer_name)
        .bind(&tx.filer_cik)
        .bind(tx.is_officer)
        .bind(tx.is_director)
        .bind(tx.is_ten_pct_owner)
        .bind(format!("{:?}", tx.code))
        .bind(tx.shares)
        .bind(tx.price_per_share)
        .bind(tx.shares_owned_after)
        .bind(tx.transaction_date.to_rfc3339())
        .bind(tx.filed_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_signal(&self, signal: &Signal) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO signals (symbol, direction, strength, reason, generated_at, source_accession)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&signal.symbol)
        .bind(format!("{:?}", signal.direction))
        .bind(format!("{:?}", signal.strength))
        .bind(&signal.reason)
        .bind(signal.generated_at.to_rfc3339())
        .bind(&signal.source_accession)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl PositionStore for Store {
    async fn load_all(&self) -> anyhow::Result<HashMap<String, f64>> {
        let rows = sqlx::query("SELECT symbol, quantity FROM positions")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>("symbol"), r.get::<f64, _>("quantity")))
            .collect())
    }

    async fn apply_delta(&self, symbol: &str, delta: f64) -> anyhow::Result<f64> {
        // SQLite has no native UPSERT-with-return in one call across all
        // driver versions in a way sqlx loves, so do it as an explicit
        // transaction: safe under sqlx's connection-per-await-point model
        // since the pool serializes writers on SQLite anyway.
        let mut txn = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO positions (symbol, quantity) VALUES (?, ?)
             ON CONFLICT(symbol) DO UPDATE SET quantity = quantity + excluded.quantity",
        )
        .bind(symbol)
        .bind(delta)
        .execute(&mut *txn)
        .await?;
        let row = sqlx::query("SELECT quantity FROM positions WHERE symbol = ?")
            .bind(symbol)
            .fetch_one(&mut *txn)
            .await?;
        let new_qty: f64 = row.get("quantity");
        txn.commit().await?;
        Ok(new_qty)
    }
}